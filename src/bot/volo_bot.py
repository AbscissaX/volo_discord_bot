import asyncio
import json
import logging
import os
from collections import defaultdict

import discord
import yaml

from ollama import AsyncClient

from src.sinks.whisper_sink import WhisperSink

DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID"))
TRANSCRIPTION_METHOD = os.getenv("TRANSCRIPTION_METHOD")
PLAYER_MAP_FILE_PATH = os.getenv("PLAYER_MAP_FILE_PATH")
MAX_DISCORD_MESSAGE_LENGTH = 2000


logger = logging.getLogger(__name__)

class VoloBot(discord.Bot):
    def __init__(self, loop):

        super().__init__(command_prefix="!", loop=loop,
                         activity=discord.CustomActivity(name='Transcribing Audio to Text'))
        self.guild_to_helper = {}
        self.guild_is_recording = {}
        self.guild_whisper_sinks = {}
        self.guild_whisper_message_tasks = {}
        self.player_map = {}
        self._is_ready = False
        if TRANSCRIPTION_METHOD == "openai":
            self.transcriber_type = "openai"
        else:
            self.transcriber_type = "local"
        if PLAYER_MAP_FILE_PATH:
            with open(PLAYER_MAP_FILE_PATH, "r", encoding="utf-8") as file:
                self.player_map = yaml.safe_load(file)

    

    async def on_ready(self):
        logger.info(f"Logged in as {self.user} to Discord.")
        self._is_ready = True


    async def close_consumers(self):
        await self.consumer_manager.close()

    def _close_and_clean_sink_for_guild(self, guild_id: int):
        whisper_sink: WhisperSink | None = self.guild_whisper_sinks.get(
            guild_id, None)

        if whisper_sink:
            logger.debug(f"Stopping whisper sink, requested by {guild_id}.")
            whisper_sink.stop_voice_thread()
            del self.guild_whisper_sinks[guild_id]
            whisper_sink.close()

    
    def start_recording(self, ctx: discord.context.ApplicationContext):
        """
        Start recording audio from the voice channel. Create a whisper sink
        and start sending transcripts to the queue.

        Since this is a critical function, this is where we should handle
        subscription checks and limits.
        """
        try:
            self.start_whisper_sink(ctx)
            self.guild_is_recording[ctx.guild_id] = True
        except Exception as e:
            logger.error(f"Error starting whisper sink: {e}")

    def start_whisper_sink(self, ctx: discord.context.ApplicationContext):
        guild_voice_sink = self.guild_whisper_sinks.get(ctx.guild_id, None)
        if guild_voice_sink:
            logger.debug(
                f"Sink is already active for guild {ctx.guild_id}.")
            return

        async def on_stop_record_callback(sink: WhisperSink, ctx):
            logger.debug(
                f"{ctx.channel.guild.id} -> on_stop_record_callback")
            self._close_and_clean_sink_for_guild(ctx.guild_id)

        transcript_queue = asyncio.Queue()

        whisper_sink = WhisperSink(
            transcript_queue,
            self.loop,
            data_length=50000,
            max_speakers=10,
            transcriber_type=self.transcriber_type,
            player_map=self.player_map,
        )

        self.guild_to_helper[ctx.guild_id].vc.start_recording(
            whisper_sink, on_stop_record_callback, ctx)

        def on_thread_exception(e):
            logger.warning(
                f"Whisper sink thread exception for guild {ctx.guild_id}. Retry in 5 seconds...\n{e}")
            self._close_and_clean_sink_for_guild(ctx.guild_id)

            # retry in 5 seconds
            self.loop.call_later(5, self.start_recording, ctx)

        whisper_sink.start_voice_thread(on_exception=on_thread_exception)

        self.guild_whisper_sinks[ctx.guild_id] = whisper_sink

    def stop_recording(self, ctx: discord.context.ApplicationContext):
        vc = ctx.guild.voice_client
        if vc:
            self.guild_is_recording[ctx.guild_id] = False
            vc.stop_recording()
        guild_id = ctx.guild_id
        whisper_message_task = self.guild_whisper_message_tasks.get(
            guild_id, None)
        if whisper_message_task:
            logger.debug("Cancelling whisper message task.")
            whisper_message_task.cancel()
            del self.guild_whisper_message_tasks[guild_id]

    def cleanup_sink(self, ctx: discord.context.ApplicationContext):
        guild_id = ctx.guild_id
        self._close_and_clean_sink_for_guild(guild_id)

    async def get_transcription(self, ctx: discord.context.ApplicationContext):
        # Get the transcription queue
        if not (self.guild_whisper_sinks.get(ctx.guild_id)):
            logger.info(
                f"No whisper sink found for guild {ctx.guild_id}. Cannot get transcription.")
            return
        whisper_sink = self.guild_whisper_sinks[ctx.guild_id]
        transcriptions = []
        if whisper_sink is None:
            logger.info(
                f"No whisper sink found for guild {ctx.guild_id}. Cannot get transcription.")
            return
    
        transcriptions_queue = whisper_sink.transcription_output_queue
        while not transcriptions_queue.empty():
            transcriptions.append(await transcriptions_queue.get())
        return transcriptions

    async def get_summary(self, ctx: discord.context.ApplicationContext, log_filename: str):
        logger.info(f"Getting summary for {ctx.guild.name} with log file {log_filename}")
        loggedData = await load_json_file(log_filename)
        if loggedData is None:
            logger.error(f"Failed to load log file {log_filename}")
            return
        if not loggedData:
            logger.info(f"No data found in log file {log_filename}")
            return
        # extract just the data from the loggedData
        lines = []
        for entry in loggedData:
            if 'data' in entry:
                character = entry['character']
                data = entry['data']
                lines.append(f'{character}: {data}')
        if not lines:
            logger.info(f"No data found in log file {log_filename}")
            return
        summary = "\n".join(lines)

        tale = await summarizer_bard(summary)

        # Send the summary to the channel in chunks
        channel = self.get_channel(360328434213322764)
        if channel is None: 
            logger.error(f"Channel with ID {360328434213322764} not found.")
            return 
        try:
            if tale:
                header = f"Bardic Tale for {ctx.guild.name}:\n"
                max_first = MAX_DISCORD_MESSAGE_LENGTH - len(header)
                # First chunk with header
                await channel.send(header + tale[:max_first])
                # Remaining chunks
                for i in range(max_first, len(tale), MAX_DISCORD_MESSAGE_LENGTH):
                    await channel.send(tale[i:i+MAX_DISCORD_MESSAGE_LENGTH])
                logger.info(f"Summary sent to {channel.name} in guild {ctx.guild.name}.")   
        except discord.Forbidden:
            logger.error(f"Permission denied to send messages in {channel.name}.")
        except discord.HTTPException as e:
            logger.error(f"Failed to send message in {channel.name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while sending summary: {e}")

        notes = await summarizer_serious(summary)

        try:
            if notes:
                header = f"Notes for {ctx.guild.name}:\n"
                max_first = MAX_DISCORD_MESSAGE_LENGTH - len(header)
                # First chunk with header
                await channel.send(header + notes[:max_first])
                # Remaining chunks
                for i in range(max_first, len(notes), MAX_DISCORD_MESSAGE_LENGTH):
                    await channel.send(notes[i:i+MAX_DISCORD_MESSAGE_LENGTH])
                logger.info(f"Summary sent to {channel.name} in guild {ctx.guild.name}.")   
        except discord.Forbidden:
            logger.error(f"Permission denied to send messages in {channel.name}.")
        except discord.HTTPException as e:
            logger.error(f"Failed to send message in {channel.name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while sending summary: {e}")
        return tale

    async def update_player_map(self, ctx: discord.context.ApplicationContext):
        player_map = {}
        for member in ctx.guild.members:
            player_map[member.id] = {
                "player": member.name,
                "character": member.display_name
            }
        logger.info(f"{str(player_map)}")
        self.player_map.update(player_map)
        if PLAYER_MAP_FILE_PATH:
            with open(PLAYER_MAP_FILE_PATH, "w", encoding="utf-8") as file:
                yaml.dump(self.player_map, file, default_flow_style=False, allow_unicode=True)

    async def stop_and_cleanup(self):
        try:
            for sink in self.guild_whisper_sinks.values():
                sink.close()
                sink.stop_voice_thread()
                logger.debug(
                    f"Stopped whisper sink for guild {sink.vc.channel.guild.id} in cleanup.")
            self.guild_whisper_sinks.clear()
        except Exception as e:
            logger.error(f"Error stopping whisper sinks: {e}")
        finally:
            logger.info("Cleanup completed.")

async def load_json_file(file_path: str):
    """
    Load a newline-delimited JSON (NDJSON) file and return its content as a list of dicts.
    """
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:
                    data.append(json.loads(line))
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading JSON file {file_path}: {e}")
        return None

async def summarizer_bard(summary: str):
    """
    Summarizes the provided text using the Ollama API.
    the Ollama program is ran locally and uses the llama3.2 model.
    Args:
        summary (str): The text to summarize.
    """
    client = AsyncClient()
    try:
        response = await client.chat(
            model="llama3.2",
            messages=[
                {"role": "system", "content": "You are a well written medieval bard who writes summaries of adventures tales using a transcript of their adventure. you may use the transcript to write a summary of the adventure. You are very good at summarizing and writing in a modern medieval style. you make sure to capture ever detail of the adventure and make it sound like a grand tale."},
                {"role": "user", "content": f"{summary}"},
            ]
        )
        return response['message']['content']
    except Exception as e:
        logger.error(f"Error summarizing text: {e}")
        return None
    
async def summarizer_serious(summary: str):
    """
    Summarizes the provided text using the Ollama API.
    the Ollama program is ran locally and uses the llama3.2 model.
    Args:
        summary (str): The text to summarize.
    """
    client = AsyncClient()
    try:
        response = await client.chat(
            model="llama3.2",
            messages=[
                {"role": "system", "content": "You are a well written meeting summarizer who writes summaries of meetings using a transcript of the meeting. you may use the transcript to write a summary of the meeting. You are very good at summarizing and writing in a modern style. you make sure to capture ever detail of the meeting and make it sound like a professional summary. you do not embellish the summary or make up details, you just summarize the meeting in a professional manner."},
                {"role": "user", "content": f"{summary}"},
            ]
        )
        return response['message']['content']
    except Exception as e:
        logger.error(f"Error summarizing text: {e}")
        return None