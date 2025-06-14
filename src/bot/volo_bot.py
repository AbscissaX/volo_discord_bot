import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import discord
import yaml

from ollama import AsyncClient

import requests
import base64

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
        self.guild_is_painting = {}
        self.player_map = {}
        self._is_ready = False
        self._art_task = None
        self._art_task_stop = asyncio.Event()
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
        await self.stop_and_cleanup()

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
                # Disconnect the voice client if still connected
                vc = getattr(sink, "vc", None)
                if vc and vc.is_connected():
                    await vc.disconnect()
            self.guild_whisper_sinks.clear()
        except Exception as e:
            logger.error(f"Error stopping whisper sinks: {e}")
        finally:
            logger.info("Cleanup completed.")

    async def start_art_chronicler(self, ctx):
        """Starts a background task that creates art from the last 5 minutes every 5 minutes."""
        if not self.guild_is_recording.get(ctx.guild_id, False):
            logger.info("Need to be recording to start the art chronicler.")
            return
        if self._art_task and not self._art_task.done():
            logger.info("Art Chronicler task has already started.")
            return

        self._art_task_stop.clear()
        self._art_task = asyncio.create_task(self._art_chronicler_task(ctx))
        self.guild_is_painting[ctx.guild_id] = True
        await ctx.respond("Started periodic 5-minute summaries.", ephemeral=True)

    async def stop_art_chronicler(self, ctx):
        """Stops the periodic summary task."""
        if self._art_task:
            self._art_task_stop.set()
            await self._art_task
            self._art_task = None
            self.guild_is_painting[ctx.guild_id] = False
        else:
            await ctx.respond("My apologies, was I suppose to be painting?", ephemeral=True)

    async def _art_chronicler_task(self, ctx):
        while not self._art_task_stop.is_set():
            await asyncio.sleep(60)  # 5 minutes
            await self.send_last_5min_summary(ctx)

    async def send_last_5min_summary(self, ctx):
        """Extracts the last 5 minutes of transcription and sends a summary."""
        """It extracts the log file for the current date and summarizes the last 5 minutes of transcriptions."""
        """ this was done to be less invasive and not impact the get transcriptios function in whisper_sink"""
        # Use the same log_filename as in configure_logging
        log_directory = '.logs/transcripts'
        current_date = datetime.now().strftime('%Y-%m-%d')
        log_filename = os.path.join(log_directory, f"{current_date}-transcription.log")

        now = datetime.now()
        five_minutes_ago = now - timedelta(minutes=60)
        recent_lines = []

        try:
            with open(log_filename, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        # Parse the 'begin' time
                        begin_time_str = entry.get('date', '') + ' ' + entry.get('begin', '')
                        begin_time = datetime.strptime(begin_time_str, '%Y-%m-%d %H:%M:%S.%f')
                        if begin_time >= five_minutes_ago:
                            character = entry.get('character', 'Unknown')
                            data = entry.get('data', '')
                            recent_lines.append(f'{character}: {data}')
                    except Exception:
                        continue
        except FileNotFoundError:
            logger.info(f"No transcription log found for {log_filename}")
            return

        if not recent_lines:
            logger.info("No recent transcriptions found for summary.")
            return

        summary_text = "\n".join(recent_lines)

        summary = await summarizer_image_prompt(summary_text)
        if summary is None:
            logger.error("Failed to generate image prompt summary.")
            return

        image_file = await generate_image(summary)
        if image_file is None:
            logger.error("Failed to generate image from prompt.")
            return
        
        channel = ctx.channel or self.get_channel(360328434213322764)
        if summary and channel:
            await channel.send(f"**5-Minute Summary:**\n{summary}")
            await channel.send(file=discord.File(image_file, filename="generated_image.png"))

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
    
async def summarizer_image_prompt(summary: str):
    """
    Summarizes the provided text using the Ollama API into a prompt for images.
    the Ollama program is ran locally and uses the impactframes/llama3_ifai_sd_prompt_mkr_q4km model.
    Args:
        summary (str): The text to summarize.
    """

    client = AsyncClient()
    logger.info(f"Summarizing text for image prompt: {summary}")
    if not summary:
        logger.error("No summary provided for image prompt generation.")
        return None
    try:
        response = await client.chat(
            model="impactframes/llama3_ifai_sd_prompt_mkr_q4km",
            messages=[
                #{"role": "system", "content": "Summerize the provided text into a detailed and specific image prompt for stable diffusion."},
                #{"role": "system", "content": "You are a expert prompt engineer who writes image prompts for stable diffusion based on a events that are happening in a transcript. "
                #"You are very good at summarizing the events and creating a prompt that captures the esseence of the events. you do not make up info or fill in gaps. the format of the prompt should be keywords only; A good "
                #"prompt needs to be detailed and specific. A good process is to look through a list of keyword categories and decide whether you want to use any of them. The keyword "
                #"categories are, Subject, Medium, Style, Art-sharing website, Resolution, Additional details, Color, Lighting. do not desribe the events in a narrative form, just "
                #"give me a list of keywords that describe the events for an image. do not explain why you chose the keywords nor state what type of keywords they are, just give me the keywords as a comma separated list. have the list start with a short descriptiopn of the characters to portay (3-4 words max), then continue with the keywords"},
                {"role": "user", "content": f"{summary}"},
            ]
        )
        return response['message']['content']
    except Exception as e:
        logger.error(f"Error summarizing text: {e}")
        return None
    
async def generate_image(prompt: str):
    """
    Generates an image based on the provided prompt using the Ollama API.
    the Ollama program is ran locally using stable diffusion and uses the plant milk model.
    Args:
        prompt (str): The prompt to generate an image from.
    """
    url = "http://127.0.0.1:7860/sdapi/v1/txt2img" # Replace with your WebUI address
    payload = {
        "prompt": f"masterpiece, best quality, absurdres, highres, very aesthetic, sharp focus, depth of field, perfect lighting, detailed illustration, highly detailed, detailed face, detailed eyes, detailed hair, ambient occlusion, perfect lighting, scenery, {prompt}",
        "negative_prompt": "censored, worst quality, low quality, normal quality, lowres, jpeg artifacts, signature, watermark, artist name, bad hands, extra digits, simple background, simple shading, simple drawing, simple coloring,",
        "steps": 40,
        "width": 512,
        "height": 512,
        "sampler_name": "Euler a"
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            image_data = data['images'][0]
            # Save or display the image data
            with open("generated_image.png", "wb") as image_file:
                image_file.write(base64.b64decode(image_data))
            return "generated_image.png"

        if response.status_code == 404:
            logger.error("Image generation endpoint not found. Please check the URL.") 
        else:
            logger.error(f"Error generating image: { response.status_code}")
    except Exception as e:
        logger.error(f"Error summarizing text: {e}")
        return None