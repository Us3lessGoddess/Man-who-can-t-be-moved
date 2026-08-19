import discord
from discord.ext import commands, tasks
import asyncio
import os
import logging
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
VOICE_CHANNEL_ID = int(os.getenv("VOICE_CHANNEL_ID"))

# How often the watchdog checks the connection is still alive (seconds)
WATCHDOG_INTERVAL = 30
# How long to wait between reconnect attempts if one fails (seconds)
RECONNECT_BACKOFF = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vc_keepalive")

intents = discord.Intents.default()
intents.voice_states = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

vc_lock = asyncio.Lock()

# --- Keep-alive web server (for UptimeRobot / Render health checks) ---
app = Flask('')

@app.route('/')
def home():
    return "VC keepalive bot is alive"

def run_web():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    Thread(target=run_web, daemon=True).start()
# ------------------------------------------------


async def connect_to_vc():
    """Join the target voice channel. Safe to call repeatedly."""
    if vc_lock.locked():
        log.info("Connect already in progress, skipping duplicate attempt.")
        return False

    async with vc_lock:
        channel = bot.get_channel(VOICE_CHANNEL_ID)
        if channel is None:
            log.error("Voice channel not found. Check VOICE_CHANNEL_ID.")
            return False

        guild = channel.guild
        voice_client = guild.voice_client

        for attempt in range(1, 4):  # try up to 3 times per call
            try:
                if voice_client is None or not voice_client.is_connected():
                    if voice_client is not None:
                        try:
                            await voice_client.disconnect(force=True)
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                    voice_client = await channel.connect(reconnect=True, self_deaf=True, timeout=30)
                    log.info(f"Connected to {channel.name}")
                    return True
                elif voice_client.channel.id != VOICE_CHANNEL_ID:
                    await voice_client.move_to(channel)
                    log.info(f"Moved to {channel.name}")
                    return True
                else:
                    # already connected to the right channel
                    return True
            except Exception as e:
                log.warning(f"connect_to_vc attempt {attempt} failed: {e}")
                await asyncio.sleep(RECONNECT_BACKOFF * attempt)

        log.error("All connect_to_vc attempts failed this cycle.")
        return False


@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user}")
    await connect_to_vc()
    if not watchdog.is_running():
        watchdog.start()


@bot.event
async def on_guild_join(guild):
    log.info(f"Joined new server: {guild.name}")
    await connect_to_vc()


@bot.event
async def on_voice_state_update(member, before, after):
    # If the bot itself got disconnected (kicked, channel deleted, server outage, etc), rejoin immediately
    if member.id == bot.user.id and after.channel is None:
        log.warning("Disconnected from voice, rejoining...")
        await asyncio.sleep(2)
        await connect_to_vc()


@tasks.loop(seconds=WATCHDOG_INTERVAL)
async def watchdog():
    """Belt-and-suspenders check in case on_voice_state_update misses an edge case."""
    for guild in bot.guilds:
        vc = guild.voice_client
        if vc is None or not vc.is_connected():
            log.info("Watchdog detected disconnect, reconnecting...")
            await connect_to_vc()


@watchdog.before_loop
async def before_watchdog():
    await bot.wait_until_ready()


keep_alive()
bot.run(TOKEN)
