import discord
from discord.ext import commands, tasks
import asyncio
import time
import sys
import os
import logging
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

sys.stdout.reconfigure(line_buffering=True)

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
VOICE_CHANNEL_ID = int(os.getenv("VOICE_CHANNEL_ID"))

# How often the watchdog checks the connection is still alive (seconds)
WATCHDOG_INTERVAL = 60
# How long to wait between reconnect attempts if one fails (seconds)
RECONNECT_BACKOFF = 10
# How long to wait after a disconnect before even trying to reconnect (seconds)
POST_DISCONNECT_DELAY = 8

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

        try:
            if voice_client is None:
                # No forced disconnect/teardown here, nothing to tear down yet.
                voice_client = await channel.connect(reconnect=True, self_deaf=True, timeout=30)
                log.info(f"Connected to {channel.name}")
                return True
            elif not voice_client.is_connected():
                # Let discord.py's own reconnect=True machinery try first rather than
                # us forcing a fresh teardown, that churn is likely what's causing
                # extra disconnects on an already shaky connection.
                await asyncio.sleep(POST_DISCONNECT_DELAY)
                if guild.voice_client is not None and guild.voice_client.is_connected():
                    return True  # it recovered on its own, nothing more to do
                try:
                    await voice_client.disconnect(force=True)
                except Exception:
                    pass
                await asyncio.sleep(RECONNECT_BACKOFF)
                voice_client = await channel.connect(reconnect=True, self_deaf=True, timeout=30)
                log.info(f"Reconnected to {channel.name}")
                return True
            elif voice_client.channel.id != VOICE_CHANNEL_ID:
                await voice_client.move_to(channel)
                log.info(f"Moved to {channel.name}")
                return True
            else:
                return True  # already connected to the right channel
        except Exception as e:
            log.warning(f"connect_to_vc failed: {e}")
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
        await asyncio.sleep(POST_DISCONNECT_DELAY)
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


BACKOFF_STATE_FILE = "/tmp/vc_bot_login_backoff.txt"
LOGIN_TIMEOUT = 90  # seconds to allow a login attempt before treating it as hung and giving up


def _get_last_backoff():
    try:
        with open(BACKOFF_STATE_FILE, "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def _save_backoff(seconds):
    try:
        with open(BACKOFF_STATE_FILE, "w") as f:
            f.write(str(seconds))
    except Exception as e:
        log.warning(f"Could not persist backoff state: {e}")


def _clear_backoff():
    try:
        os.remove(BACKOFF_STATE_FILE)
    except OSError:
        pass


async def _start_with_timeout():
    log.info("Attempting to log in...")
    try:
        async with bot:
            await asyncio.wait_for(bot.start(TOKEN), timeout=LOGIN_TIMEOUT)
        log.info("bot.start() exited cleanly.")
        _clear_backoff()
    except asyncio.TimeoutError:
        log.warning(f"Login attempt hung for over {LOGIN_TIMEOUT}s with no response, giving up on this attempt.")
        raise


def run_with_backoff():
    """bot.run() can hang forever without ever raising, even in a completely fresh process,
    that's what happened here, so retrying in the same process (the old approach) isn't
    enough on its own. Instead we drive the connection ourselves with bot.start() inside
    asyncio.wait_for, which forces a hard ceiling on how long a single attempt is allowed
    to hang before we give up on it. Either way (raised exception or forced timeout), we
    sleep for a real, growing cooldown and then let the process actually exit, so Render
    spins up a genuinely fresh one next time. The backoff duration is persisted to a small
    local file so it keeps growing across restarts instead of resetting every time."""
    max_backoff = 3600  # cap at 1 hour
    try:
        asyncio.run(_start_with_timeout())
        return
    except Exception as e:
        log.warning(f"Login attempt failed: {e}")

    prev = _get_last_backoff()
    backoff = 60 if prev == 0 else min(prev * 2, max_backoff)
    _save_backoff(backoff)
    log.info(f"Sleeping {backoff}s before exiting, Render will restart with a fresh process after that.")
    time.sleep(backoff)
    sys.exit(1)


keep_alive()
run_with_backoff()
