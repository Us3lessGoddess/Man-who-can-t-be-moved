import discord
from discord.ext import commands, tasks
import asyncio
import time
import sys
import threading
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
HARD_WATCHDOG_TIMEOUT = LOGIN_TIMEOUT + 60  # absolute last-resort ceiling, see _hard_watchdog below

_login_resolved = threading.Event()


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


def _hard_watchdog(timeout_seconds):
    """Last-resort safety net. asyncio.wait_for's cancellation is supposed to unstick a hung
    bot.start(), but it isn't guaranteed to. This runs in a genuinely separate OS thread, so
    it keeps ticking no matter what's stuck in the asyncio world, and force-kills the whole
    process at the OS level if nothing has resolved within the timeout."""
    if not _login_resolved.wait(timeout=timeout_seconds):
        log.warning(f"HARD WATCHDOG: still stuck after {timeout_seconds}s, force-killing the process.")
        os._exit(1)


async def _start_with_timeout():
    log.info("Attempting to log in...")
    async with bot:
        start_task = asyncio.create_task(bot.start(TOKEN))
        ready_task = asyncio.create_task(bot.wait_until_ready())
        done, _ = await asyncio.wait(
            {start_task, ready_task}, timeout=LOGIN_TIMEOUT, return_when=asyncio.FIRST_COMPLETED
        )
        if ready_task in done:
            # Actually connected within the time limit, this is healthy. From here on let
            # bot.start() run for as long as the bot stays up, no artificial timeout, that
            # was the bug: applying LOGIN_TIMEOUT to the whole session instead of just the
            # handshake was forcibly killing a perfectly good connection every 90 seconds.
            log.info("Logged in and ready.")
            _login_resolved.set()
            _clear_backoff()
            await start_task
        elif start_task in done:
            # bot.start() itself ended before ever becoming ready, a real login failure
            _login_resolved.set()
            exc = start_task.exception()
            if exc:
                raise exc
        else:
            log.warning(f"Login attempt hung for over {LOGIN_TIMEOUT}s with no response, giving up on this attempt.")
            start_task.cancel()
            ready_task.cancel()
            _login_resolved.set()
            raise TimeoutError("Login timed out")


def run_with_backoff():
    """bot.run() can hang forever without ever raising, even in a completely fresh process,
    and even with an asyncio-level timeout wrapping it, that's what happened here: an
    asyncio.wait_for timeout was in place and still didn't unstick an 8-hour hang. So on top
    of that, a genuinely separate hard watchdog thread guarantees the process dies within a
    bounded window no matter what it's stuck on. Either way, we sleep for a real, growing
    cooldown and then let the process actually exit, so Render spins up a genuinely fresh one
    next time. The backoff duration is persisted to a small local file so it keeps growing
    across restarts instead of resetting every time."""
    max_backoff = 3600  # cap at 1 hour
    watchdog_thread = threading.Thread(target=_hard_watchdog, args=(HARD_WATCHDOG_TIMEOUT,), daemon=True)
    watchdog_thread.start()
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
