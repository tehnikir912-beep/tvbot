import asyncio
import gzip
import io
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html import escape as html_escape

import aiohttp
from aiohttp import web
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
    _ZONEINFO_AVAILABLE = True
except Exception:
    _ZONEINFO_AVAILABLE = False
    ZoneInfo = None

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "tvbot.db"
EPG_URL = "http://epg.one/ru.xml.gz"
SCHEDULE_LIMIT = 12
EPG_UPDATE_INTERVAL = 12 * 3600
EPG_CHECK_INTERVAL = 3600
REMINDER_TICK = 30
SCHEMA_VERSION = 4

if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в файле .env")

TIMEZONES = [
    ("Europe/Minsk", "Минск (UTC+3)", 3),
    ("Europe/Moscow", "Москва (UTC+3)", 3),
    ("Europe/Kyiv", "Киев (UTC+2/3)", 2),
    ("Europe/Warsaw", "Варшава (UTC+1/2)", 1),
    ("Europe/Berlin", "Берлин (UTC+1/2)", 1),
    ("Europe/London", "Лондон (UTC+0/1)", 0),
    ("Asia/Yerevan", "Ереван (UTC+4)", 4),
    ("Asia/Tbilisi", "Тбилиси (UTC+4)", 4),
    ("Asia/Baku", "Баку (UTC+4)", 4),
    ("Asia/Dubai", "Дубай (UTC+4)", 4),
    ("Asia/Yekaterinburg", "Екатеринбург (UTC+5)", 5),
    ("Asia/Tashkent", "Ташкент (UTC+5)", 5),
    ("Asia/Novosibirsk", "Новосибирск (UTC+7)", 7),
    ("Asia/Vladivostok", "Владивосток (UTC+10)", 10),
    ("America/New_York", "Нью-Йорк (UTC-5/-4)", -5),
    ("UTC", "UTC", 0),
]

_TZ_OFFSETS = {name: offset for name, _, offset in TIMEZONES}
_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def get_tz_by_name(tz_name):
    if _ZONEINFO_AVAILABLE and tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            pass
    offset = _TZ_OFFSETS.get(tz_name, 3)
    return timezone(timedelta(hours=offset))


DEFAULT_TZ = get_tz_by_name("Europe/Minsk")

dp = Dispatcher()
search_modes = {}
last_context = {}
last_channel_view = {}
_epg_task = None
_reminder_task = None

keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📡 Найти канал")],
        [
            KeyboardButton(text="📺 Мои каналы"),
            KeyboardButton(text="🕐 Сейчас и далее"),
        ],
        [
            KeyboardButton(text="⭐ Избранные передачи"),
            KeyboardButton(text="⏰ Мои напоминания"),
        ],
        [KeyboardButton(text="⚙️ Настройки")],
    ],
    resize_keyboard=True,
)


def esc(text):
    return html_escape(text or "")


def shorten(text, max_len):
    text = text or ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


async def safe_edit(message, text, kb):
    try:
        await message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            print(f"[edit] {e}")
    except Exception as e:
        print(f"[edit] {type(e).__name__}: {e}")


# ---------- DB ----------

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cursor = await db.execute("SELECT value FROM meta WHERE key = 'schema_version'")
        row = await cursor.fetchone()
        current = int(row[0]) if row else 0

        if current < 1:
            await db.execute("DROP TABLE IF EXISTS favorites")
            await db.execute("DROP TABLE IF EXISTS channels")
            await db.execute("DROP TABLE IF EXISTS programmes")
            await db.execute("DROP TABLE IF EXISTS program_favorites")

            await db.execute("""
                CREATE TABLE favorites (
                    user_id INTEGER NOT NULL,
                    channel_id TEXT NOT NULL,
                    PRIMARY KEY (user_id, channel_id)
                )
            """)
            await db.execute("""
                CREATE TABLE channels (
                    channel_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    name_lower TEXT NOT NULL
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_channels_lower ON channels(name_lower)")
            await db.execute("""
                CREATE TABLE programmes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    start_time INTEGER NOT NULL,
                    stop_time INTEGER
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_programme_start ON programmes(start_time)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_programme_channel ON programmes(channel_id)")
            await db.execute("""
                CREATE TABLE program_favorites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    channel_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    start_time INTEGER NOT NULL,
                    description TEXT,
                    reminder_minutes INTEGER DEFAULT NULL,
                    reminder_sent INTEGER DEFAULT 0,
                    repeat_mode TEXT DEFAULT 'none',
                    UNIQUE(user_id, channel_id, title, start_time)
                )
            """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                timezone TEXT
            )
        """)

        cursor = await db.execute("PRAGMA table_info(program_favorites)")
        cols = [r[1] for r in await cursor.fetchall()]
        if "reminder_minutes" not in cols:
            await db.execute("ALTER TABLE program_favorites ADD COLUMN reminder_minutes INTEGER DEFAULT NULL")
        if "reminder_sent" not in cols:
            await db.execute("ALTER TABLE program_favorites ADD COLUMN reminder_sent INTEGER DEFAULT 0")
        if "repeat_mode" not in cols:
            await db.execute("ALTER TABLE program_favorites ADD COLUMN repeat_mode TEXT DEFAULT 'none'")

        await db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        await db.commit()


async def get_meta(key):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return int(row[0]) if row else 0


async def set_meta(key, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value)))
        await db.commit()


async def get_user_tz_name(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT timezone FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
    return row[0] if row and row[0] else "Europe/Minsk"


async def get_user_tz(user_id):
    name = await get_user_tz_name(user_id)
    return get_tz_by_name(name)


async def set_user_tz(user_id, tz_name):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users (user_id, timezone) VALUES (?, ?)",
            (user_id, tz_name),
        )
        await db.commit()


# ---------- Утилиты ----------

def xmltv_time(value):
    if not value:
        return None
    value = value.strip()
    try:
        if len(value) > 14:
            dt = datetime.strptime(value[:20], "%Y%m%d%H%M%S %z")
        else:
            dt = datetime.strptime(value[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def local_time(timestamp, tz=None):
    if not timestamp:
        return "?"
    return datetime.fromtimestamp(timestamp, tz or DEFAULT_TZ).strftime("%d.%m %H:%M")


def short_time(timestamp, now=None, tz=None):
    if not timestamp:
        return "?"
    if now is None:
        now = time.time()
    _tz = tz or DEFAULT_TZ
    dt = datetime.fromtimestamp(timestamp, _tz)
    now_dt = datetime.fromtimestamp(now, _tz)
    if dt.date() == now_dt.date():
        return dt.strftime("%H:%M")
    return dt.strftime("%d.%m %H:%M")


def date_start_ts(target_date, tz):
    return int(datetime(
        target_date.year, target_date.month, target_date.day,
        tzinfo=tz
    ).timestamp())


# ---------- EPG ----------

async def fetch_and_store_epg():
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(EPG_URL) as response:
            response.raise_for_status()
            data = await response.read()

    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)

    channels_count = 0
    programmes_count = 0

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM programmes")
        await db.execute("DELETE FROM channels")
        await db.commit()

        batch = []
        for _, elem in ET.iterparse(io.BytesIO(data), events=("end",)):
            if elem.tag == "channel":
                channel_id = elem.attrib.get("id", "")
                names = [
                    c.text.strip() for c in elem
                    if c.tag == "display-name" and c.text and c.text.strip()
                ]
                if channel_id and names:
                    name = names[0]
                    await db.execute(
                        "INSERT OR REPLACE INTO channels (channel_id, name, name_lower) VALUES (?, ?, ?)",
                        (channel_id, name, name.lower()),
                    )
                    channels_count += 1
                elem.clear()
            elif elem.tag == "programme":
                channel_id = elem.attrib.get("channel", "")
                title = next((c.text.strip() for c in elem if c.tag == "title" and c.text and c.text.strip()), "")
                description = next((c.text.strip() for c in elem if c.tag == "desc" and c.text and c.text.strip()), "")
                start = xmltv_time(elem.attrib.get("start", ""))
                stop = xmltv_time(elem.attrib.get("stop", ""))
                if channel_id and title and start:
                    batch.append((channel_id, title, description, start, stop))
                if len(batch) >= 2000:
                    await db.executemany(
                        "INSERT INTO programmes (channel_id, title, description, start_time, stop_time) VALUES (?, ?, ?, ?, ?)",
                        batch,
                    )
                    programmes_count += len(batch)
                    batch.clear()
                elem.clear()

        if batch:
            await db.executemany(
                "INSERT INTO programmes (channel_id, title, description, start_time, stop_time) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
            programmes_count += len(batch)

        await db.commit()

    return channels_count, programmes_count


async def epg_updater_loop():
    while True:
        try:
            last = await get_meta("last_epg_update")
            if int(time.time()) - last > EPG_UPDATE_INTERVAL:
                print("[EPG] auto-update start")
                cc, pc = await fetch_and_store_epg()
                await set_meta("last_epg_update", int(time.time()))
                print(f"[EPG] done: {cc} ch, {pc} prog")
        except Exception as e:
            print(f"[EPG] {e}")
        await asyncio.sleep(EPG_CHECK_INTERVAL)


# ---------- Reminder loop ----------

async def reminder_loop(bot: Bot):
    while True:
        try:
            now = int(time.time())
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    """
                    SELECT pf.id, pf.user_id, pf.channel_id, pf.title, pf.start_time,
                           pf.reminder_minutes, pf.repeat_mode
                    FROM program_favorites pf
                    WHERE pf.reminder_minutes IS NOT NULL
                      AND pf.reminder_sent = 0
                      AND pf.start_time - pf.reminder_minutes * 60 <= ?
                      AND pf.start_time > ?
                    """,
                    (now, now),
                )
                rows = await cursor.fetchall()

                for fav_id, user_id, channel_id, title, start, minutes, repeat_mode in rows:
                    tz = await get_user_tz(user_id)

                    c2 = await db.execute("SELECT name FROM channels WHERE channel_id = ?", (channel_id,))
                    ch_row = await c2.fetchone()
                    ch_name = ch_row[0] if ch_row else "?"

                    remaining = start - now
                    if remaining < 60:
                        eta = "меньше минуты"
                    elif remaining < 3600:
                        eta = f"{remaining // 60} мин"
                    else:
                        h = remaining // 3600
                        m = (remaining % 3600) // 60
                        eta = f"{h} ч {m} мин" if m else f"{h} ч"

                    repeat_note = ""
                    if repeat_mode == "daily":
                        repeat_note = "\n🔁 Напомню снова завтра в это же время."
                    elif repeat_mode == "weekly":
                        repeat_note = "\n🔁 Напомню снова через неделю."

                    text = (
                        f"⏰ <b>Напоминание!</b>\n\n"
                        f"Через {eta} начнётся:\n"
                        f"🎬 <b>{esc(title)}</b>\n"
                        f"📺 {esc(ch_name)}\n"
                        f"🕒 {local_time(start, tz)}"
                        f"{repeat_note}"
                    )
                    try:
                        await bot.send_message(user_id, text, parse_mode="HTML")
                    except Exception as e:
                        print(f"[reminder] to {user_id}: {e}")

                    moved_to_next = False
                    if repeat_mode in ("daily", "weekly"):
                        min_gap = 20 * 3600 if repeat_mode == "daily" else 6 * 24 * 3600
                        threshold = start + min_gap
                        c3 = await db.execute(
                            """
                            SELECT start_time, description FROM programmes
                            WHERE channel_id = ? AND title = ? AND start_time >= ?
                            ORDER BY start_time ASC LIMIT 1
                            """,
                            (channel_id, title, threshold),
                        )
                        next_row = await c3.fetchone()
                        if next_row:
                            new_start, new_desc = next_row
                            await db.execute(
                                "UPDATE program_favorites SET start_time = ?, description = ?, reminder_sent = 0 WHERE id = ?",
                                (new_start, new_desc or "", fav_id),
                            )
                            moved_to_next = True
                            print(f"[reminder] {fav_id} moved to next: {new_start}")

                    if not moved_to_next:
                        await db.execute("UPDATE program_favorites SET reminder_sent = 1 WHERE id = ?", (fav_id,))
                await db.commit()
        except Exception as e:
            print(f"[reminder_loop] {e}")
        await asyncio.sleep(REMINDER_TICK)


# ---------- Запросы ----------

async def search_channels(query):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT channel_id, name FROM channels WHERE name_lower LIKE ? ORDER BY name LIMIT 15",
            (f"%{query.lower()}%",),
        )
        return await cursor.fetchall()


async def get_channel_name(channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT name FROM channels WHERE channel_id = ?", (channel_id,))
        row = await cursor.fetchone()
        return row[0] if row else None


async def get_channel_dates(channel_id, tz):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT start_time FROM programmes "
            "WHERE channel_id = ? AND (stop_time IS NULL OR stop_time >= ?) "
            "ORDER BY start_time",
            (channel_id, now),
        )
        rows = await cursor.fetchall()

    today = datetime.fromtimestamp(now, tz).date()
    dates = set()
    for (start,) in rows:
        d = datetime.fromtimestamp(start, tz).date()
        if start <= now:
            dates.add(today)
        else:
            dates.add(d)
    return sorted(dates)


async def get_programmes_for_date(channel_id, target_date, tz, limit, offset):
    start_ts = date_start_ts(target_date, tz)
    end_ts = date_start_ts(target_date + timedelta(days=1), tz)
    now = int(time.time())
    today = datetime.fromtimestamp(now, tz).date()

    async with aiosqlite.connect(DB_PATH) as db:
        if target_date == today:
            cursor = await db.execute(
                """
                SELECT id, title, description, start_time, stop_time
                FROM programmes
                WHERE channel_id = ?
                  AND start_time < ?
                  AND (stop_time IS NULL OR stop_time >= ?)
                ORDER BY start_time
                LIMIT ? OFFSET ?
                """,
                (channel_id, end_ts, now, limit + 1, offset),
            )
        else:
            cursor = await db.execute(
                """
                SELECT id, title, description, start_time, stop_time
                FROM programmes
                WHERE channel_id = ?
                  AND start_time >= ?
                  AND start_time < ?
                  AND (stop_time IS NULL OR stop_time >= ?)
                ORDER BY start_time
                LIMIT ? OFFSET ?
                """,
                (channel_id, start_ts, end_ts, now, limit + 1, offset),
            )
        return await cursor.fetchall()


async def get_program(programme_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, channel_id, title, description, start_time, stop_time FROM programmes WHERE id = ?",
            (programme_id,),
        )
        return await cursor.fetchone()


async def get_current_and_next(channel_id):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, title, start_time, stop_time FROM programmes "
            "WHERE channel_id = ? AND start_time <= ? AND (stop_time IS NULL OR stop_time >= ?) "
            "ORDER BY start_time DESC LIMIT 1",
            (channel_id, now, now)
        )
        current = await cur.fetchone()

        nxt = await db.execute(
            "SELECT id, title, start_time, stop_time FROM programmes "
            "WHERE channel_id = ? AND start_time > ? "
            "ORDER BY start_time ASC LIMIT 1",
            (channel_id, now)
        )
        next_prog = await nxt.fetchone()
        return current, next_prog


# ---------- Мои каналы ----------

async def add_my_channel(user_id, channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO favorites (user_id, channel_id) VALUES (?, ?)", (user_id, channel_id))
        await db.commit()


async def remove_my_channel(user_id, channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM favorites WHERE user_id = ? AND channel_id = ?", (user_id, channel_id))
        await db.commit()


async def is_my_channel(user_id, channel_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM favorites WHERE user_id = ? AND channel_id = ?", (user_id, channel_id))
        return await cursor.fetchone() is not None


async def get_my_channels(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT f.channel_id, c.name FROM favorites f
            JOIN channels c ON c.channel_id = f.channel_id
            WHERE f.user_id = ? ORDER BY c.name
            """,
            (user_id,),
        )
        return await cursor.fetchall()


# ---------- Избранные передачи ----------

async def add_program_favorite(user_id, channel_id, title, start_time, description):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT OR IGNORE INTO program_favorites (user_id, channel_id, title, start_time, description) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, channel_id, title, start_time, description or ""),
        )
        await db.commit()
        return cursor.rowcount > 0


async def remove_program_favorite_by_id(fav_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM program_favorites WHERE id = ? AND user_id = ?", (fav_id, user_id))
        await db.commit()


async def get_favorite_program_by_attrs(user_id, channel_id, title, start_time):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM program_favorites WHERE user_id=? AND channel_id=? AND title=? AND start_time=?",
            (user_id, channel_id, title, start_time)
        )
        return await cur.fetchone()


async def get_favorite_programs_for_view(user_id):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT id, channel_id, title, start_time, reminder_minutes, repeat_mode
            FROM program_favorites
            WHERE user_id = ? AND start_time > ?
            ORDER BY start_time
            LIMIT 30
            """,
            (user_id, now),
        )
        return await cursor.fetchall()


async def get_favorite_program_by_id(fav_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, channel_id, title, start_time, description, reminder_minutes, reminder_sent, repeat_mode "
            "FROM program_favorites WHERE id = ? AND user_id = ?",
            (fav_id, user_id),
        )
        return await cursor.fetchone()


async def set_program_reminder(fav_id, user_id, minutes, repeat_mode):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE program_favorites SET reminder_minutes = ?, repeat_mode = ?, reminder_sent = 0 "
            "WHERE id = ? AND user_id = ?",
            (minutes, repeat_mode, fav_id, user_id),
        )
        await db.commit()


async def set_repeat_mode(fav_id, user_id, repeat_mode):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE program_favorites SET repeat_mode = ?, reminder_sent = 0 WHERE id = ? AND user_id = ?",
            (repeat_mode, fav_id, user_id),
        )
        await db.commit()


async def clear_program_reminder(fav_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE program_favorites SET reminder_minutes = NULL, repeat_mode = 'none', reminder_sent = 0 "
            "WHERE id = ? AND user_id = ?",
            (fav_id, user_id),
        )
        await db.commit()


# ---------- Виды ----------

def _day_label(target_date, today):
    if target_date == today:
        return f"Сегодня, {target_date.strftime('%d.%m')}"
    if target_date == today + timedelta(days=1):
        return f"Завтра, {target_date.strftime('%d.%m')}"
    wd = _WEEKDAYS[target_date.weekday()]
    return f"{wd}, {target_date.strftime('%d.%m')}"


def _tab_label(target_date, today, selected):
    if target_date == today:
        base = "Сегодня"
    elif target_date == today + timedelta(days=1):
        base = "Завтра"
    else:
        base = target_date.strftime("%d.%m")
    if target_date == selected:
        return f"[{base}]"
    return base


async def build_channel_view(user_id, channel_id, date_str=None, offset=None, use_context=True):
    channel_name = await get_channel_name(channel_id) or "Неизвестный канал"
    tz = await get_user_tz(user_id)

    if use_context and (date_str is None or offset is None):
        ctx = last_channel_view.get(user_id)
        if ctx and ctx[0] == channel_id:
            if date_str is None:
                date_str = ctx[1]
            if offset is None:
                offset = ctx[2]
    if offset is None:
        offset = 0

    dates = await get_channel_dates(channel_id, tz)
    now = int(time.time())
    today = datetime.fromtimestamp(now, tz).date()

    if not dates:
        lines = [f"📺 <b>{esc(channel_name)}</b>", "", "Ближайшие передачи не найдены."]
        rows = []
        in_mine = await is_my_channel(user_id, channel_id)
        if in_mine:
            rows.append([InlineKeyboardButton(text="💔 Убрать из моих каналов", callback_data=f"fdc:{channel_id}")])
        else:
            rows.append([InlineKeyboardButton(text="📺 Добавить в мои каналы", callback_data=f"fac:{channel_id}")])
        ctx = last_context.get(user_id)
        if ctx:
            if ctx["type"] == "mine":
                rows.append([InlineKeyboardButton(text="🔙 К моим каналам", callback_data="mine_back")])
            elif ctx["type"] == "now_next":
                rows.append([InlineKeyboardButton(text="🔙 К «Сейчас и далее»", callback_data="now_next_back")])
            elif ctx["type"] == "search":
                rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="search_back")])
        return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)

    selected = None
    if date_str:
        try:
            selected = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            selected = None
    if selected not in dates:
        selected = dates[0]

    last_channel_view[user_id] = (channel_id, selected.strftime("%Y%m%d"), offset)

    programmes = await get_programmes_for_date(channel_id, selected, tz, SCHEDULE_LIMIT, offset)
    if not programmes and offset > 0:
        offset = 0
        last_channel_view[user_id] = (channel_id, selected.strftime("%Y%m%d"), offset)
        programmes = await get_programmes_for_date(channel_id, selected, tz, SCHEDULE_LIMIT, 0)

    has_more = len(programmes) > SCHEDULE_LIMIT
    programmes = programmes[:SCHEDULE_LIMIT]

    lines = [
        f"📺 <b>{esc(channel_name)}</b>",
        f"🗓 <b>{_day_label(selected, today)}</b>",
        "",
    ]
    if not programmes:
        lines.append("На этот день передач нет.")
    else:
        lines.append("Тапни на передачу — увидишь описание:")

    all_rows = []

    if len(dates) > 1:
        tab_buttons = [
            InlineKeyboardButton(
                text=_tab_label(d, today, selected),
                callback_data=f"ocd:{channel_id}:{d.strftime('%Y%m%d')}:0",
            )
            for d in dates
        ]
        for i in range(0, len(tab_buttons), 3):
            all_rows.append(tab_buttons[i:i + 3])

    for pid, title, desc, start, stop in programmes:
        live = start <= now and (stop is None or stop >= now)
        icon = "🔴" if live else "🕒"
        time_only = datetime.fromtimestamp(start, tz).strftime("%H:%M")
        all_rows.append([InlineKeyboardButton(
            text=f"{icon} {time_only} — {title[:30]}",
            callback_data=f"p:{pid}",
        )])

    selected_key = selected.strftime("%Y%m%d")
    page_row = []
    if offset > 0:
        page_row.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"ocd:{channel_id}:{selected_key}:{max(0, offset - SCHEDULE_LIMIT)}",
        ))
    if has_more:
        page_row.append(InlineKeyboardButton(
            text="Ещё ➡️",
            callback_data=f"ocd:{channel_id}:{selected_key}:{offset + SCHEDULE_LIMIT}",
        ))
    if page_row:
        all_rows.append(page_row)

    in_mine = await is_my_channel(user_id, channel_id)
    if in_mine:
        all_rows.append([InlineKeyboardButton(text="💔 Убрать из моих каналов", callback_data=f"fdc:{channel_id}")])
    else:
        all_rows.append([InlineKeyboardButton(text="📺 Добавить в мои каналы", callback_data=f"fac:{channel_id}")])

    ctx = last_context.get(user_id)
    if ctx:
        if ctx["type"] == "mine":
            all_rows.append([InlineKeyboardButton(text="🔙 К моим каналам", callback_data="mine_back")])
        elif ctx["type"] == "now_next":
            all_rows.append([InlineKeyboardButton(text="🔙 К «Сейчас и далее»", callback_data="now_next_back")])
        elif ctx["type"] == "search":
            all_rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="search_back")])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=all_rows)


async def build_program_view(user_id, programme_id):
    row = await get_program(programme_id)
    if not row:
        return None, None

    pid, channel_id, title, description, start, stop = row
    channel_name = await get_channel_name(channel_id) or "?"
    tz = await get_user_tz(user_id)
    now = int(time.time())

    start_dt = datetime.fromtimestamp(start, tz)
    time_str = start_dt.strftime("%d.%m %H:%M")
    if stop:
        stop_dt = datetime.fromtimestamp(stop, tz)
        if stop_dt.date() == start_dt.date():
            time_str += f" — {stop_dt.strftime('%H:%M')}"
        else:
            time_str += f" — {stop_dt.strftime('%d.%m %H:%M')}"

    is_live = start <= now and (stop is None or stop >= now)
    is_finished = stop is not None and stop < now

    status = ""
    if is_live:
        status = "\n\n🔴 <b>Идёт сейчас</b>"
    elif is_finished:
        status = "\n\n⚪ Передача завершилась."

    text = (
        f"🎬 <b>{esc(title)}</b>\n"
        f"📺 Канал: {esc(channel_name)}\n"
        f"🕒 {time_str}"
        f"{status}\n\n"
        f"{esc(description[:800]) if description else 'Описание отсутствует.'}"
    )

    ctx = last_context.get(user_id)
    if ctx and ctx["type"] == "now_next":
        back_btn = InlineKeyboardButton(text="🔙 К «Сейчас и далее»", callback_data="now_next_back")
    else:
        ctx_view = last_channel_view.get(user_id)
        if ctx_view and ctx_view[0] == channel_id:
            back_cb = f"ocd:{channel_id}:{ctx_view[1]}:{ctx_view[2]}"
        else:
            back_cb = f"oc:{channel_id}"
        back_btn = InlineKeyboardButton(text="🔙 К каналу", callback_data=back_cb)

    rows = []

    if not is_live and not is_finished:
        fav_row = await get_favorite_program_by_attrs(user_id, channel_id, title, start)
        if fav_row:
            fav_btn = InlineKeyboardButton(text="💔 Убрать из избранного", callback_data=f"pfd:{pid}")
        else:
            fav_btn = InlineKeyboardButton(text="⭐ Добавить в избранные передачи", callback_data=f"pfa:{pid}")
        rows.append([fav_btn])

    rows.append([back_btn])

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def _repeat_label(mode):
    return {"none": "один раз", "daily": "ежедневно", "weekly": "еженедельно"}.get(mode or "none", "один раз")


async def build_favorite_program_view(user_id, fav_id):
    row = await get_favorite_program_by_id(fav_id, user_id)
    if not row:
        return None, None

    fav_id, channel_id, title, start, description, reminder_minutes, reminder_sent, repeat_mode = row
    channel_name = await get_channel_name(channel_id) or "?"
    tz = await get_user_tz(user_id)
    now = int(time.time())

    start_dt = datetime.fromtimestamp(start, tz)
    time_str = start_dt.strftime("%d.%m %H:%M")

    lines = [
        f"🎬 <b>{esc(title)}</b>",
        f"📺 {esc(channel_name)}",
        f"🕒 {time_str}",
        "",
        esc(description[:800]) if description else "Описание отсутствует.",
        "",
    ]

    buttons = []
    if start < now:
        lines.append("⚠️ Передача уже прошла.")
    elif reminder_minutes:
        if reminder_sent and repeat_mode == "none":
            lines.append(f"✅ Напоминание отправлено (за {reminder_minutes} мин).")
        else:
            remind_at = start - reminder_minutes * 60
            remind_dt = datetime.fromtimestamp(remind_at, tz)
            rep = f", {_repeat_label(repeat_mode)}" if repeat_mode != "none" else ""
            lines.append(f"⏰ За {reminder_minutes} мин (в {remind_dt.strftime('%H:%M')}){rep}")
        buttons.append([InlineKeyboardButton(text="⏰ Изменить напоминание", callback_data=f"fprem:{fav_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="⏰ Установить напоминание", callback_data=f"fprem:{fav_id}")])

    buttons.append([InlineKeyboardButton(text="💔 Убрать из избранного", callback_data=f"fpr:{fav_id}")])
    buttons.append([InlineKeyboardButton(text="🔙 К избранным передачам", callback_data="fav_prog_back")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons)


async def build_reminder_menu(user_id, fav_id):
    row = await get_favorite_program_by_id(fav_id, user_id)
    if not row:
        return None, None

    fav_id, channel_id, title, start, description, reminder_minutes, reminder_sent, repeat_mode = row
    tz = await get_user_tz(user_id)

    start_str = datetime.fromtimestamp(start, tz).strftime("%d.%m %H:%M")

    cur = "не установлено"
    if reminder_minutes:
        cur = f"за {reminder_minutes} мин"
        if repeat_mode != "none":
            cur += f", {_repeat_label(repeat_mode)}"

    text = (
        f"⏰ Напоминание для:\n<b>{esc(title)}</b>\n"
        f"🕒 {start_str}\n\n"
        f"Сейчас: {cur}\n\n"
        f"Выбери время:\n"
        f"<i>💡 «Ежедневно» или «Еженедельно» — бот будет напоминать о каждом новом эпизоде этой передачи в это же время.</i>"
    )

    time_opts = [
        (5, "5 мин"), (15, "15 мин"), (30, "30 мин"),
        (60, "1 час"), (120, "2 часа"), (1440, "1 день"),
    ]
    rows = []
    for i in range(0, len(time_opts), 3):
        chunk = time_opts[i:i + 3]
        row_btns = []
        for mins, label in chunk:
            mark = " ✓" if reminder_minutes == mins else ""
            row_btns.append(InlineKeyboardButton(
                text=f"{label}{mark}",
                callback_data=f"fprem_set:{fav_id}:{mins}",
            ))
        rows.append(row_btns)

    if not reminder_minutes or reminder_minutes < 1440:
        repeat_row = []
        for mode, label in [("none", "Один раз"), ("daily", "Ежедневно"), ("weekly", "Еженедельно")]:
            mark = " ✓" if (repeat_mode or "none") == mode else ""
            repeat_row.append(InlineKeyboardButton(
                text=f"{label}{mark}",
                callback_data=f"fprem_rep:{fav_id}:{mode}",
            ))
        rows.append(repeat_row)

    if reminder_minutes:
        rows.append([InlineKeyboardButton(text="🔕 Убрать напоминание", callback_data=f"fprem_del:{fav_id}")])

    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"fp:{fav_id}")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def render_my_channels(user_id):
    channels = await get_my_channels(user_id)
    if not channels:
        return None, None
    rows = [[InlineKeyboardButton(text=f"📺 {name}", callback_data=f"oc:{cid}")] for cid, name in channels]
    return "📺 <b>Мои каналы:</b>\n\nВыбери канал, чтобы увидеть расписание.", InlineKeyboardMarkup(inline_keyboard=rows)


async def render_now_and_next(user_id):
    channels = await get_my_channels(user_id)
    if not channels:
        return None, None
    tz = await get_user_tz(user_id)

    buttons = []
    for cid, name in channels:
        cur, nxt = await get_current_and_next(cid)
        ch_short = shorten(name, 16)

        if cur:
            pid, title, start, stop = cur
            title_short = shorten(title, 24)
            end_str = ""
            if stop:
                end_str = f" (до {datetime.fromtimestamp(stop, tz).strftime('%H:%M')})"
            label = f"🔴 {ch_short} · {title_short}{end_str}"
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"nnp:{pid}")])

        if nxt:
            nid, n_title, n_start, n_stop = nxt
            start_str = datetime.fromtimestamp(n_start, tz).strftime("%H:%M")
            title_short = shorten(n_title, 22)
            label = f"🕒 {ch_short} · далее: {title_short} в {start_str}"
            buttons.append([InlineKeyboardButton(text=label, callback_data=f"nnp:{nid}")])

    if not buttons:
        return "🕐 <b>Сейчас и далее</b>\n\nНа твоих каналах нет доступных передач.", None

    text = "🕐 <b>Сейчас и далее</b>\n\n<i>Тапни на передачу — увидишь описание.</i>"
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


async def render_favorite_programs(user_id):
    favs = await get_favorite_programs_for_view(user_id)
    if not favs:
        return None, None
    tz = await get_user_tz(user_id)
    now = int(time.time())
    rows = []
    for fav_id, ch_id, title, start, reminder_minutes, repeat_mode in favs:
        if reminder_minutes and repeat_mode != "none":
            prefix = "🔁⏰ "
        elif reminder_minutes:
            prefix = "⏰ "
        else:
            prefix = ""
        rows.append([InlineKeyboardButton(
            text=f"{prefix}🎬 {short_time(start, now, tz)} — {title[:25]}",
            callback_data=f"fp:{fav_id}",
        )])
    return "⭐ <b>Избранные передачи:</b>", InlineKeyboardMarkup(inline_keyboard=rows)


async def render_my_reminders(user_id):
    favs = await get_favorite_programs_for_view(user_id)
    if not favs:
        return None, None
    tz = await get_user_tz(user_id)
    now = int(time.time())
    rows = []
    has_any = False
    for fav_id, ch_id, title, start, reminder_minutes, repeat_mode in favs:
        if not reminder_minutes:
            continue
        has_any = True
        rep = "🔁 " if repeat_mode != "none" else ""
        rows.append([InlineKeyboardButton(
            text=f"{rep}⏰ {short_time(start, now, tz)} — {title[:20]} (за {reminder_minutes}м)",
            callback_data=f"fp:{fav_id}",
        )])
    if not has_any:
        return None, None
    return "⏰ <b>Мои напоминания:</b>", InlineKeyboardMarkup(inline_keyboard=rows)


async def render_settings(user_id):
    tz_name = await get_user_tz_name(user_id)
    label = next((lbl for key, lbl, _ in TIMEZONES if key == tz_name), tz_name)

    note = ""
    if not _ZONEINFO_AVAILABLE:
        note = "\n⚠️ <i>Пакет zoneinfo недоступен — используется упрощённое смещение. Установи: pip install tzdata</i>"

    text = (
        f"⚙️ <b>Настройки</b>\n\n"
        f"Часовой пояс: <b>{esc(label)}</b>\n\n"
        f"Все времена в боте показываются в этом поясе.\n"
        f"<i>ℹ️ Существующие напоминания придут в то же реальное время (хранятся в UTC).</i>"
        f"{note}\n\n"
        f"Выбери свой:"
    )
    rows = []
    pair = []
    for key, lbl, _ in TIMEZONES:
        mark = " ✓" if key == tz_name else ""
        pair.append(InlineKeyboardButton(text=f"{lbl}{mark}", callback_data=f"tz_set:{key}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def channels_keyboard(channels):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"📺 {name}", callback_data=f"oc:{cid}")] for cid, name in channels]
    )


# ---------- Хендлеры: меню ----------

@dp.message(CommandStart())
async def start_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    last_context.pop(message.from_user.id, None)
    last_channel_view.pop(message.from_user.id, None)
    await message.answer(
        "Привет! 👋 Я твой Телегид.\n\n"
        "📡 Найти канал — найти и добавить канал в «Мои каналы»\n"
        "📺 Мои каналы — быстрый доступ к твоим каналам\n"
        "🕐 Сейчас и далее — что идёт и что дальше\n"
        "⭐ Избранные передачи — сохранённые передачи с напоминаниями\n"
        "⏰ Мои напоминания — активные напоминания\n"
        "⚙️ Настройки — часовой пояс\n\n"
        "Расписание обновляется автоматически.",
        reply_markup=keyboard,
    )


@dp.message(F.text == "/updateepg")
async def update_handler(message: Message):
    await message.answer("📥 Обновляю EPG вручную...")
    try:
        cc, pc = await fetch_and_store_epg()
        await set_meta("last_epg_update", int(time.time()))
        await message.answer(f"✅ Каналов: {cc}, передач: {pc}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {type(e).__name__}")
        print(f"[manual EPG] {e}")


@dp.message(F.text == "/cancel")
async def cancel_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    last_context.pop(message.from_user.id, None)
    last_channel_view.pop(message.from_user.id, None)
    await message.answer("Ок, отменил.")


@dp.message(F.text == "📡 Найти канал")
async def find_channel_handler(message: Message):
    search_modes[message.from_user.id] = "channel"
    last_context.pop(message.from_user.id, None)
    await message.answer("📡 Введи название канала (можно частями):\nнапример: Беларусь, ОНТ, НТВ")


@dp.message(F.text == "📺 Мои каналы")
async def my_channels_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    user_id = message.from_user.id
    text, kb = await render_my_channels(user_id)
    if kb is None:
        await message.answer(
            "📺 <b>Мои каналы пусты.</b>\n\n"
            "Найди канал через «📡 Найти канал» и нажми «📺 Добавить в мои каналы».",
            parse_mode="HTML",
        )
        return
    last_context[user_id] = {"type": "mine"}
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(F.text == "🕐 Сейчас и далее")
async def now_and_next_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    user_id = message.from_user.id
    text, kb = await render_now_and_next(user_id)
    if text is None:
        await message.answer(
            "🕐 <b>Сначала добавь каналы в «Мои каналы».</b>\n\n"
            "Найди канал через «📡 Найти канал» и нажми «📺 Добавить в мои каналы».",
            parse_mode="HTML",
        )
        return
    last_context[user_id] = {"type": "now_next"}
    if kb:
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


@dp.message(F.text == "⭐ Избранные передачи")
async def favorite_programs_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    user_id = message.from_user.id
    text, kb = await render_favorite_programs(user_id)
    if kb is None:
        await message.answer(
            "⭐ <b>Избранные передачи пусты.</b>\n\n"
            "Открой канал → тапни передачу → добавь в избранные.",
            parse_mode="HTML",
        )
        return
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(F.text == "⏰ Мои напоминания")
async def my_reminders_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    user_id = message.from_user.id
    text, kb = await render_my_reminders(user_id)
    if kb is None:
        await message.answer(
            "⏰ <b>Нет активных напоминаний.</b>\n\n"
            "Открой «⭐ Избранные передачи» → выбери передачу → установи напоминание.",
            parse_mode="HTML",
        )
        return
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(F.text == "⚙️ Настройки")
async def settings_handler(message: Message):
    search_modes.pop(message.from_user.id, None)
    text, kb = await render_settings(message.from_user.id)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


# ---------- Callbacks: канал ----------

@dp.callback_query(F.data.startswith("oc:"))
async def open_channel_handler(callback: CallbackQuery):
    channel_id = callback.data[3:]
    text, kb = await build_channel_view(callback.from_user.id, channel_id, use_context=False)
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("ocd:"))
async def open_channel_day_handler(callback: CallbackQuery):
    rest = callback.data[4:]
    try:
        channel_id, date_str, page_str = rest.rsplit(":", 2)
        page = int(page_str)
    except ValueError:
        await callback.answer("Некорректная навигация", show_alert=True)
        return
    text, kb = await build_channel_view(
        callback.from_user.id, channel_id,
        date_str=date_str, offset=page, use_context=False,
    )
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("fac:"))
async def channel_add_handler(callback: CallbackQuery):
    channel_id = callback.data[4:]
    await add_my_channel(callback.from_user.id, channel_id)
    text, kb = await build_channel_view(callback.from_user.id, channel_id)
    await safe_edit(callback.message, text, kb)
    await callback.answer("📺 Канал добавлен")


@dp.callback_query(F.data.startswith("fdc:"))
async def channel_remove_handler(callback: CallbackQuery):
    channel_id = callback.data[4:]
    await remove_my_channel(callback.from_user.id, channel_id)
    text, kb = await build_channel_view(callback.from_user.id, channel_id)
    await safe_edit(callback.message, text, kb)
    await callback.answer("Убран из моих каналов")


# ---------- Callbacks: передача ----------

@dp.callback_query(F.data.startswith("p:"))
async def open_program_handler(callback: CallbackQuery):
    pid = int(callback.data[2:])
    text, kb = await build_program_view(callback.from_user.id, pid)
    if kb is None:
        await callback.answer("Передача не найдена", show_alert=True)
        return
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("nnp:"))
async def open_program_from_now_next(callback: CallbackQuery):
    pid = int(callback.data[4:])
    text, kb = await build_program_view(callback.from_user.id, pid)
    if kb is None:
        await callback.answer("Передача не найдена", show_alert=True)
        return
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("pfa:"))
async def program_fav_add(callback: CallbackQuery):
    pid = int(callback.data[4:])
    row = await get_program(pid)
    if not row:
        await callback.answer("Передача не найдена", show_alert=True)
        return
    _, channel_id, title, description, start, stop = row
    await add_program_favorite(callback.from_user.id, channel_id, title, start, description)
    await callback.answer("⭐ Добавлено в избранные передачи")
    text, kb = await build_program_view(callback.from_user.id, pid)
    if kb:
        await safe_edit(callback.message, text, kb)


@dp.callback_query(F.data.startswith("pfd:"))
async def program_fav_del(callback: CallbackQuery):
    pid = int(callback.data[4:])
    row = await get_program(pid)
    if not row:
        await callback.answer("Передача не найдена", show_alert=True)
        return
    _, channel_id, title, description, start, stop = row
    fav_row = await get_favorite_program_by_attrs(callback.from_user.id, channel_id, title, start)
    if fav_row:
        await remove_program_favorite_by_id(fav_row[0], callback.from_user.id)
    await callback.answer("Убрано из избранного")
    text, kb = await build_program_view(callback.from_user.id, pid)
    if kb:
        await safe_edit(callback.message, text, kb)


# ---------- Callbacks: избранная передача ----------

@dp.callback_query(F.data.startswith("fp:"))
async def open_fav_program_handler(callback: CallbackQuery):
    fav_id = int(callback.data[3:])
    text, kb = await build_favorite_program_view(callback.from_user.id, fav_id)
    if kb is None:
        await callback.answer("Не найдено", show_alert=True)
        return
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("fpr:"))
async def fav_program_remove(callback: CallbackQuery):
    fav_id = int(callback.data[4:])
    await remove_program_favorite_by_id(fav_id, callback.from_user.id)
    await callback.answer("Убрано из избранного")
    text, kb = await render_favorite_programs(callback.from_user.id)
    if kb is None:
        await safe_edit(callback.message, "⭐ <b>Избранные передачи пусты.</b>", None)
    else:
        await safe_edit(callback.message, text, kb)


@dp.callback_query(F.data == "fav_prog_back")
async def fav_prog_back_handler(callback: CallbackQuery):
    text, kb = await render_favorite_programs(callback.from_user.id)
    if kb is None:
        await safe_edit(callback.message, "⭐ <b>Избранные передачи пусты.</b>", None)
    else:
        await safe_edit(callback.message, text, kb)
    await callback.answer()


# ---------- Callbacks: напоминание ----------

@dp.callback_query(F.data.startswith("fprem:"))
async def open_reminder_menu(callback: CallbackQuery):
    fav_id = int(callback.data[6:])
    row = await get_favorite_program_by_id(fav_id, callback.from_user.id)
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    if row[3] < int(time.time()):
        await callback.answer("Передача уже прошла", show_alert=True)
        return
    text, kb = await build_reminder_menu(callback.from_user.id, fav_id)
    if kb is None:
        await callback.answer("Не найдено", show_alert=True)
        return
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("fprem_set:"))
async def set_reminder_handler(callback: CallbackQuery):
    parts = callback.data.split(":")
    fav_id = int(parts[1])
    minutes = int(parts[2])

    row = await get_favorite_program_by_id(fav_id, callback.from_user.id)
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    start = row[3]
    current_repeat = row[7] or "none"
    now = int(time.time())

    if start < now:
        await callback.answer("Передача уже прошла", show_alert=True)
        return
    if start - minutes * 60 <= now:
        await callback.answer(
            f"Слишком поздно: передача начнётся через {(start - now) // 60} мин",
            show_alert=True,
        )
        return

    if minutes >= 1440 and current_repeat != "none":
        current_repeat = "none"
        await callback.answer("Повтор отключён (за 1 день только разово)")
    else:
        await callback.answer(f"⏰ За {minutes} мин")

    await set_program_reminder(fav_id, callback.from_user.id, minutes, current_repeat)
    text, kb = await build_reminder_menu(callback.from_user.id, fav_id)
    await safe_edit(callback.message, text, kb)


@dp.callback_query(F.data.startswith("fprem_rep:"))
async def set_repeat_handler(callback: CallbackQuery):
    parts = callback.data.split(":")
    fav_id = int(parts[1])
    mode = parts[2]

    row = await get_favorite_program_by_id(fav_id, callback.from_user.id)
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return
    minutes = row[5]
    if not minutes:
        await callback.answer("Сначала выбери время", show_alert=True)
        return
    if minutes >= 1440 and mode != "none":
        await callback.answer("За 1 день только разово. Смени время.", show_alert=True)
        return

    await set_repeat_mode(fav_id, callback.from_user.id, mode)
    text, kb = await build_reminder_menu(callback.from_user.id, fav_id)
    await safe_edit(callback.message, text, kb)
    await callback.answer("Сохранено")


@dp.callback_query(F.data.startswith("fprem_del:"))
async def remove_reminder_handler(callback: CallbackQuery):
    fav_id = int(callback.data[10:])
    await clear_program_reminder(fav_id, callback.from_user.id)
    text, kb = await build_favorite_program_view(callback.from_user.id, fav_id)
    await safe_edit(callback.message, text, kb)
    await callback.answer("Напоминание отменено")


# ---------- Callbacks: часовой пояс ----------

@dp.callback_query(F.data.startswith("tz_set:"))
async def tz_set_handler(callback: CallbackQuery):
    tz_name = callback.data[7:]
    if not any(k == tz_name for k, _, _ in TIMEZONES):
        await callback.answer("Неизвестный пояс", show_alert=True)
        return
    await set_user_tz(callback.from_user.id, tz_name)
    text, kb = await render_settings(callback.from_user.id)
    await safe_edit(callback.message, text, kb)
    lbl = next((l for k, l, _ in TIMEZONES if k == tz_name), tz_name)
    await callback.answer(f"Часовой пояс: {lbl}")


# ---------- Навигация ----------

@dp.callback_query(F.data == "mine_back")
async def mine_back_handler(callback: CallbackQuery):
    text, kb = await render_my_channels(callback.from_user.id)
    if kb is None:
        await safe_edit(callback.message, "📺 <b>Мои каналы пусты.</b>", None)
    else:
        await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data == "now_next_back")
async def now_next_back_handler(callback: CallbackQuery):
    text, kb = await render_now_and_next(callback.from_user.id)
    if text is None:
        await safe_edit(callback.message, "🕐 <b>Мои каналы пусты.</b>", None)
    else:
        await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data == "search_back")
async def search_back_handler(callback: CallbackQuery):
    ctx = last_context.get(callback.from_user.id)
    if not ctx or ctx["type"] != "search":
        await callback.answer("Нечего показывать", show_alert=True)
        return
    title, channels = ctx["data"]
    await safe_edit(callback.message, title, channels_keyboard(channels))
    await callback.answer()


# ---------- Текстовый поиск ----------

@dp.message()
async def message_handler(message: Message):
    if not message.text:
        return

    text = message.text.strip()
    user_id = message.from_user.id
    mode = search_modes.get(user_id)

    if mode != "channel":
        await message.answer("Выбери раздел в меню.")
        return

    results = await search_channels(text)
    if not results:
        await message.answer(
            f"По запросу «{esc(text)}» ничего не найдено.\n"
            "Попробуй короче (например, «белар», «онт», «нтв»).",
            parse_mode="HTML",
        )
        return

    title = f"📡 Найдено каналов: {len(results)}"
    last_context[user_id] = {"type": "search", "data": (title, list(results))}
    await message.answer(title, reply_markup=channels_keyboard(results), parse_mode="HTML")


# ---------- Запуск ----------

async def main():
    global _epg_task, _reminder_task
    await init_db()
    bot = Bot(token=TOKEN)

    # --- Мини-сервер для Render Web Service ---
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")
    # -----------------------------------------

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM channels")
        row = await cursor.fetchone()
        channels_exist = row[0] > 0 if row else False

    if not channels_exist:
        print("[startup] loading EPG...")
        try:
            cc, pc = await fetch_and_store_epg()
            await set_meta("last_epg_update", int(time.time()))
            print(f"[startup] {cc} ch, {pc} prog")
        except Exception as e:
            print(f"[startup] {e}")

    _epg_task = asyncio.create_task(epg_updater_loop())
    _reminder_task = asyncio.create_task(reminder_loop(bot))

    try:
        print("Телегид запущен.")
        await dp.start_polling(bot, tasks_concurrency_limit=10)
    finally:
        for t in (_epg_task, _reminder_task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
