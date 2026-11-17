import asyncio
import io
import logging
import os
import random
import re
import time
from collections import defaultdict, deque

from telethon import TelegramClient, errors, events, functions, types, utils
from telethon.sessions import StringSession

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("xvip")

# ---------- config ----------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
STRING_SESSION = os.environ["STRING_SESSION"]  # no /login flow, set manually
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()}

# per pipeline: <NAME>_CONVERTER (@username of converter bot), <NAME>_DEST (channel id)
PIPELINES = {}
ENV_SRC = {}  # pipeline -> raw tokens from <NAME>_SOURCE_CHANNELS
ENV_SOURCES = defaultdict(set)  # chat_id -> {pipeline}, resolved at startup


def _env(*names):
    for n in names:
        v = os.getenv(n, "").strip()
        if v:
            return v
    return ""


for _n in ("tera", "disk", "flezen"):
    _N = _n.upper()
    _conv = _env(f"{_N}_CONVERTER_BOT", f"{_N}_CONVERTER")
    _dest = _env(f"{_N}_DESTINATION", f"{_N}_DEST")
    ENV_SRC[_n] = _env(f"{_N}_SOURCE_CHANNELS").replace(",", " ").split()
    if _conv and _dest:
        _is_id = _dest.lstrip("-").isdigit()
        PIPELINES[_n] = {
            "converter": int(_conv) if _conv.lstrip("-").isdigit() else _conv,
            "dest": int(_dest) if _is_id else _dest,  # numeric id (channel) or @username (bot)
            "via_user": not _is_id,  # bots can't message bots, so userbot sends to a bot dest
            "conv_id": None,
        }

SEND_DELAY = float(os.getenv("SEND_DELAY", "2.5"))  # gap between sends to a converter
POST_DELAY = float(os.getenv("POST_DELAY", "3"))  # gap between posts to a destination
conv_q = defaultdict(asyncio.Queue)  # converter -> FIFO queue of source messages
post_q = defaultdict(asyncio.Queue)  # destination -> FIFO queue of ready posts
bg_tasks = []  # keep worker task refs alive

user = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
bot = TelegramClient("bot_session", API_ID, API_HASH)

# ---------- state ----------
sources = defaultdict(set)  # chat_id -> {pipeline}
titles = {}
stats = defaultdict(lambda: {"in": 0, "out": 0, "skip": 0, "err": 0})
handlers = []  # (callback, event) currently attached to userbot

_seen, _seen_set = deque(maxlen=5000), set()


def dup(key) -> bool:
    """Dedup set: True if already processed."""
    if key in _seen_set:
        return True
    if len(_seen) == _seen.maxlen:
        _seen_set.discard(_seen[0])
    _seen.append(key)
    _seen_set.add(key)
    return False


def is_admin(e) -> bool:
    return e.is_private and e.sender_id in ADMIN_IDS


# ---------- sources / handler reload ----------
async def sync_sources() -> bool:
    """Pull sources from Supabase. Returns True if changed. Keeps cache if DB is down."""
    rows = await db.fetch_sources()
    if rows is None:
        return False
    new, t = defaultdict(set), {}
    for r in rows:
        cid = int(r["chat_id"])
        new[cid].add(r["pipeline"])
        t[cid] = r.get("title") or str(cid)
    for cid, ps in ENV_SOURCES.items():
        new[cid] |= ps
    if new == sources:
        return False
    sources.clear()
    sources.update(new)
    titles.update(t)
    return True


async def reload_handlers():
    """Explicit add/remove (no decorators) so handlers never double-register."""
    for cb, ev in handlers:
        user.remove_event_handler(cb, ev)
    handlers.clear()
    ids = list(sources)
    if not ids:
        return
    for cb, ev in (
        (on_source, events.NewMessage(chats=ids)),
        (on_deleted, events.MessageDeleted(chats=ids)),
    ):
        user.add_event_handler(cb, ev)
        handlers.append((cb, ev))
    log.info("Handlers reloaded for %d source(s)", len(ids))


async def refresh_loop():
    while True:
        await asyncio.sleep(300)
        try:
            if await sync_sources():
                await reload_handlers()
        except Exception:
            log.exception("refresh_loop")


# ---------- pipeline: source -> converter -> destination ----------
URL_RE = re.compile(r"https?://\S+|t\.me/\S+", re.I)
PERMANENT = tuple(
    getattr(errors, n)
    for n in (
        "ChatWriteForbiddenError", "ChatAdminRequiredError", "UserIsBlockedError", "ChannelPrivateError",
        "PeerIdInvalidError", "ChatIdInvalidError", "MediaEmptyError", "PhotoInvalidError",
    )
    if hasattr(errors, n)
)


def pace(d: float) -> float:
    """Natural-looking gap: base delay with random jitter."""
    return d * random.uniform(0.8, 1.4)


def has_link(msg) -> bool:
    if URL_RE.search(msg.raw_text or ""):
        return True
    return any(isinstance(e, (types.MessageEntityTextUrl, types.MessageEntityUrl)) for e in (msg.entities or []))


async def with_retry(fn, what: str, attempts: int = 6):
    """FloodWait: wait exactly as told, retry forever. Other errors: backoff, up to `attempts`.
    Permanent errors (not admin, blocked, etc.) raise immediately."""
    n = 0
    while True:
        try:
            return await fn()
        except errors.FloodWaitError as e:
            log.warning("FloodWait %ss (%s), wait karke retry", e.seconds, what)
            await asyncio.sleep(e.seconds + 2)
        except PERMANENT:
            raise
        except Exception as e:
            n += 1
            if n >= attempts:
                raise
            wait = min(2**n * 2, 120)
            log.warning("%s failed (%s), retry %d/%d in %ss", what, e, n, attempts, wait)
            await asyncio.sleep(wait)


async def on_source(event):
    msg = event.message
    for p in list(sources.get(event.chat_id, ())):
        if p not in PIPELINES or dup(("src", p, event.chat_id, msg.id)):
            continue
        conv_q[PIPELINES[p]["converter"]].put_nowait((p, event.chat_id, msg, time.monotonic()))


async def conv_worker(conv):
    """One at a time, in order, with natural pacing."""
    q = conv_q[conv]
    while True:
        p, chat_id, msg, t0 = await q.get()
        try:
            if time.monotonic() - t0 > 600:  # long-queued: refresh stale file reference
                fresh = await with_retry(lambda: user.get_messages(chat_id, ids=msg.id), f"refetch:{p}")
                msg = fresh or msg
            await with_retry(lambda: user.send_message(conv, msg), f"conv:{p}")
            stats[p]["in"] += 1
        except Exception:
            stats[p]["err"] += 1
            log.exception("converter send failed (%s)", p)
        finally:
            q.task_done()
        await asyncio.sleep(pace(SEND_DELAY))


async def on_reply(event):
    """Converter reply -> post only if it has image + caption + link. Everything else is skipped."""
    msg = event.message
    p = next((k for k, v in PIPELINES.items() if v["conv_id"] == event.sender_id), None)
    if not p or dup(("rep", p, msg.id)):
        return
    text = msg.raw_text or ""  # raw_text matches msg.entities offsets (msg.text does not)
    if not (msg.photo and text.strip() and has_link(msg)):
        stats[p]["skip"] += 1
        log.info("Skip (%s): image+caption+link teeno nahi hain", p)
        return
    try:
        # download now (small), so queue wait can't break an expired file reference
        data = await with_retry(lambda: user.download_media(msg, file=bytes), f"download:{p}")
        if not data:
            raise RuntimeError("empty download")
    except Exception:
        stats[p]["err"] += 1
        log.exception("image download failed (%s)", p)
        return
    post_q[PIPELINES[p]["dest"]].put_nowait((p, data, text, msg.entities))


async def deliver(p, data, text, ents):
    c = PIPELINES[p]
    dest = c["dest"]
    sender = user if c["via_user"] else bot  # bots can't message bots

    def img():
        b = io.BytesIO(data)
        b.name = "image.jpg"
        return b

    if len(text) <= 1024:  # Telegram photo-caption limit
        await with_retry(lambda: sender.send_file(dest, img(), caption=text, formatting_entities=ents), f"post:{p}")
    else:  # caption too long: image first, full text right after
        await with_retry(lambda: sender.send_file(dest, img()), f"post:{p}")
        await with_retry(
            lambda: sender.send_message(dest, text, formatting_entities=ents, link_preview=False), f"post:{p}"
        )


async def post_worker(dest):
    """One at a time, in order, with natural pacing."""
    q = post_q[dest]
    while True:
        p, data, text, ents = await q.get()
        try:
            await deliver(p, data, text, ents)
            stats[p]["out"] += 1
        except Exception:
            stats[p]["err"] += 1
            log.exception("post failed (%s)", p)
        finally:
            q.task_done()
        await asyncio.sleep(pace(POST_DELAY))


async def on_deleted(event):
    txt = f"🗑 Delete alert\nSource: {titles.get(event.chat_id, event.chat_id)}\nMsg IDs: {event.deleted_ids}"
    for a in ADMIN_IDS:
        try:
            await bot.send_message(a, txt)
        except Exception:
            log.warning("alert to %s failed", a)


# ---------- admin commands (bot) ----------
async def resolve_title(cid: int):
    try:
        ent = await user.get_entity(cid)
    except ValueError:
        await user.get_dialogs()  # refresh entity cache
        ent = await user.get_entity(cid)
    return getattr(ent, "title", None) or str(cid)


@bot.on(events.NewMessage(pattern=r"^/start", func=is_admin))
async def cmd_start(e):
    await e.reply(
        "XVIP Hybrid Bot\n\n"
        "/addsource <pipeline> <chat_id>\n/delsource <pipeline> <chat_id>\n"
        "/sources\n/report\n\nPipelines: " + ", ".join(PIPELINES)
    )


@bot.on(events.NewMessage(pattern=r"^/addsource(?:@\w+)?\s+(\w+)\s+(-?\d+)", func=is_admin))
async def cmd_add(e):
    p, cid = e.pattern_match.group(1).lower(), int(e.pattern_match.group(2))
    if p not in PIPELINES:
        return await e.reply(f"Pipeline galat. Options: {', '.join(PIPELINES)}")
    try:
        title = await resolve_title(cid)
    except Exception:
        return await e.reply("Userbot is channel me nahi hai ya ID galat hai.")
    if not await db.upsert_source(cid, p, title):
        return await e.reply("Supabase error, baad me try karo.")
    sources[cid].add(p)
    titles[cid] = title
    await reload_handlers()
    await e.reply(f"✅ {title} → {p}")


@bot.on(events.NewMessage(pattern=r"^/delsource(?:@\w+)?\s+(\w+)\s+(-?\d+)", func=is_admin))
async def cmd_del(e):
    p, cid = e.pattern_match.group(1).lower(), int(e.pattern_match.group(2))
    if not await db.delete_source(cid, p):
        return await e.reply("Supabase error, baad me try karo.")
    sources[cid].discard(p)
    if not sources[cid]:
        sources.pop(cid, None)
    await reload_handlers()
    await e.reply(f"🗑 Removed {cid} from {p}")


@bot.on(events.NewMessage(pattern=r"^/sources", func=is_admin))
async def cmd_sources(e):
    if not sources:
        return await e.reply("Koi source nahi hai.")
    await e.reply("\n".join(f"{titles.get(c, c)} (`{c}`) → {', '.join(sorted(ps))}" for c, ps in sources.items()))


@bot.on(events.NewMessage(pattern=r"^/report", func=is_admin))
async def cmd_report(e):
    out = []
    for p, c in PIPELINES.items():
        srcs = [titles.get(cid, str(cid)) for cid, ps in sources.items() if p in ps]
        s = stats[p]
        out.append(
            f"**{p.upper()}** → `{c['dest']}`\n"
            f"Sources ({len(srcs)}): {', '.join(srcs) or '—'}\n"
            f"In {s['in']} | Out {s['out']} | Skip {s['skip']} | Err {s['err']} | "
            f"Queue {conv_q[c['converter']].qsize() + post_q[c['dest']].qsize()}"
        )
    out.append(f"DB: {'OK' if db.ok else 'DOWN'} | Userbot: {'OK' if user.is_connected() else 'DOWN'}")
    await e.reply("\n\n".join(out))


# ---------- main ----------
async def main():
    if not PIPELINES:
        seen = {
            k: ("set" if os.getenv(k, "").strip() else "EMPTY")
            for k in os.environ
            if any(x in k.upper() for x in ("TERA", "DISK", "FLEZEN", "CONVERTER", "DEST"))
        }
        raise SystemExit(
            "Koi pipeline nahi mili. Expected: <TERA|DISK|FLEZEN>_CONVERTER_BOT + _DESTINATION. "
            f"Railway me ye matching vars dikh rahe hain: {seen or 'KOI NAHI'}"
        )

    await user.connect()
    if not await user.is_user_authorized():
        raise SystemExit("STRING_SESSION invalid/expired, nayi generate karke env me daalo.")
    await bot.start(bot_token=BOT_TOKEN)
    await user.get_dialogs()  # fill entity cache

    for c in PIPELINES.values():
        c["conv_id"] = (await user.get_entity(c["converter"])).id
    for p, toks in ENV_SRC.items():
        if p not in PIPELINES:
            continue
        for t in toks:
            try:
                cid = int(t) if t.lstrip("-").isdigit() else utils.get_peer_id(await user.get_entity(t))
            except Exception:
                log.warning("Env source %r (%s) resolve nahi hua", t, p)
                continue
            ENV_SOURCES[cid].add(p)
            try:
                titles[cid] = await resolve_title(cid)
            except Exception:
                titles[cid] = str(cid)
    for cid, ps in ENV_SOURCES.items():
        sources[cid] |= ps  # works even if Supabase is down

    user.add_event_handler(
        on_reply,
        events.NewMessage(from_users=[c["conv_id"] for c in PIPELINES.values()], incoming=True),
    )

    await bot(
        functions.bots.SetBotCommandsRequest(
            scope=types.BotCommandScopeDefault(),
            lang_code="",
            commands=[
                types.BotCommand("addsource", "Add source: pipeline chat_id"),
                types.BotCommand("delsource", "Remove source: pipeline chat_id"),
                types.BotCommand("sources", "List sources"),
                types.BotCommand("report", "Live status"),
            ],
        )
    )

    for conv in {c["converter"] for c in PIPELINES.values()}:
        bg_tasks.append(asyncio.create_task(conv_worker(conv)))
    for dest in {c["dest"] for c in PIPELINES.values()}:
        bg_tasks.append(asyncio.create_task(post_worker(dest)))

    await sync_sources()
    await reload_handlers()
    bg_tasks.append(asyncio.create_task(refresh_loop()))
    log.info("Running. Pipelines: %s", ", ".join(PIPELINES))
    await asyncio.gather(user.run_until_disconnected(), bot.run_until_disconnected())


if __name__ == "__main__":
    asyncio.run(main())
