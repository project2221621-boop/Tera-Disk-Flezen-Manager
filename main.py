import asyncio
import io
import logging
import os
from collections import defaultdict, deque

from telethon import TelegramClient, events, functions, types
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
for _n in ("tera", "disk", "flezen"):
    _conv, _dest = os.getenv(f"{_n.upper()}_CONVERTER"), os.getenv(f"{_n.upper()}_DEST")
    if _conv and _dest:
        PIPELINES[_n] = {"converter": _conv, "dest": int(_dest), "conv_id": None}

user = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
bot = TelegramClient("bot_session", API_ID, API_HASH)

# ---------- state ----------
sources = defaultdict(set)  # chat_id -> {pipeline}
titles = {}
stats = defaultdict(lambda: {"in": 0, "out": 0, "err": 0})
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
async def on_source(event):
    msg = event.message
    for p in list(sources.get(event.chat_id, ())):
        if p not in PIPELINES or dup(("src", p, event.chat_id, msg.id)):
            continue
        try:
            await user.send_message(PIPELINES[p]["converter"], msg)
            stats[p]["in"] += 1
            await asyncio.sleep(1.5)  # gentle on flood limits
        except Exception:
            stats[p]["err"] += 1
            log.exception("send to converter failed (%s)", p)


async def on_reply(event):
    """Converter reply is self-contained: post it as-is to the pipeline's destination."""
    msg = event.message
    p = next((k for k, v in PIPELINES.items() if v["conv_id"] == event.sender_id), None)
    if not p or dup(("rep", p, msg.id)):
        return
    dest = PIPELINES[p]["dest"]
    try:
        if msg.photo or msg.document:
            buf = io.BytesIO()
            await user.download_media(msg, file=buf)
            buf.seek(0)
            buf.name = "file.jpg" if msg.photo else (msg.file.name or "file")
            await bot.send_file(dest, buf, caption=msg.text or "", formatting_entities=msg.entities)
        elif msg.text:
            await bot.send_message(dest, msg.text, formatting_entities=msg.entities, link_preview=False)
        else:
            return
        stats[p]["out"] += 1
    except Exception:
        stats[p]["err"] += 1
        log.exception("post to destination failed (%s)", p)


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
            f"In {s['in']} | Out {s['out']} | Err {s['err']}"
        )
    out.append(f"DB: {'OK' if db.ok else 'DOWN'} | Userbot: {'OK' if user.is_connected() else 'DOWN'}")
    await e.reply("\n\n".join(out))


# ---------- main ----------
async def main():
    if not PIPELINES:
        raise SystemExit("Koi pipeline configure nahi hai (TERA_CONVERTER/TERA_DEST etc.)")

    await user.connect()
    if not await user.is_user_authorized():
        raise SystemExit("STRING_SESSION invalid/expired, nayi generate karke env me daalo.")
    await bot.start(bot_token=BOT_TOKEN)
    await user.get_dialogs()  # fill entity cache

    for c in PIPELINES.values():
        c["conv_id"] = (await user.get_entity(c["converter"])).id
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

    await sync_sources()
    await reload_handlers()
    asyncio.create_task(refresh_loop())
    log.info("Running. Pipelines: %s", ", ".join(PIPELINES))
    await asyncio.gather(user.run_until_disconnected(), bot.run_until_disconnected())


if __name__ == "__main__":
    asyncio.run(main())
