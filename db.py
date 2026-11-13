"""
Supabase persistence (REST, no SDK). Never raises: failures return None/False
so a Supabase outage can't crash-loop the bot on Railway.

Run once in Supabase SQL editor:

    create table if not exists sources (
        chat_id  bigint not null,
        pipeline text   not null,
        title    text,
        primary key (chat_id, pipeline)
    );

Use the service_role key in SUPABASE_KEY.
"""
import logging
import os

import httpx

log = logging.getLogger("db")

URL = os.getenv("SUPABASE_URL", "").rstrip("/")
KEY = os.getenv("SUPABASE_KEY", "")
ok = True  # last request status, shown in /report
_client = None


def _c():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=f"{URL}/rest/v1",
            headers={
                "apikey": KEY,
                "Authorization": f"Bearer {KEY}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )
    return _client


async def _req(method, path, **kw):
    global ok
    try:
        r = await _c().request(method, path, **kw)
        r.raise_for_status()
        ok = True
        return r
    except Exception as e:
        ok = False
        log.warning("Supabase error: %s", e)
        return None


async def fetch_sources():
    r = await _req("GET", "/sources?select=chat_id,pipeline,title")
    return r.json() if r else None


async def upsert_source(chat_id: int, pipeline: str, title: str) -> bool:
    r = await _req(
        "POST",
        "/sources?on_conflict=chat_id,pipeline",
        json={"chat_id": chat_id, "pipeline": pipeline, "title": title},
        headers={"Prefer": "resolution=merge-duplicates"},
    )
    return r is not None


async def delete_source(chat_id: int, pipeline: str) -> bool:
    r = await _req("DELETE", f"/sources?chat_id=eq.{chat_id}&pipeline=eq.{pipeline}")
    return r is not None
