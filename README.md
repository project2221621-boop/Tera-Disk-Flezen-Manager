# Tera Disk Flezen Manager

Telegram multi-pipeline forwarding system: **Telethon userbot + bot client**.
Pipelines: **Tera**, **Disk**, **Flezen**.

## Flow

```
Source channels ──(userbot)──> Converter bot ──(reply)──> Destination (channel ya bot)
```

1. Userbot source channels ke naye messages padhta hai.
2. Har message us pipeline ke converter bot ko jata hai (FIFO queue, natural delay).
3. Converter ka reply tabhi post hota hai jab **image + caption + link** teeno ho. Warna skip.
4. Post destination par jata hai (queue + retry + FloodWait handling).

Destination channel ho to bot client post karta hai. Destination bot ho to userbot bhejta hai (bot, bot ko message nahi bhej sakta).

## Files

| File | Kaam |
|---|---|
| `main.py` | Clients, pipelines, queues, admin commands |
| `db.py` | Supabase REST helper (fail hone par crash nahi hota) |
| `requirements.txt` | Dependencies |

## Supabase setup

SQL editor me ek baar chalao:

```sql
create table if not exists sources (
    chat_id  bigint not null,
    pipeline text   not null,
    title    text,
    primary key (chat_id, pipeline)
);
```

`SUPABASE_KEY` me **service_role** key daalo.

## Environment variables

Railway → Service → **Variables** me daalo. Neeche sab **temp (sample) values** hain, apni real values se badlo.

```env
# --- Telegram (my.telegram.org se) ---
API_ID=1234567
API_HASH=0123456789abcdef0123456789abcdef
STRING_SESSION=1BVtsOK8Bu...paste_generated_string_here
BOT_TOKEN=1234567890:AAExampleBotTokenReplaceMe
ADMIN_IDS=111111111,222222222

# --- Supabase ---
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_KEY=eyJhbGciOi...your_service_role_key

# --- TERA pipeline ---
TERA_CONVERTER_BOT=@tera_converter_bot
TERA_DESTINATION=-1001234567890
TERA_SOURCE_CHANNELS=-1001111111111,-1002222222222

# --- DISK pipeline ---
DISK_CONVERTER_BOT=@disk_converter_bot
DISK_DESTINATION=-1003333333333
DISK_SOURCE_CHANNELS=-1004444444444

# --- FLEZEN pipeline (destination bot example) ---
FLEZEN_CONVERTER_BOT=@flezen_converter_bot
FLEZEN_DESTINATION=@flezen_dest_bot
FLEZEN_SOURCE_CHANNELS=-1005555555555

# --- Optional (defaults neeche) ---
SEND_DELAY=2.5
POST_DELAY=3
```

| Variable | Zaroori | Matlab |
|---|---|---|
| `API_ID`, `API_HASH` | Haan | Telegram API credentials |
| `STRING_SESSION` | Haan | Userbot ki Telethon session string |
| `BOT_TOKEN` | Haan | Bot client ka token (@BotFather) |
| `ADMIN_IDS` | Haan | Admins ke numeric IDs, comma-separated |
| `SUPABASE_URL`, `SUPABASE_KEY` | Haan | Dynamic sources ka storage |
| `<P>_CONVERTER_BOT` | Haan | Pipeline ka converter bot (`@username`) |
| `<P>_DESTINATION` | Haan | Channel ID (`-100...`) ya bot (`@username`) |
| `<P>_SOURCE_CHANNELS` | Nahi | Starting sources (ID ya username, comma-separated) |
| `SEND_DELAY` | Nahi | Converter ko bhejne ke beech gap (seconds, default 2.5) |
| `POST_DELAY` | Nahi | Destination par post ke beech gap (seconds, default 3) |

`<P>` = `TERA`, `DISK`, `FLEZEN`. Jis pipeline ke `CONVERTER_BOT` aur `DESTINATION` dono set hon, wahi active hoti hai.

## STRING_SESSION kaise banaye

```
pip install telethon
python -c "from telethon.sync import TelegramClient as C; from telethon.sessions import StringSession as S; c=C(S(), int(input('API_ID: ')), input('API_HASH: ')); c.start(); print('\nSTRING_SESSION:\n'); print(c.session.save())"
```

Phone number, OTP aur 2FA password terminal me poochega. Ye string account ka full access hai, kisi ke saath share mat karo.

## Deploy (Railway)

1. Code GitHub repo me push karo (`main.py`, `db.py`, `requirements.txt`).
2. Railway me repo se service banao.
3. Upar wale variables daalo.
4. Start command: `python main.py` (Railway usually khud detect kar leta hai).

## Setup checklist

- Userbot account sab source channels me joined ho.
- Userbot ne converter bots ko aur (agar destination bot hai to) destination bot ko ek baar `/start` kiya ho.
- Bot client destination channels me **admin** ho (post permission ke saath).
- Har admin ne bot ko ek baar `/start` kiya ho (deletion alerts ke liye).
- Converter bot aur destination bot alag ho (warna loop banega).

## Admin commands

Sirf `ADMIN_IDS` wale users, bot ke private chat me.

| Command | Kaam |
|---|---|
| `/start` | Menu + Userbot Status (🟢 Connected / 🔴 Disconnected) |
| `/status` | Account/login status: userbot account, bot, database, pipelines, uptime |
| `/report` | Live status of all source channels: ✅ live / ❌ deleted, messages seen, last message time; pipeline-wise In / Out / Skip / Err / Queue |
| `/tera_add` `/tera_remove` `/tera_list` | Tera sources manage karo |
| `/disk_add` `/disk_remove` `/disk_list` | Disk sources manage karo |
| `/flezen_add` `/flezen_remove` `/flezen_list` | Flezen sources manage karo |

**Source status icons** (`/report` aur `_list` dono me; IDs monospace me, tap karke copy ho jati hain):

- ✅ Live: userbot channel padh pa raha hai
- ❌ Deleted: channel delete ho gaya, ya userbot hata diya gaya / ban ho gaya (live sources upar, deleted neeche)
- ⚠️ Temporary check fail (timeout/network). 60 sec baad dobara check hota hai

Add/remove me chat ID (`-100...`) ya `@username`, dono chalte hain:

```
/tera_add -1001234567890
/disk_add @my_source_channel
/flezen_remove -1005555555555
/tera_list
```

Add karte hi source Supabase me save hota hai aur handler turant reload hota hai (restart nahi chahiye). Ye saari commands bot ke menu me bhi dikhti hain.

`*_SOURCE_CHANNELS` env wale sources har restart par wapas load hote hain. Unhe `_remove` se nahi hata sakte, Railway variable se nikalo.

## Behaviour

- **Filter:** sirf image + caption + link wale posts destination par jate hain. Baaki `Skip` me gine jate hain.
- **Queue:** har converter aur har destination ki alag FIFO queue. Order bana rehta hai, random jitter ke saath natural pacing.
- **Retry:** FloodWait par bot Telegram ke bataye time tak ruk kar retry karta hai. Network ya temporary error par backoff ke saath 6 retries.
- **Long caption:** 1024 characters se lamba ho to pehle image, phir turant poora text.
- **Dedup:** ek hi message dobara process nahi hota.
- **Supabase down:** bot crash nahi hota, cache aur env sources se chalta rehta hai.
- **Deletion alerts:** source channel me message delete ho to admins ko alert jata hai.

## Limitations

- Queue memory me hai. Backlog chalte waqt redeploy ya crash hua to pending posts chale jate hain.
- Permanent errors (bot destination me admin nahi, user ne bot block kiya) retry se theek nahi hote. Wo log me dikhte hain.
- Converter reply me image **photo** ke roop me honi chahiye (document/file ke roop me nahi).
- Converter agar pehle "processing..." bhejke baad me message edit karta hai, to edit handled nahi hai.
