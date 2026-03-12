# seedr-tg

Async Seedr.cc to Telegram relay service for a single channel and single Seedr account.

## What it does

1. Watches one configured Telegram source chat for magnet links.
2. Queues magnets in persistent FIFO order.
3. Sends the magnet to Seedr and waits for torrent completion.
4. Rejects torrents larger than 4 GB as soon as metadata resolves.
5. Downloads completed Seedr files to local disk.
6. Deletes the related Seedr folder after local download completes.
7. Uploads the files back to Telegram using a premium MTProto session.
8. Shows progress and cancellation controls in a private admin chat.

## Configuration

Copy `.env.example` to `.env` and fill these values:

- `TELEGRAM_BOT_TOKEN`: Bot token used for intake and admin messaging.
- `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`: Telegram API credentials for the premium uploader account.
- `TELEGRAM_PHONE_NUMBER`: Phone number of the premium Telegram account.
- `TELEGRAM_SOURCE_CHAT_ID`: Channel or chat that receives the magnet messages.
- `TELEGRAM_TARGET_CHAT_ID`: Destination where completed files are uploaded.
- `TELEGRAM_ADMIN_CHAT_ID`: Private admin chat for progress and cancel controls.
- `SEEDR_TOKEN_JSON`: Preferred Seedr session token JSON.
- `SEEDR_EMAIL` and `SEEDR_PASSWORD`: Fallback auth if no token JSON is provided.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
seedr-tg
```

## Notes

- The MTProto session is stored locally via Telethon. The first run will require Telegram login confirmation.
- Raw magnets do not expose total size reliably, so the 4 GB limit is enforced immediately after Seedr resolves metadata.
- The current implementation is intentionally single-worker FIFO.