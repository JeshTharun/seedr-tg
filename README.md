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
9. Supports direct URL relay with rename rules through `/direct`.

## Configuration

Copy `.env.example` to `.env` and fill these values:

- `TELEGRAM_BOT_TOKEN`: Bot token used for intake and admin messaging.
- `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`: Telegram API credentials for the premium uploader account.
- `TELEGRAM_SOURCE_CHAT_ID`: Channel or chat that receives the magnet messages.
- `TELEGRAM_TARGET_CHAT_ID`: Destination where completed files are uploaded.
- `TELEGRAM_ADMIN_CHAT_ID`: Private admin chat for progress and cancel controls.
- `MONGODB_URI`: MongoDB connection for jobs, auth state, and Telegram user session storage.
- `SEEDR_TOKEN_JSON`: Optional bootstrap token. If omitted, start device auth from the bot with `/seedr_auth`.
- `TELEGRAM_USER_SESSION_STRING`: Optional bootstrap MTProto string session. If omitted, create it from the bot with `/session_start <phone>`.
- `WEB_API_ALLOWED_ORIGINS`: Comma-separated list of browser origins allowed to call the web API (for example your Vercel frontend URL).
- `DIRECT_DOWNLOAD_CHUNK_SIZE_BYTES`: Optional chunk size for direct URL streaming downloads (default `1048576`).
- `DIRECT_FILENAME_MAX_BYTES`: Optional max UTF-8 filename byte size for Telegram-safe direct uploads (default `255`).

All other runtime knobs are now hardcoded defaults in `src/seedr_tg/config.py` to keep `.env` minimal.

## Run (OS-specific)

### Linux (Ubuntu/Debian)

If `python` is missing (common on fresh servers), use `python3`.

```bash
apt update
apt install -y python3 python3-venv python3-pip
apt install -y aria2

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
seedr-tg
```

Optional: if you want `python` to map to Python 3:

```bash
apt install -y python-is-python3
```

### macOS

```bash
brew install aria2

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
seedr-tg
```

### Windows (PowerShell)

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
seedr-tg
```

### Windows (cmd.exe)

```bat
py -3 -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
seedr-tg
```

## Troubleshooting

### `seedr-tg: command not found`

This means the project package was not installed into the active virtual environment yet.

Run from the repository root after activating `.venv`:

```bash
python -m pip install -e .
which seedr-tg
seedr-tg
```

If you still do not get the command, run with module syntax as a fallback:

```bash
python -m seedr_tg.main
```

## PM2 Integration (Linux)

Use this when you want the bot to run as a managed background service with auto-restart.

1. Install PM2:

```bash
npm install -g pm2
```

2. Start the bot with PM2 from your project directory:

```bash
cd ~/seedr-tg
pm2 start .venv/bin/seedr-tg --name seedr-tg --cwd ~/seedr-tg
```

3. Check status and logs:

```bash
pm2 status
pm2 logs seedr-tg
```

4. Persist across reboots:

```bash
pm2 save
pm2 startup
```

Run the command printed by `pm2 startup`, then run `pm2 save` again.

5. Restart after code or env updates:

```bash
cd ~/seedr-tg
source .venv/bin/activate
python -m pip install -r requirements.txt
pm2 restart seedr-tg
```

6. Stop or remove the process:

```bash
pm2 stop seedr-tg
pm2 delete seedr-tg
```

## Notes

- Use `/session_start <phone>`, `/session_code <code>`, and optionally `/session_password <password>` in the admin chat to create the Telegram premium uploader session and store it in MongoDB.
- Use `/seedr_auth` and `/seedr_auth_done` in the admin chat to complete Seedr device-code authentication and store the refreshed token in MongoDB.
- Optional high-speed downloader mode: set `USE_ARIA2_DOWNLOADS=true` and keep `aria2c` installed. If aria2 fails for any file, the app automatically falls back to the built-in HTTP downloader.
- aria2 tuning keys: `ARIA2_SPLIT`, `ARIA2_MAX_CONNECTION_PER_SERVER`, `ARIA2_MIN_SPLIT_SIZE`, and `ARIA2_FILE_ALLOCATION`.
- Adaptive upload governor is enabled by default and tunes upload concurrency between `UPLOAD_GOVERNOR_MIN_CONCURRENCY` and `UPLOAD_CONCURRENCY`, reducing on flood waits and scaling back up after stable uploads.
- Upload extension filter is enforced: only `.mp4`, `.mkv`, and `.zip` files are uploaded; all other file types are skipped.
- Raw magnets do not expose total size reliably, so the 4 GB limit is enforced immediately after Seedr resolves metadata.
- The current implementation is intentionally single-worker FIFO.

## Direct URL Command

Use `/direct` in the source chat or admin chat to download a file from a direct URL, rename it safely, and upload it back to Telegram.

Command format:

```text
/direct <url> [--rename <name>] [--prefix <value>] [--sub <pattern=>replacement>] [--sub-cs <pattern=>replacement>]
```

Behavior summary:

- URL only: keeps original filename (sanitized).
- `--rename`: replaces filename stem with your custom value.
- `--prefix`: prepends text before the current stem.
- `--sub`: regex substitution on stem, case-insensitive by default.
- `--sub-cs`: case-sensitive regex substitution.
- Extension is always preserved.
- If target filename already exists in temp scope, numeric suffixes like ` (1)`, ` (2)` are appended.
- Temporary files are cleaned up on both success and failure.

Examples:

```text
/direct https://example.com/video.mp4
/direct https://example.com/video.mp4 --rename "Episode 04"
/direct https://example.com/video.mp4 --prefix "[MyTag] "
/direct https://example.com/video.mp4 --sub "S01E04=>S01E04 [WEB]" --sub "\\.=> "
/direct https://example.com/video.mp4 --sub-cs "WEB=>Web"
```

Success reply includes:

- original name
- renamed name
- file size
- elapsed time

## Web UI (Magnet Submit)

The Python backend now also starts a local web API on `http://127.0.0.1:8787` with:

- `POST /api/magnets` to enqueue a magnet link.
- `GET /api/jobs` to read active queue status.

Frontend app is in `frontend/` and uses shadcn components.

Run it in a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Then open `http://localhost:3000` and submit magnets from the web UI. The existing Python queue/download/upload pipeline is unchanged, so uploads to Telegram continue automatically.