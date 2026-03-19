# Seedr Web Queue Frontend

This Next.js app provides a web UI to submit magnet links into the Python backend queue.

## Run

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## Backend API

The UI targets `http://127.0.0.1:8787` by default and uses:

- `POST /api/magnets`
- `GET /api/jobs`

You can override the default endpoint with:

```bash
NEXT_PUBLIC_SEEDR_API_BASE_URL=http://127.0.0.1:8787
```
