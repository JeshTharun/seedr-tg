"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ArrowRight,
  ClipboardPaste,
  Inbox,
  LoaderCircle,
  RefreshCcw,
} from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";

type QueueJob = {
  id: number;
  phase: string;
  progress_percent: number;
  current_step: string;
};

type MagnetResponse = {
  accepted: boolean;
  duplicate: boolean;
  job_id: number | null;
  phase: string | null;
  message: string;
};

const API_BASE = process.env.NEXT_PUBLIC_SEEDR_API_BASE_URL ?? "http://127.0.0.1:8787";

export default function Home() {
  const apiBase = API_BASE;
  const [magnet, setMagnet] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [statusTone, setStatusTone] = useState<"ok" | "error">("ok");
  const [queue, setQueue] = useState<QueueJob[]>([]);
  const [isQueueLoading, setIsQueueLoading] = useState(false);

  const magnetLooksValid = useMemo(() => magnet.trim().startsWith("magnet:?"), [magnet]);

  const loadQueue = useCallback(async (base = apiBase) => {
    setIsQueueLoading(true);
    try {
      const response = await fetch(`${base}/api/jobs`, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Queue request failed with ${response.status}`);
      }
      const payload = (await response.json()) as QueueJob[];
      setQueue(payload);
    } catch {
      setQueue([]);
    } finally {
      setIsQueueLoading(false);
    }
  }, [apiBase]);

  useEffect(() => {
    loadQueue(apiBase);
    const timer = window.setInterval(() => loadQueue(apiBase), 6000);
    return () => window.clearInterval(timer);
  }, [apiBase, loadQueue]);

  async function submitMagnet() {
    const value = magnet.trim();
    if (!value) {
      setStatusTone("error");
      setStatusMessage("Paste a magnet link before submitting.");
      return;
    }
    setIsSubmitting(true);
    setStatusMessage(null);
    try {
      const response = await fetch(`${apiBase}/api/magnets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ magnet_link: value }),
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || `Request failed with ${response.status}`);
      }

      const payload = (await response.json()) as MagnetResponse;
      if (payload.accepted) {
        setStatusTone("ok");
        setStatusMessage(`Queued as job #${payload.job_id}. Telegram upload will run automatically.`);
        setMagnet("");
      } else {
        setStatusTone("error");
        setStatusMessage(payload.message || "Magnet was ignored.");
      }
      await loadQueue(apiBase);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setStatusTone("error");
      setStatusMessage(`Could not queue magnet: ${message}`);
    } finally {
      setIsSubmitting(false);
    }
  }

  async function pasteMagnetFromClipboard() {
    try {
      const text = await navigator.clipboard.readText();
      if (text.trim()) {
        setMagnet(text.trim());
      }
    } catch {
      setStatusTone("error");
      setStatusMessage("Could not read from clipboard. Paste manually or allow clipboard permission.");
    }
  }

  return (
    <div className="relative min-h-screen overflow-hidden bg-[#111214] px-4 py-6 text-zinc-100 sm:px-8 sm:py-10">
      <div className="pointer-events-none absolute inset-0 [background:radial-gradient(circle_at_10%_-10%,rgba(84,82,106,0.35),transparent_45%),radial-gradient(circle_at_80%_-20%,rgba(65,94,122,0.2),transparent_45%)]" />

      <main className="relative mx-auto flex w-full max-w-6xl flex-col gap-6 rounded-2xl border border-zinc-700/50 bg-[#1a1b1f]/92 p-4 shadow-2xl backdrop-blur-sm sm:p-8">
        <header className="space-y-2 px-1 sm:px-2">
          <h1 className="text-3xl font-semibold tracking-tight text-zinc-100 sm:text-5xl">
            Queue Magnets
          </h1>
          <p className="max-w-3xl text-sm text-zinc-400 sm:text-base">
            Submit magnet links to the relay service. Downloads are processed and uploaded to
            Telegram automatically.
          </p>
        </header>

        <div className="grid gap-5 md:grid-cols-[1.3fr_1fr]">
          <Card className="rounded-2xl border-zinc-700/60 bg-[#23242a] text-zinc-100 shadow-lg">
            <CardHeader className="px-6 pb-2 pt-5">
              <CardTitle className="text-2xl font-medium">New Magnet</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4 px-6 pb-5">
              <div className="space-y-2">
                <label className="text-xs uppercase tracking-wide text-zinc-400" htmlFor="magnet">
                  Magnet Link
                </label>
                <Textarea
                  id="magnet"
                  value={magnet}
                  onChange={(event) => setMagnet(event.target.value)}
                  rows={6}
                  placeholder="magnet:?xt=urn:btih:..."
                  className="min-h-40 resize-y rounded-lg border-zinc-600 bg-[#1c1d22] font-mono text-zinc-100 placeholder:text-zinc-500"
                />
              </div>

              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <Button
                  type="button"
                  variant="secondary"
                  onClick={pasteMagnetFromClipboard}
                  className="h-9 w-fit rounded-md border border-zinc-600 bg-[#2b2d36] text-zinc-200 hover:bg-[#353846]"
                >
                  <ClipboardPaste className="size-4" />
                  Paste
                </Button>

                <div className="flex items-center gap-2 text-xs text-zinc-500">
                  <span>{magnet.length} / 120</span>
                  <span className="hidden sm:inline">|</span>
                  <span>{magnetLooksValid || magnet.length === 0 ? "" : "Invalid format"}</span>
                </div>
              </div>

              <div className="flex justify-end">
                <Button
                  onClick={submitMagnet}
                  disabled={isSubmitting || !magnetLooksValid}
                  className="h-10 rounded-full bg-[#6452b3] px-6 text-sm font-semibold text-white hover:bg-[#755fe0] disabled:bg-zinc-700 disabled:text-zinc-400"
                >
                  {isSubmitting ? (
                    <>
                      <LoaderCircle className="size-4 animate-spin" />
                      Queuing
                    </>
                  ) : (
                    <>
                      Queue Magnet
                      <ArrowRight className="size-4" />
                    </>
                  )}
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-2xl border-zinc-700/60 bg-[#23242a] text-zinc-100 shadow-lg">
            <CardHeader className="px-6 pb-3 pt-5">
              <CardTitle className="flex items-center gap-2 text-2xl font-medium">
                Active Queue
                <RefreshCcw className={`size-4 text-zinc-400 ${isQueueLoading ? "animate-spin" : ""}`} />
              </CardTitle>
              <p className="text-sm text-zinc-400">Auto-refreshes every 6 seconds.</p>
            </CardHeader>
            <CardContent className="space-y-3 px-6 pb-6">
              {queue.length === 0 ? (
                <div className="flex min-h-56 flex-col items-center justify-center rounded-xl border border-dashed border-zinc-600 bg-[#1c1d22] text-center">
                  <Inbox className="mb-3 size-6 text-zinc-500" />
                  <p className="text-sm text-zinc-300">Queue is empty.</p>
                  <p className="text-sm text-zinc-500">Magnets will appear here.</p>
                </div>
              ) : (
                queue.slice(0, 6).map((job) => (
                  <div
                    key={job.id}
                    className="rounded-xl border border-zinc-700 bg-[#1c1d22] p-3 shadow-sm"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-sm font-semibold text-zinc-100">Job #{job.id}</p>
                      <Badge className="rounded-md bg-zinc-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-zinc-200 hover:bg-zinc-700">
                        {job.phase}
                      </Badge>
                    </div>
                    <p className="mt-1 line-clamp-2 text-xs text-zinc-400">{job.current_step}</p>
                    <progress
                      className="mt-3 h-1.5 w-full appearance-none overflow-hidden rounded-full [&::-webkit-progress-bar]:bg-zinc-800 [&::-webkit-progress-value]:bg-[#6452b3] [&::-webkit-progress-value]:transition-all"
                      value={Math.max(0, Math.min(100, job.progress_percent))}
                      max={100}
                    />
                  </div>
                ))
              )}
            </CardContent>
          </Card>
        </div>

        {statusMessage && (
          <Alert
            className={
              statusTone === "ok"
                ? "rounded-xl border-emerald-700 bg-emerald-950/40 text-emerald-200"
                : "rounded-xl border-red-700 bg-red-950/40 text-red-200"
            }
          >
            <AlertTitle>{statusTone === "ok" ? "Queued" : "Failed"}</AlertTitle>
            <AlertDescription>{statusMessage}</AlertDescription>
          </Alert>
        )}
      </main>
    </div>
  );
}
