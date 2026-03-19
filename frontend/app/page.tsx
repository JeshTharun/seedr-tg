"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowRight, Link2, LoaderCircle, RadioTower, Sparkles } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
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
  const [apiBase, setApiBase] = useState(API_BASE);
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

  return (
    <div className="relative min-h-screen overflow-hidden bg-[radial-gradient(circle_at_20%_10%,#ffd8aa_0%,transparent_35%),radial-gradient(circle_at_90%_15%,#8ec5ff_0%,transparent_40%),linear-gradient(180deg,#fff7ed_0%,#fff 45%,#f4f6ff_100%)] p-5 md:p-10">
      <div className="pointer-events-none absolute inset-0 opacity-50 [background:linear-gradient(115deg,transparent_0%,transparent_45%,rgba(13,148,136,0.08)_45%,rgba(13,148,136,0.08)_50%,transparent_50%,transparent_100%)]" />

      <main className="relative mx-auto flex w-full max-w-5xl flex-col gap-6">
        <header className="rounded-3xl border border-black/10 bg-white/80 p-6 shadow-sm backdrop-blur md:p-8">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-3">
              <Badge className="rounded-full bg-teal-600 text-white hover:bg-teal-600">
                Seedr to Telegram Relay
              </Badge>
              <h1 className="text-3xl font-semibold tracking-tight text-zinc-900 md:text-5xl">
                Queue Magnets From Web
              </h1>
              <p className="max-w-3xl text-sm text-zinc-700 md:text-base">
                Submit magnet links here. The existing Python backend keeps using your current Seedr
                pipeline and uploads to Telegram automatically after download.
              </p>
            </div>
            <Sparkles className="mt-1 hidden size-8 text-amber-500 md:block" />
          </div>
        </header>

        <div className="grid gap-6 md:grid-cols-[1.5fr_1fr]">
          <Card className="rounded-3xl border-black/10 bg-white/85 backdrop-blur">
            <CardHeader>
              <CardTitle className="text-xl">New Magnet</CardTitle>
              <CardDescription>
                Default API endpoint is local. Change it only if your Python backend runs elsewhere.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium text-zinc-700" htmlFor="api-base">
                  Backend API URL
                </label>
                <Input
                  id="api-base"
                  value={apiBase}
                  onChange={(event) => setApiBase(event.target.value)}
                  placeholder="http://127.0.0.1:8787"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-medium text-zinc-700" htmlFor="magnet">
                  Magnet Link
                </label>
                <Textarea
                  id="magnet"
                  value={magnet}
                  onChange={(event) => setMagnet(event.target.value)}
                  rows={8}
                  placeholder="magnet:?xt=urn:btih:..."
                  className="resize-y"
                />
              </div>
            </CardContent>
            <CardFooter className="flex flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-2 text-xs text-zinc-600">
                <Link2 className="size-4" />
                {magnetLooksValid ? "Magnet format looks valid" : "Magnet must start with magnet:?"}
              </div>
              <Button
                onClick={submitMagnet}
                disabled={isSubmitting || !magnetLooksValid}
                className="rounded-full bg-zinc-900 px-6 hover:bg-zinc-800"
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
            </CardFooter>
          </Card>

          <Card className="rounded-3xl border-black/10 bg-white/85 backdrop-blur">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-xl">
                <RadioTower className="size-5 text-teal-600" />
                Active Queue
              </CardTitle>
              <CardDescription>Auto-refreshes every 6 seconds.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {isQueueLoading && (
                <p className="text-sm text-zinc-600">Refreshing queue...</p>
              )}
              {queue.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-zinc-300 p-4 text-sm text-zinc-600">
                  Queue is empty.
                </div>
              ) : (
                queue.slice(0, 6).map((job) => (
                  <div
                    key={job.id}
                    className="rounded-2xl border border-zinc-200 bg-white p-3 shadow-sm"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-sm font-semibold text-zinc-900">Job #{job.id}</p>
                      <Badge variant="secondary" className="rounded-full uppercase tracking-wide">
                        {job.phase}
                      </Badge>
                    </div>
                    <p className="mt-1 line-clamp-2 text-xs text-zinc-600">{job.current_step}</p>
                    <progress
                      className="mt-3 h-2 w-full appearance-none overflow-hidden rounded-full [&::-webkit-progress-bar]:bg-zinc-200 [&::-webkit-progress-value]:bg-teal-500 [&::-webkit-progress-value]:transition-all"
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
                ? "rounded-2xl border-teal-300 bg-teal-50"
                : "rounded-2xl border-red-300 bg-red-50"
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
