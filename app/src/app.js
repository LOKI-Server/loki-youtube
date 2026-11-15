"use strict";

const byId = id => document.getElementById(id);
const csrf = document.querySelector('meta[name="csrf-token"]').content;
let selection;
let serverClock = 0;
let estimatedAt = performance.now();

async function api(path, method = "GET", data) {
  const request = { method, headers: { "x-csrf-token": csrf }, cache: "no-store" };
  if (data !== undefined) {
    request.headers["content-type"] = "application/json";
    request.body = JSON.stringify(data);
  }
  const response = await fetch(path, request);
  const json = await response.json().catch(() => ({}));
  if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) {
    throw new Error(json.error || "Sign in again to continue.");
  }
  return json;
}

function status(message, error = false) {
  byId("status").textContent = message;
  byId("status").classList.toggle("error", error);
}

function formatSize(bytes) {
  return `${(bytes / 1048576).toFixed(1)} MiB`;
}

function submit(id, operation) {
  byId(id).addEventListener("submit", async event => {
    event.preventDefault();
    const button = event.currentTarget.querySelector("button");
    button.disabled = true;
    try { await operation(); } catch (error) { status(error.message, true); }
    finally { button.disabled = false; }
  });
}

submit("inspect", async () => {
  byId("download").hidden = true;
  status("Looking up available qualities…");
  const video = await api("/api/inspect", "POST", { url: byId("url").value });
  selection = video.selection;
  byId("title").textContent = video.title || "YouTube video";
  byId("quality").replaceChildren(new Option("Best available", ""), ...video.formats.map(format =>
    new Option(`${format.height}p ${format.ext.toUpperCase()}${format.size ? ` · ${formatSize(format.size)}` : ""}`, format.id)
  ));
  byId("download").hidden = false;
  status("");
});

submit("download", async () => {
  await api("/api/downloads", "POST", { selection, format: byId("quality").value || null });
  byId("download").hidden = true;
  status("Preparing your video. Its countdown starts when it is ready.");
  await refresh();
});

const labels = { downloading: "Preparing…", ready: "Ready", failed: "Could not prepare video", too_large: "Video exceeded the server’s file limit", expired: "Expired", interrupted: "Interrupted" };

async function refresh() {
  const { items, now } = await api("/api/history");
  serverClock = now;
  estimatedAt = performance.now();
  const list = items.map(item => {
    const row = document.createElement("li");
    const description = document.createElement("div");
    const title = document.createElement("strong");
    const details = document.createElement("small");
    title.textContent = item.title || item.video_id;
    details.textContent = [labels[item.status] ?? item.status, item.quality, item.size ? formatSize(item.size) : "", new Date(item.created * 1000).toLocaleString()].filter(Boolean).join(" · ");
    description.append(title, details);
    row.append(description);
    if (item.status === "ready") {
      const pickup = document.createElement("div");
      pickup.className = "pickup";
      const countdown = document.createElement("span");
      countdown.dataset.expires = item.expires;
      const link = document.createElement("a");
      link.className = "download";
      link.href = `/download/${encodeURIComponent(item.id)}`;
      link.download = "";
      link.textContent = "Download";
      pickup.append(countdown, link);
      row.append(pickup);
    }
    return row;
  });
  if (!list.length) {
    const empty = document.createElement("li");
    empty.textContent = "No downloads yet.";
    list.push(empty);
  }
  byId("history").replaceChildren(...list);
  tick();
}

function tick() {
  const now = serverClock + Math.floor((performance.now() - estimatedAt) / 1000);
  for (const timer of document.querySelectorAll("[data-expires]")) {
    const seconds = Math.max(0, Number(timer.dataset.expires) - now);
    timer.textContent = seconds ? `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}` : "Expired";
    timer.nextElementSibling.hidden = seconds === 0;
  }
}

byId("clear").addEventListener("click", async () => {
  if (!confirm("Clear past downloads from your history?")) return;
  try { await api("/api/history", "DELETE"); await refresh(); }
  catch (error) { status(error.message, true); }
});

byId("logout").addEventListener("click", async () => {
  try {
    const response = await fetch("/logout", { method: "POST", headers: { "x-csrf-token": csrf }, redirect: "manual" });
    if (response.type !== "opaqueredirect" && !response.ok) throw new Error("Could not sign out.");
    location.replace("/_loki/signed-out");
  }
  catch (error) { status(error.message, true); }
});

async function poll() {
  try { await refresh(); } catch (error) { status(error.message, true); }
  setTimeout(poll, 3000);
}

setInterval(tick, 250);
poll();
