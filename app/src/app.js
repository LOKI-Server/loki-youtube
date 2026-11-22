"use strict";

const $ = id => document.getElementById(id);
const csrf = document.querySelector('meta[name="csrf-token"]').content;
let found = null;           // The last link looked up, with its choices.
let clock = { server: 0, at: performance.now() };
let timer = null;
const collecting = new Set();

async function api(path, method = "GET", data) {
  const request = { method, headers: { "x-csrf-token": csrf }, cache: "no-store" };
  if (data !== undefined) {
    request.headers["content-type"] = "application/json";
    request.body = JSON.stringify(data);
  }
  const response = await fetch(path, request);
  const json = await response.json().catch(() => null);
  if (!response.ok || !json) {
    if (response.status === 401) throw new Error("Your LOKI session ended. Reload the page to sign in again.");
    throw new Error(json?.error || "Something went wrong. Try again.");
  }
  return json;
}

function say(message, tone = "") {
  $("status").textContent = message;
  $("status").className = tone;
}

const size = bytes => {
  if (!bytes) return "";
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
  if (bytes >= 1048576) return `${Math.round(bytes / 1048576)} MB`;
  return `${Math.max(1, Math.round(bytes / 1024))} KB`;
};

const length = seconds => {
  if (!seconds) return "";
  seconds = Math.round(seconds);
  const h = Math.floor(seconds / 3600), m = Math.floor(seconds / 60) % 60, s = String(seconds % 60).padStart(2, "0");
  return h ? `${h}:${String(m).padStart(2, "0")}:${s}` : `${m}:${s}`;
};

function el(tag, props = {}, ...children) {
  const { dataset, ...rest } = props;
  const node = Object.assign(document.createElement(tag), rest);
  if (dataset) Object.assign(node.dataset, dataset);
  node.append(...children.filter(child => child !== null && child !== undefined && child !== ""));
  return node;
}

// Looking up a link.
async function inspect() {
  const url = $("url").value.trim();
  if (!url) return;
  const button = $("inspect").querySelector("button");
  button.disabled = true;
  $("choose").hidden = true;
  found = null;
  say("Looking up that link…", "busy");
  try {
    found = await api("/api/inspect", "POST", { url });
    $("title").textContent = found.title;
    $("details").textContent = [found.site, found.uploader, length(found.duration)].filter(Boolean).join(" · ");
    // Start on 1080p or the best below it; 4K files are often several gigabytes.
    const preferred = found.choices.find(c => /^v\d+$/.test(c.id) && Number(c.id.slice(1)) <= 1080) || found.choices[0];
    $("choices").replaceChildren(...found.choices.map(choice => {
      const input = el("input", { type: "radio", name: "choice", value: choice.id, checked: choice === preferred });
      return el("label", { className: "choice" }, input, el("span", { className: "label", textContent: choice.label }), el("span", { className: "muted", textContent: choice.size ? `~${size(choice.size)}` : "" }));
    }));
    $("choose").hidden = false;
    say(found.merge ? "" : "The server has no ffmpeg, so only qualities with sound and picture together are offered.", "muted");
  } catch (error) {
    say(error.message, "error");
  } finally {
    button.disabled = false;
  }
}

$("inspect").addEventListener("submit", event => { event.preventDefault(); inspect(); });
// Pasting a link looks it up straight away.
$("url").addEventListener("paste", () => setTimeout(() => { if ($("url").checkValidity()) inspect(); }, 0));

$("cancel-choice").addEventListener("click", () => { $("choose").hidden = true; found = null; say(""); });

$("choose").addEventListener("submit", async event => {
  event.preventDefault();
  if (!found) return;
  const picked = $("choices").querySelector("input:checked");
  const choice = found.choices.find(c => c.id === picked?.value);
  if (!choice) return;
  const button = $("choose").querySelector('button[type="submit"]');
  button.disabled = true;
  try {
    await api("/api/downloads", "POST", { url: found.url, choice: choice.id, title: found.title, quality: choice.label, site: found.site || "" });
    $("choose").hidden = true;
    $("url").value = "";
    found = null;
    say("Downloading. It appears below, and you can leave this page while it works.", "ok");
    await refresh();
  } catch (error) {
    say(error.message, "error");
  } finally {
    button.disabled = false;
  }
});

// The list of downloads.
const states = {
  downloading: "Downloading",
  processing: "Finishing up",
  collecting: "Saving to LOKI",
  ready: "Ready",
  failed: "Failed",
  cancelled: "Cancelled",
  expired: "Expired",
};

function row(item) {
  const active = ["downloading", "processing", "collecting"].includes(item.status);
  const meta = [item.quality, item.site, item.status === "ready" || item.status === "expired" ? size(item.size) : "", new Date(item.created * 1000).toLocaleString()].filter(Boolean).join(" · ");
  const badge = el("span", { className: `badge ${item.status}`, textContent: states[item.status] || item.status });
  const body = el("div", { className: "item-body" },
    el("div", { className: "item-title" }, el("strong", { textContent: item.title }), badge),
    el("div", { className: "muted small", textContent: meta }));
  if (active) {
    const percent = item.percent ?? null;
    const bar = el("progress", { max: 100 });
    if (percent !== null && item.status !== "processing") bar.value = percent;
    const label = item.status === "processing" ? "Merging and tagging the file…"
      : item.status === "collecting" ? `Copying to LOKI${percent !== null ? ` · ${Math.floor(percent)}%` : ""}`
      : percent !== null ? `${Math.floor(percent)}%` : "Starting…";
    body.append(bar, el("div", { className: "muted small", textContent: label }));
  }
  if (item.status === "failed" && item.error) body.append(el("div", { className: "error small", textContent: item.error }));
  const actions = el("div", { className: "item-actions" });
  if (item.status === "ready") {
    actions.append(el("span", { className: "countdown muted small", dataset: { expires: item.expires } }));
    actions.append(el("a", { className: "button primary", href: `/files/${encodeURIComponent(item.id)}`, textContent: "Save file" }));
  }
  const remove = el("button", { type: "button", className: "ghost", textContent: active ? "Cancel" : "Remove" });
  remove.addEventListener("click", async () => {
    if (active && !confirm("Stop this download?")) return;
    remove.disabled = true;
    try { await api(`/api/downloads/${encodeURIComponent(item.id)}`, "DELETE"); await refresh(); }
    catch (error) { say(error.message, "error"); remove.disabled = false; }
  });
  actions.append(remove);
  return el("li", { className: "item" }, body, actions);
}

async function refresh() {
  const { items, now, keep_minutes } = await api("/api/downloads");
  clock = { server: now, at: performance.now() };
  $("keep").textContent = `Finished files stay ready to save for ${keep_minutes} minutes.`;
  $("list").replaceChildren(...(items.length ? items.map(row) : [el("li", { className: "empty muted", textContent: "Nothing yet. Paste a link above to start." })]));
  tick();
  for (const item of items) if (item.status === "collecting") collect(item.id);
  return items.some(item => ["downloading", "processing", "collecting"].includes(item.status));
}

// Moves a finished file from the server's worker into LOKI, one step at a time.
async function collect(id) {
  if (collecting.has(id)) return;
  collecting.add(id);
  try {
    let item;
    do { item = await api(`/api/downloads/${encodeURIComponent(id)}/collect`, "POST"); }
    while (item.status === "collecting" && document.visibilityState !== "hidden");
  } catch (error) {
    say(error.message, "error");
  } finally {
    collecting.delete(id);
  }
}

function tick() {
  const now = clock.server + Math.floor((performance.now() - clock.at) / 1000);
  for (const node of document.querySelectorAll("[data-expires]")) {
    const left = Math.max(0, Number(node.dataset.expires) - now);
    const m = Math.floor(left / 60), s = String(left % 60).padStart(2, "0");
    node.textContent = left ? `Ready for ${m}:${s}` : "Expired";
    node.nextElementSibling?.toggleAttribute("hidden", left === 0);
  }
}

async function poll() {
  let busy = false;
  try { busy = await refresh(); } catch (error) { say(error.message, "error"); }
  clearTimeout(timer);
  timer = setTimeout(poll, busy ? 2000 : 15000);
}

$("clear").addEventListener("click", async () => {
  if (!confirm("Remove finished, failed and expired downloads from your list?")) return;
  try { await api("/api/downloads", "DELETE"); await refresh(); }
  catch (error) { say(error.message, "error"); }
});

$("logout").addEventListener("click", async () => {
  try {
    const response = await fetch("/logout", { method: "POST", headers: { "x-csrf-token": csrf }, redirect: "manual" });
    if (response.type !== "opaqueredirect" && !response.ok) throw new Error("Could not sign out.");
    location.replace("/_loki/signed-out");
  } catch (error) { say(error.message, "error"); }
});

document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") poll(); });
setInterval(tick, 1000);
poll();
