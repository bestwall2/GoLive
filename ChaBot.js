

import fs from "fs";
import { spawn } from "child_process";
// Node 24 includes global fetch; do NOT import node-fetch here
import os from "os";
import process from "process";
import path from "path";
import crypto from 'crypto';
import dotenv from 'dotenv';

dotenv.config();

/* ================= CONFIG ================= */

const CONFIG = {
  pollInterval: 20000,
  initialDelay: 50000, // 50 seconds for ALL servers initial start
  newServerDelay: 30000, // 30 seconds for NEW servers
  crashedServerDelay: 90000, // 1:30 minutes for CRASHED servers
  rotationInterval: 13500000, // 3:45 hours in milliseconds

  // Connection orchestration
  // Increased to support running 12 servers at once
  maxConcurrentConnects: 12, // number of simultaneous RTMPS handshake attempts allowed
  connectStabilityWindow: 10_000, // ms: after process 'start', wait this to call it stable (release slot earlier if desired)
  connectTimeout: 20_000, // ms: if no 'start' event in this time after run(), consider startup failed
  startupBackoffBase: 30_000, // base backoff for startup failures
  startupBackoffCap: 10 * 60_000, // cap (10min)
  globalFailureThreshold: 4, // failures within timeframe to trigger global cooldown
  globalFailureWindow: 60_000, // timeframe for counting failures (ms)
  globalCooldownDuration: 2 * 60_000, // ms: how long to pause all connecting on global failure

  // NEW: group restart behavior (true = enabled, false = disabled)
  // If enabled, when one stream that shares a token fails, other streams
  // with the same token are stopped and all are restarted together after
  // CONFIG.crashedServerDelay.
  restartGroupOnTokenFailure: true,

  // Facebook Post Configuration
  facebookPost: {
    postId: process.env.FB_POST_ID,
    accessToken: process.env.FB_ACCESS_TOKEN,
  },
};

const CACHE_FILE = "./streams_cache.json";

/* ================= STATE ================= */

let systemState = "running";
let apiItems = new Map(); // current api list with STABLE IDs
let activeStreams = new Map(); // child_processes
let streamCache = new Map(); // stream_url cache WITH creationTime
let streamStartTimes = new Map(); // track stream start times
let streamRotationTimers = new Map(); // rotation timers
let restartTimers = new Map(); // restart timers (per-stream)
let serverStates = new Map(); // server states
let startupTimer = null; // for initial startup delay
let isRestarting = false; // flag to prevent multiple restarts

// NEW: must-fix runtime variables
let isUpdatingFacebookPost = false;
let lastPostedCacheHash = null;

// Orchestration-specific
let availableConnectSlots = CONFIG.maxConcurrentConnects;
const startQueue = []; // FIFO queue for connection attempts
const connectionHolders = new Map(); // map item.id -> { held: true } if slot is held
const perStreamAttempts = new Map(); // map item.id -> attempt count (startup failures)
let recentStartupFailures = []; // timestamps of recent startup failures across all streams
let globalCooldownUntil = 200; // timestamp until which new connections are paused

// NEW: group restart timers keyed by token
const groupRestartTimers = new Map(); // token -> timeout id


/* ================= CLEANUP OLD TIMERS AFTER ROTATION ================= */

function cleanupTimersAfterRotation(itemId) {
  const item = apiItems.get(itemId);
  if (!item) return;

  log(`🧹 Cleaning up timers after rotation for ${item.name}...`);

  let cleanedCount = 0;

  // 1. Clean up any pending restart timers (rotation makes them obsolete)
  if (restartTimers.has(itemId)) {
    clearTimeout(restartTimers.get(itemId));
    restartTimers.delete(itemId);
    cleanedCount++;
    log(`  ↳ Cleared pending restart timer for ${item.name}`);
  }

  // 2. Reset per-stream attempt counter (fresh start after rotation)
  perStreamAttempts.delete(itemId);
  log(`  ↳ Reset attempt counter for ${item.name}`);

  // 3. Clean up group restart timer if this was the last stream with that token
  if (item && item.token) {
    const token = item.token;

    // Check if this was the only stream using this token
    let otherStreamsWithSameToken = 0;
    for (const [id, apiItem] of apiItems) {
      if (id !== itemId && apiItem && apiItem.token === token) {
        otherStreamsWithSameToken++;
      }
    }

    // If no other streams use this token, clean up group timer
    if (otherStreamsWithSameToken === 0 && groupRestartTimers.has(token)) {
      clearTimeout(groupRestartTimers.get(token));
      groupRestartTimers.delete(token);
      cleanedCount++;
      log(`  ↳ Cleared orphaned group restart timer for token ${token}`);
    }
  }

  // 4. Clean up recent startup failures that might be stale
  const now = Date.now();
  const threshold = now - CONFIG.globalFailureWindow;
  const initialCount = recentStartupFailures.length;
  recentStartupFailures = recentStartupFailures.filter(ts => ts > threshold);

  if (initialCount > recentStartupFailures.length) {
    const removed = initialCount - recentStartupFailures.length;
    log(`  ↳ Removed ${removed} stale startup failure records`);
  }

  // 5. Clear any stale connection holders
  if (connectionHolders.has(itemId) && !connectionHolders.get(itemId).held) {
    connectionHolders.delete(itemId);
    log(`  ↳ Removed stale connection holder for ${item.name}`);
  }

  if (cleanedCount > 0) {
    log(`✅ Cleaned up ${cleanedCount} timers after rotation for ${item.name}`);
  }
}


/* ================= STABLE ID GENERATION ================= */

function generateStableId(streamData) {
  // Create a deterministic hash from name + source for stable IDs
  const str = `${streamData.name}|${streamData.source}`;
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return `item_${Math.abs(hash).toString(16).substring(0, 8)}`;
}

/* ================= CACHE MANAGEMENT ================= */

function loadCache() {
  if (!fs.existsSync(CACHE_FILE)) {
    log(`📁 Cache file doesn't exist, will create new one`);
    return;
  }
  try {
    const json = JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"));
    Object.entries(json).forEach(([k, v]) => {
      // Ensure old cache items get creationTime if missing
      if (!v.creationTime) {
        v.creationTime = Date.now() - Math.random() * 3600000;
        log(`⚠️ Added creationTime to old cache item ${k}`);
      }
      streamCache.set(k, v);
    });
    log(`✅ Loaded ${streamCache.size} cached streams`);
  } catch (error) {
    log(`❌ Error loading cache: ${error.message}`);
    // Create fresh cache if corrupted
    streamCache.clear();
    try {
      fs.writeFileSync(CACHE_FILE, JSON.stringify({}, null, 2));
    } catch (e) {
      log(`❌ Failed to create new cache file: ${e.message}`);
    }
  }
}

function saveCache() {
  try {
    const o = {};
    streamCache.forEach((v, k) => (o[k] = v));
    fs.writeFileSync(CACHE_FILE, JSON.stringify(o, null, 2));
  } catch (error) {
    log(`❌ Error saving cache: ${error.message}`);
  }
}

/* ================= LOGGER ================= */

const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);


/* ================= ENCRYPTION FUNCTIONS ================= */

function generateKey(password) {
  try {
    // SHA-256 hash of password (matches Java implementation)
    const hash = crypto.createHash('sha256');
    hash.update(Buffer.from(password, 'utf-8'));
    const key = hash.digest();
    return key;
  } catch (err) {
    throw new Error(`Key generation error: ${err.message}`);
  }
}

function encryptData(data, password) {
  try {
    // Generate key from password using SHA-256
    const key = generateKey(password);

    // Create cipher with AES (matches Java's "AES" which defaults to AES/ECB/PKCS5Padding)
    const cipher = crypto.createCipheriv('aes-256-ecb', key, null);

    // Encrypt the data
    let encrypted = cipher.update(data, 'utf8', 'base64');
    encrypted += cipher.final('base64');

    return encrypted;
  } catch (err) {
    log(`❌ Encryption error: ${err.message}`);
    return data;
  }
}

function decryptData(encryptedData, password) {
  try {
    // Generate key from password using SHA-256
    const key = generateKey(password);

    // Create decipher with AES-256-ECB
    const decipher = crypto.createDecipheriv('aes-256-ecb', key, null);

    // Decrypt the data
    let decrypted = decipher.update(encryptedData, 'base64', 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
  } catch (err) {
    log(`❌ Decryption error: ${err.message}`);
    return encryptedData;
  }
}


/* ================= FACEBOOK API ================= */

async function createLive(token, name) {
  log(`🌐 Creating Facebook Live for: ${name}`);

  const r = await fetch("https://graph.facebook.com/v19.0/me/live_videos", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title: name,
      status: "UNPUBLISHED",
      access_token: token,
    }),
  });
  const j = await r.json();
  if (j.error) {
    log(`❌ Facebook API error: ${j.error.message}`);
    throw new Error(j.error.message);
  }
  log(`✅ Created Live ID: ${j.id}`);
  return j.id;
}

async function getStreamAndDash(liveId, token) {
  log(`🌐 Getting stream URL for Live ID: ${liveId}`);
  const fields = "stream_url,dash_preview_url,status";
  for (let i = 0; i < 6; i++) {
    try {
      const r = await fetch(
        `https://graph.facebook.com/v19.0/${liveId}?fields=${fields}&access_token=${token}`
      );
      const j = await r.json();
      if (j.stream_url) {
        log(`✅ Stream URL ready for ${liveId}`);
        return {
          stream_url: j.stream_url,
          dash: j.dash_preview_url || "N/A",
          status: j.status || "UNKNOWN",
        };
      }
      log(`⏳ Waiting for stream URL (attempt ${i + 1}/6)...`);
      await new Promise((r) => setTimeout(r, 2000));
    } catch (error) {
      log(`⚠️ Stream URL attempt ${i + 1} failed: ${error.message}`);
      if (i === 5) throw error;
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error("Preview not ready");
}

async function createLiveWithTimestamp(token, name) {
  try {
    const liveId = await createLive(token, name);
    const preview = await getStreamAndDash(liveId, token);

    return {
      liveId,
      ...preview,
      creationTime: Date.now()
    };
  } catch (error) {
    // Check if it's a token error
    if (error.message.includes("access token") ||
      error.message.includes("token") ||
      error.message.includes("OAuth") ||
      error.message.includes("permission") ||
      error.message.includes("expired") ||
      error.message.includes("invalid")) {

      const errorMsg = `❌ <b>TOKEN ERROR for ${name}</b>\n\n` +
        `Error: ${error.message}\n` +
        `Time: ${new Date().toLocaleString()}\n` +
        `Action: Stream will not start until token is fixed`;

      log(`🔴 Token error for ${name}: ${error.message}`);

      throw new Error(`TOKEN_ERROR: ${error.message}`);
    }

    throw error;
  }
}

/* ================= CONNECTION QUEUE / SEMAPHORE ================= */

function tryProcessStartQueue() {
  // If in global cooldown, do not start new connects.
  if (Date.now() < globalCooldownUntil) {
    log(`⏸️ Global cooldown active, delaying connection starts until ${new Date(globalCooldownUntil).toLocaleTimeString()}`);
    return;
  }

  while (availableConnectSlots > 0 && startQueue.length > 0) {
    const next = startQueue.shift();
    availableConnectSlots--;
    connectionHolders.set(next.id, { held: true });
    next.resolve();
  }
}

function enqueueStart(item) {
  return new Promise((resolve) => {
    // If already holding slot for this item, resolve immediately
    if (connectionHolders.has(item.id) && connectionHolders.get(item.id).held) {
      resolve();
      return;
    }

    startQueue.push({ id: item.id, resolve });
    tryProcessStartQueue();
  });
}

function releaseConnectSlot(itemId) {
  // Only release if we had previously acquired for this item
  if (connectionHolders.has(itemId) && connectionHolders.get(itemId).held) {
    connectionHolders.set(itemId, { held: false });
    availableConnectSlots = Math.min(availableConnectSlots + 1, CONFIG.maxConcurrentConnects);
    // process queued starts
    setImmediate(tryProcessStartQueue);
  }
}

/* Track global startup failures and trigger cooldown if needed */
function recordStartupFailure() {
  const now = Date.now();
  recentStartupFailures.push(now);
  // prune old entries
  recentStartupFailures = recentStartupFailures.filter(ts => now - ts <= CONFIG.globalFailureWindow);
  if (recentStartupFailures.length >= CONFIG.globalFailureThreshold) {
    globalCooldownUntil = Date.now() + CONFIG.globalCooldownDuration;
    log(`🚨 Too many startup failures (${recentStartupFailures.length}). Entering global cooldown until ${new Date(globalCooldownUntil).toLocaleTimeString()}`);
    // clear queue to avoid immediate retries piling up — they will be retried by their own timers
    while (startQueue.length > 0) {
      const queued = startQueue.shift();
      // If necessary, notify the queued start by resolving so that startFFmpeg continues and will handle per-stream backoff
      queued.resolve();
    }
    // clear recent failures after cooldown started
    recentStartupFailures = [];
  }
}

/* ================= HELPERS ================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jitteredBackoff(base, attempt, cap) {
  const exp = Math.min(cap, base * (2 ** attempt));
  const jitter = Math.round(Math.random() * Math.min(10_000, exp * 0.25));
  return exp + jitter;
}

function getUserAgent(type = "default") {
  // Simple user-agent selector. Extendable if needed.
  if (type === "mobile") {
    return "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
  }
  return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";
}


/* ================= SOURCE-TYPE ARG BUILDER =================
  Improved HTTP source handling with better reliability
*/

function buildInputArgsForSource(source) {
  const s = String(source || "").trim().toLowerCase();

  // Only handle HTTP/HTTPS streams
  if (!s.startsWith("http://") && !s.startsWith("https://")) {
    throw new Error("Only HTTP/HTTPS streams are supported");
  }

  // Detect HLS (.m3u8) streams
  const isHLS = s.includes(".m3u8") || s.includes("hls");

  if (isHLS) {
    // Optimized args for HLS / live .ts streams
    return [
      "-user_agent", getUserAgent("default"),
      "-reconnect", "1",
      "-reconnect_at_eof", "1",
      "-reconnect_streamed", "1",
      "-reconnect_on_network_error", "1",
      "-reconnect_delay_max", "10",
      "-multiple_requests", "1",
      "-rw_timeout", "0",
      "-timeout", "0",
      "-fflags", "+genpts+igndts",
      "-max_delay", "10000000", // 10 seconds buffer
      "-thread_queue_size", "16384",
      "-analyzeduration", "10M",
      "-probesize", "10M",
      "-itsoffset", "50",
      "-i", s
    ];
  } else {
    // HTTP progressive / .ts segments
    return [
      "-user_agent", getUserAgent("default"),
      // --- Input Reliability ---
      "-reconnect", "1",
      "-reconnect_at_eof", "1",
      "-reconnect_streamed", "1",
      "-reconnect_on_network_error", "1",
      "-reconnect_delay_max", "5",
      
      // --- Buffer and Analysis ---
      "-analyzeduration", "10M",
      "-probesize", "10M",
      "-thread_queue_size", "4096", // High buffer for network spikes
      "-i", s
    ];
  }
}

/* THIS TO GET FRESH IMG ALSO DASH URL READY FOR POSTING*/
function rewriteFacebookUrl(url) {
  const u = new URL(url);
  let newDomain;

  // Check if the URL is a video (mpd/m3u8 or contains /hvideo/)
  const isVideo = u.pathname.includes('/hvideo') || u.pathname.endsWith('.mpd');

  if (isVideo) {
    // Video URL
    newDomain = 'https://MatricNejma@video.xx.fbcdn.net';
  } else {
    // Image URL
    newDomain = 'https://scontent-a-mad.xx.fbcdn.net';
  }

  // Rebuild the URL with the new domain, keeping the original path and query string
  return `${newDomain}${u.pathname}${u.search}`;
}

/* ================= GET FRESH DASH URLS WITH IMAGES FOR JSON ================= */

// Function to get fresh DASH URLs and image URLs for all active streams
async function getFreshDashUrlsForPost() {
  const freshStreams = [];

  for (const [id, item] of apiItems) {
    const cache = streamCache.get(id);
    if (!cache || !cache.liveId) continue;

    try {
      // 1. Get fresh DASH URL from Facebook
      const freshData = await getStreamAndDash(cache.liveId, item.token);

      // 2. Get the image URL from Facebook API (item.img)

      let imageUrl = "";

      if (item.img) { // item.img contains Facebook post/photo ID
        try {
          // Use the stream's token to access the image
          const imageRes = await fetch(
            `https://graph.facebook.com/v19.0/${item.img}?fields=images&access_token=${item.token}`,
            { timeout: 5000 }
          );

          if (imageRes.ok) {
            const imageData = await imageRes.json();
            if (imageData?.images?.[0]?.source) {
              imageUrl = imageData.images[0].source;
              imageUrl = rewriteFacebookUrl(imageUrl);
              log(`✅ Got Facebook image URL for ${item.name}`);
            }
          }
        } catch (imgError) {
          log(`⚠️ Failed to get Facebook image for ${item.name}: ${imgError.message}`);
        }
      }

      freshStreams.push({
        img: imageUrl, // Direct image URL from API
        servers: `[{"name":"LIVE TV 🟢","url":"${rewriteFacebookUrl(freshData.dash)}"}]`,
        name: item.name
      });

      log(`✅ Got fresh DASH URL for ${item.name}`);

    } catch (error) {
      log(`⚠️ Failed to get fresh data for ${item.name}, using cached: ${error.message}`);
      // Fallback to cached DASH URL, but still use API image
      const imageUrl = "https://www.facebook.com" || ""; // Image from API

      freshStreams.push({
        img: imageUrl,
        servers: `[{"name":"LIVE TV 🟢","url":"${rewriteFacebookUrl(cache.dash)}"}]`,
        name: item.name
      });
    }
  }

  return freshStreams;
}

/* ================= UPDATE FACEBOOK POST ================= */

async function updateFacebookPost() {
  if (isUpdatingFacebookPost) return;
  isUpdatingFacebookPost = true;

  try {
    const streams = await getFreshDashUrlsForPost();


    if (streams.length === 0) {
      log("⚠️ No streams to update on Facebook");
      isUpdatingFacebookPost = false;
      return;
    }

    // Convert to JSON string
    const jsonData = JSON.stringify(streams);

    // Encrypt the data with password "ahmed"
    const encryptedData = encryptData(jsonData, "♕");

    // Create the final payload
    const payload = "ANAMATRIC" + encryptedData + "ENDMATRIC";

    if (payload === lastPostedCacheHash) {
      log("ℹ️ Facebook post already up to date");
      isUpdatingFacebookPost = false;
      return;
    }

    lastPostedCacheHash = payload;
    log(`🔄 Updating Facebook post with ${streams.length} encrypted streams`);

    // Update Facebook post
    const response = await fetch(
      `https://graph.facebook.com/v19.0/${CONFIG.facebookPost.postId}`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: new URLSearchParams({
          message: payload,
          access_token: CONFIG.facebookPost.accessToken,
        }).toString(),
      }
    );

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(`Facebook API error: ${JSON.stringify(errorData)}`);
    }

    log(`✅ Facebook post updated with encrypted data`);

  } catch (err) {
    log(`❌ Facebook update error: ${err.message}`);
  } finally {
    isUpdatingFacebookPost = false;
  }
}

/* ================= FFMPEG START (uses buildInputArgsForSource) ================= */

async function startFFmpeg(item, force = false) {
  const cache = streamCache.get(item.id);
  if (!cache) {
    log(`❌ No cache for ${item.name}, cannot start`);
    return;
  }

  if (activeStreams.has(item.id) && !force) {
    log(`⚠️ ${item.name} is already running, skipping`);
    return;
  }

  if (serverStates.get(item.id) === "token_error") {
    log(`⚠️ ${item.name} has token error; skipping start`);
    return;
  }

  // Check key age
  const timeUntilRotation = CONFIG.rotationInterval - (Date.now() - cache.creationTime);
  if (timeUntilRotation <= 0) {
    log(`⚠️ ${item.name} has expired key, rotating before starting`);
    rotateStreamKey(item);
    return;
  }

  // Acquire a connection slot (this enqueues if no slot available)
  log(`🧾 Enqueueing start for ${item.name}`);
  await enqueueStart(item);
  if (systemState !== "running") {
    releaseConnectSlot(item.id);
    return;
  }
  // mark connecting
  serverStates.set(item.id, "connecting");
  //  log(`⏳ ${item.name} is connecting (slot acquired). Waiting 5s before ffmpeg.spawn()...`);

  // small pre-start wait to reduce tight bursts (keeps startup cadence smoother)
  //await sleep(5000);

  // Build input args based on source type
  const source = item.source || "";
  const inputArgs = buildInputArgsForSource(source);

  // Output (minimal requested)
  const outputArgs = [
    // --- Codecs (Passthrough) ---
    "-c:v", "copy",
    "-c:a", "copy",
    
    // --- Facebook Specific Output Fixes ---
    "-f", "flv",
    "-flvflags", "no_duration_filesize",
    "-rtmp_live", "live",
    "-rtmp_buffer", "2000", // 2-second buffer for RTMPS overhead
    
    // --- Critical Timestamp & Interleaving Fixes ---
    "-fflags", "+genpts+discardcorrupt+igndts",
    "-max_interleave_delta", "100M", // Prevents frame drops due to timestamp gaps
    "-loglevel", "error",
    // --- Secure Connection Timeouts ---
    "-rw_timeout", "15000000", // 15 seconds (Facebook SSL can be slow)
    cache.stream_url
  ];

  const args = [...inputArgs, ...outputArgs];

  log(`▶ Spawning ffmpeg for ${item.name}: ffmpeg ${args.join(" ")}`);

  let startTimeout = null;
  let stabilityTimer = null;
  let hadStartEvent = false;
  let slotReleased = false;
  let child = null;

  function ensureReleaseSlot() {
    if (!slotReleased) {
      slotReleased = true;
      releaseConnectSlot(item.id);
    }
  }

  // connection timeout - if no 'spawn' in connectTimeout, treat as startup failure
  startTimeout = setTimeout(() => {
    if (!hadStartEvent) {
      log(`❌ ${item.name} connection start timeout (${CONFIG.connectTimeout}ms). Killing process and scheduling retry.`);
      try {
        if (child) {
          child.kill("SIGKILL");
        }
      } catch (e) { }
      ensureReleaseSlot();
      classifyStartupFailure(item);
    }
  }, CONFIG.connectTimeout);

  try {
    child = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
  } catch (err) {
    log(`❌ spawn() failed for ${item.name}: ${err.message}`);
    ensureReleaseSlot();
    classifyStartupFailure(item, err.message);
    return;
  }

  // Save in active streams immediately
  activeStreams.set(item.id, child);

  // 'spawn' event indicates child was forked; treat as start event
  child.on("spawn", () => {
    hadStartEvent = true;
    streamStartTimes.set(item.id, Date.now());
    serverStates.set(item.id, "running");
    log(`✅ FFmpeg spawned for ${item.name} (pid=${child.pid})`);
    // schedule release slot after stability window (so we avoid many simultaneous connects completing at same instant)
    stabilityTimer = setTimeout(() => {
      ensureReleaseSlot();
    }, CONFIG.connectStabilityWindow);
    // clear connect timeout
    if (startTimeout) {
      clearTimeout(startTimeout);
      startTimeout = null;
    }
    // reset per-stream startup attempt count on success
    perStreamAttempts.set(item.id, 0);
    startRotationTimer(item);
    // Update Facebook post when stream starts

  });

  child.stderr.on("data", (chunk) => {
    const text = String(chunk);
    const lines = text.split(/\r?\n/);
    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line) continue;
      // Log relevant lines
      if (line.includes("buffer") || line.includes("queue") ||
        line.includes("speed") || line.includes("bitrate") ||
        line.includes("muxing") || line.includes("delay") ||
        line.includes("Error opening output") ||
        line.includes("failed") || line.includes("Connection timed out") ||
        line.includes("Connection reset by peer") ||
        line.toLowerCase().includes("error while writing") ||
        line.toLowerCase().includes("error")) {
        log(`📊 ${item.name} FFmpeg: ${line}`);
      }
    }
  });

  child.stdout.on("data", (chunk) => {
    const text = String(chunk);
    if (text.trim()) {
      if (!hadStartEvent) {
        hadStartEvent = true;
        streamStartTimes.set(item.id, Date.now());
        serverStates.set(item.id, "running");
        log(`✅ FFmpeg stdout seen for ${item.name}`);
        if (startTimeout) {
          clearTimeout(startTimeout);
          startTimeout = null;
        }
        perStreamAttempts.set(item.id, 0);
        startRotationTimer(item);
        // Update Facebook post when stream starts

      }
    }
  });

  child.on("error", (err) => {
    const message = err && err.message ? err.message : String(err);
    log(`❌ FFmpeg spawn error for ${item.name}: ${message}`);
    ensureReleaseSlot();
    activeStreams.delete(item.id);
    if (!hadStartEvent) {
      classifyStartupFailure(item, message);
    } else {
      // runtime crash handling will consider group restart logic
      handleStreamCrash(item, message, { runtime: true });
    }
    if (startTimeout) {
      clearTimeout(startTimeout); startTimeout = null;
    }
    if (stabilityTimer) {
      clearTimeout(stabilityTimer); stabilityTimer = null;
    }
  });

  child.on("exit", (code, signal) => {
    const reason = `exit code ${code}${signal ? ` signal ${signal}` : ""}`;
    log(`🔚 FFmpeg exited for ${item.name}: ${reason}`);
    activeStreams.delete(item.id);

    if (!hadStartEvent) {
      ensureReleaseSlot();
      classifyStartupFailure(item, `Startup exit: ${reason}`);
    } else {
      // runtime crash handling will consider group restart logic
      handleStreamCrash(item, `Process exited (${reason})`, { runtime: true });
    }

    if (startTimeout) {
      clearTimeout(startTimeout); startTimeout = null;
    }
    if (stabilityTimer) {
      clearTimeout(stabilityTimer); stabilityTimer = null;
    }
  });
}

/* Classify startup failure and schedule retry with exponential backoff */
function classifyStartupFailure(item, message = "Startup failure") {
  // increment per-stream attempts
  const attempts = (perStreamAttempts.get(item.id) || 0) + 1;
  perStreamAttempts.set(item.id, attempts);

  log(`⚠️ ${item.name} startup failure #${attempts}: ${message}`);

  // record global failure for cooldown heuristics
  recordStartupFailure();

  // compute backoff
  let backoff = jitteredBackoff(CONFIG.startupBackoffBase, attempts - 1, CONFIG.startupBackoffCap);
  log(`⏰ Will retry ${item.name} in ${(backoff / 1000).toFixed(1)}s (attempt ${attempts})`);

  serverStates.set(item.id, "restarting");

  // schedule retry
  if (restartTimers.has(item.id)) {
    clearTimeout(restartTimers.get(item.id));
    restartTimers.delete(item.id);
  }
  const timer = setTimeout(() => {
    if (systemState === "running") {
      // When retrying, ensure we don't rapidly fill queue in global cooldown — the enqueueStart will wait
      startFFmpeg(apiItems.get(item.id), true).catch(err => {
        log(`⚠️ Error during retried start: ${err && err.message}`);
      });
    }
  }, backoff);
  restartTimers.set(item.id, timer);
}

/* ================= FFMPEG STOP & CRASH HANDLING ================= */

/*
  New behavior:
  - If CONFIG.restartGroupOnTokenFailure is true, a runtime crash for a stream
    will cause other streams that share the same token to be stopped and a
    grouped restart will be scheduled after CONFIG.crashedServerDelay.
*/
function handleStreamCrash(item, reason, opts = { runtime: false }) {
  const state = serverStates.get(item.id);

  // If rotating, we let rotation flow handle the resume
  if (state === "rotating") {
    log(`🔄 ${item.name} crashed during rotation: ${reason}`);
    return;
  }

  const uptime = streamStartTimes.has(item.id)
    ? formatUptime(Date.now() - streamStartTimes.get(item.id))
    : "Unknown";

  // runtime crash vs startup failure is handled elsewhere
  if (opts.runtime) {
    log(`🔄 ${item.name} will restart in ${CONFIG.crashedServerDelay / 1000}s (runtime crash)`);

    // If group-restart-by-token is enabled, attempt to stop sibling streams and schedule a group restart
    if (CONFIG.restartGroupOnTokenFailure && item && item.token) {
      const token = item.token;
      // If a group restart is already scheduled for this token, do not schedule again
      if (groupRestartTimers.has(token)) {
        log(`ℹ️ Group restart already scheduled for token ${token}, skipping duplicate schedule.`);
        // Still mark this server as restarting and stop the failed one
        serverStates.set(item.id, "restarting");
        stopFFmpeg(item.id);
        return;
      }

      // Find all API items that have the same token
      const sameTokenIds = [];
      for (const [id, apiItem] of apiItems) {
        if (apiItem && apiItem.token === token) {
          sameTokenIds.push(id);
        }
      }

      // If there's more than 1 stream with this token, we perform group stop+restart
      if (sameTokenIds.length > 1) {
        log(`🔁 Detected ${sameTokenIds.length} streams sharing token. Performing grouped restart for token ${token}.`);

        // Stop all active streams that share the token
        for (const id of sameTokenIds) {
          if (activeStreams.has(id)) {
            log(`⏹️ Stopping sibling stream ${id} (same token)`);
            try {
              stopFFmpeg(id, true);
            } catch (e) {
              log(`⚠️ Error stopping sibling ${id}: ${e.message}`);
            }
          }
        }

        // Clear any per-stream restart timers to avoid double restarts
        for (const id of sameTokenIds) {
          if (restartTimers.has(id)) {
            clearTimeout(restartTimers.get(id));
            restartTimers.delete(id);
          }
          serverStates.set(id, "restarting");
        }

        // Schedule a single grouped restart for all streams sharing this token
        const groupTimer = setTimeout(() => {
          log(`▶ Group restart timer fired for token ${token}. Restarting ${sameTokenIds.length} streams.`);
          groupRestartTimers.delete(token);

          for (const id of sameTokenIds) {
            const apiItem = apiItems.get(id);
            if (!apiItem) continue;
            if (serverStates.get(id) === "token_error") {
              log(`⚠️ Skipping start for ${apiItem.name} (token_error)`);
              continue;
            }
            startFFmpeg(apiItem).catch(e => {
              log(`⚠️ Error starting ${apiItem.name} during group restart: ${e && e.message}`);
            });
          }
        }, CONFIG.crashedServerDelay);

        groupRestartTimers.set(token, groupTimer);

        return;
      }
      // otherwise fall-through to single-stream restart behavior below
    }

    // Default single-stream runtime crash behavior (no group restart or only one stream with token)
    serverStates.set(item.id, "restarting");
    stopFFmpeg(item.id);

    if (restartTimers.has(item.id)) {
      clearTimeout(restartTimers.get(item.id));
      restartTimers.delete(item.id);
    }
    const restartTimer = setTimeout(() => {
      if (systemState === "running") {
        startFFmpeg(item).catch(e => log(`⚠️ Error restarting after runtime crash: ${e.message}`));
      }
    }, CONFIG.crashedServerDelay);
    restartTimers.set(item.id, restartTimer);
  } else {
    // startup-related crashes are handled in classifyStartupFailure which schedules a retry
    log(`⚠️ ${item.name} startup crash classified earlier: ${reason}`);
  }
}

function stopFFmpeg(id, skipReport = false) {
  try {
    const proc = activeStreams.get(id);
    if (proc) {
      try { proc.kill("SIGTERM"); } catch { }
      setTimeout(() => {
        try { proc.kill("SIGKILL"); } catch { }
      }, 5000);

      if (!skipReport) {
        const state = serverStates.get(id);
        if (state === "running") {
          const item = apiItems.get(id);
          if (item) {
            const uptime = streamStartTimes.has(id)
              ? formatUptime(Date.now() - streamStartTimes.get(id))
              : "Unknown";
            log(`⏹️ Stopped ${item.name} (was running for ${uptime})`);
          }
        }
      }
    }
  } catch (err) {
    log(`❌ Error stopping ${id}: ${err.message}`);
  }

  activeStreams.delete(id);
  streamStartTimes.delete(id);
  // Ensure we release any slot we thought we held for this id
  releaseConnectSlot(id);
}

/* ================= SYSTEM RESTART ================= */

async function restartSystem() {
  log("🔄 SYSTEM RESTART COMMAND RECEIVED - Executing PM2 restart...");

  const { exec } = await import("child_process");
  exec("pm2 restart ChatBot", (error, stdout, stderr) => {
    if (error) {
      log(`❌ PM2 restart failed: ${error.message}`);
      return;
    }
    log(`✅ PM2 restart initiated: ${stdout}`);
  });
}


/* ================= ROTATION SYSTEM ================= */

function startRotationTimer(item) {
  if (streamRotationTimers.has(item.id)) {
    clearTimeout(streamRotationTimers.get(item.id));
  }

  const cache = streamCache.get(item.id);
  if (!cache) return;

  const timeUntilRotation = CONFIG.rotationInterval - (Date.now() - cache.creationTime);

  if (timeUntilRotation <= 0) {
    log(`⏰ ${item.name} key has expired, rotating now`);
    rotateStreamKey(item);
    return;
  }

  const minutesLeft = Math.round(timeUntilRotation / 1000 / 60);
  const hoursLeft = (timeUntilRotation / 1000 / 60 / 60).toFixed(1);
  log(`⏰ Rotation timer for ${item.name}: ${minutesLeft} minutes (${hoursLeft} hours) remaining`);

  const rotationTimer = setTimeout(async () => {
    log(`🔄 Global rotation triggered by ${item.name} timer`);
    await rotateAllStreams();
  }, timeUntilRotation);

  streamRotationTimers.set(item.id, rotationTimer);
}

async function rotateAllStreams() {
  if (isRestarting) {
    log("⚠️ Rotation/Restart already in progress, skipping redundant call.");
    return;
  }
  isRestarting = true;
  systemState = "rotating";

  log("🔄 Starting global rotation for ALL streams simultaneously...");

  // Stop all active FFmpeg processes immediately
  for (const [id, proc] of activeStreams) {
    log(`⏹️ Stopping ${id} for global rotation`);
    stopFFmpeg(id, true);
  }
  activeStreams.clear();

  // Clear all existing timers
  streamRotationTimers.forEach(t => clearTimeout(t));
  streamRotationTimers.clear();
  restartTimers.forEach(t => clearTimeout(t));
  restartTimers.clear();

  const now = Date.now();

  // Rotate each stream sequentially to respect potential API limits
  for (const [id, item] of apiItems) {
    log(`🔄 Rotating ${item.name}...`);
    serverStates.set(id, "rotating");
    try {
      const newCache = await createLiveWithTimestamp(item.token, item.name);
      newCache.creationTime = now; // Synchronize ages
      streamCache.set(id, newCache);
      log(`✅ Successfully rotated ${item.name}`);
    } catch (error) {
      log(`❌ Failed to rotate ${item.name}: ${error.message}`);
      // Even if one fails, we continue with others and still restart at the end
    }
  }

  saveCache();

  try {
    log("🔄 Updating Facebook post after global rotation...");
    await updateFacebookPost();
  } catch (err) {
    log(`⚠️ Facebook post update failed: ${err.message}`);
  }

  log("✅ Global rotation complete. Restarting system in 10s...");
  await sleep(10000);
  restartSystem();
}

async function rotateStreamKey(item) {
  // Now simply triggers global rotation
  await rotateAllStreams();
}



/* ================= UPTIME CALCULATION ================= */

function formatUptime(uptimeMs) {
  if (!uptimeMs || uptimeMs < 0) return "Not active";

  const seconds = Math.floor((uptimeMs / 1000) % 60);
  const minutes = Math.floor((uptimeMs / (1000 * 60)) % 60);
  const hours = Math.floor((uptimeMs / (1000 * 60 * 60)) % 24);
  const days = Math.floor(uptimeMs / (1000 * 60 * 60 * 24));

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);

  return parts.join(" ");
}

function formatTimeSinceCreation(itemId) {
  const cache = streamCache.get(itemId);
  if (!cache || !cache.creationTime) return "Unknown";

  const age = Date.now() - cache.creationTime;
  return formatUptime(age);
}

/* ================= STATUS REPORT FOR DASHBOARD ================= */

function sendStatusReport() {
  const activeCount = activeStreams.size;
  const totalCount = apiItems.size;

  let report = `<b>Summary: Active: ${activeCount} | Total: ${totalCount}</b>\n`;
  report += `------------------------------------------\n`;

  for (const [id, cache] of streamCache) {
    const item = apiItems.get(id);
    const startTime = streamStartTimes.get(id);
    const state = serverStates.get(id);
    const isActive = activeStreams.has(id);

    if (item) {
      const keyAge = formatTimeSinceCreation(id);
      const creationTime = cache.creationTime ?
        new Date(cache.creationTime).toLocaleString() : "Unknown";

      report += `\n<b>${item.name}</b>\n`;
      report += `• Status: ${state || "unknown"}\n`;
      report += `• Active: ${isActive ? "🟢" : "🔴"}\n`;
      report += `• Stream Uptime: ${formatUptime(
        startTime ? Date.now() - startTime : 0
      )}\n`;
      report += `• Key Age: ${keyAge} (created: ${creationTime})\n`;
    }
  }

  if (streamCache.size === 0) {
    report += `\nNo streams configured.\n`;
  }

  try {
    const statusPath = path.join(process.cwd(), 'dashboard', 'status.txt');
    fs.writeFileSync(statusPath, report, 'utf8');
  } catch (err) {
    log(`❌ Error writing status report: ${err.message}`);
  }
}

/* ================= API FETCH WITH STABLE IDS ================= */

async function fetchApiList() {
  try {
    const filePath = path.join(process.cwd(), 'dashboard', 'channels.json');
    if (!fs.existsSync(filePath)) {
      log(`❌ Channels file not found: ${filePath}`);
      return new Map();
    }

    const data = fs.readFileSync(filePath, 'utf8');
    const channels = JSON.parse(data);

    const map = new Map();
    if (Array.isArray(channels)) {
      channels.forEach((item) => {
        // Map fields to match what ChaBot expects
        const streamData = {
          name: item.channelName,
          token: item.pageToken,
          source: item.channelSource,
          img: item.imageUrl
        };
        const id = generateStableId(streamData); // STABLE ID
        map.set(id, {
          id: id,
          ...streamData
        });
      });
    }
    return map;
  } catch (error) {
    log(`❌ Error reading local channels: ${error.message}`);
    return new Map(); // Return empty map on error
  }
}

/* ================= FULL CACHE SYNCHRONIZATION ================= */

async function synchronizeCacheWithApi() {
  if (isRestarting) {
    log("⏳ System is currently rotating/restarting. Skipping synchronization until complete.");
    return { removedCount: 0, addedCount: 0 };
  }

  const newApiItems = await fetchApiList();

  log(`🔄 Starting cache synchronization...`);
  log(`📊 API: ${newApiItems.size} items, Cache: ${streamCache.size} entries`);

  // Find existing creationTime to synchronize new servers
  let referenceCreationTime = Date.now();
  for (const [id, cache] of streamCache) {
    if (cache.creationTime) {
      referenceCreationTime = cache.creationTime;
      break;
    }
  }

  // 1. Remove cache entries that no longer exist in API
  let removedCount = 0;
  for (const [cacheId] of streamCache) {
    if (!newApiItems.has(cacheId)) {
      log(`🧹 Removing orphaned cache: ${cacheId}`);

      // Clean up timers
      if (restartTimers.has(cacheId)) {
        clearTimeout(restartTimers.get(cacheId));
        restartTimers.delete(cacheId);
      }
      if (streamRotationTimers.has(cacheId)) {
        clearTimeout(streamRotationTimers.get(cacheId));
        streamRotationTimers.delete(cacheId);
      }

      // Stop FFmpeg if running
      stopFFmpeg(cacheId, true);

      // Remove from state maps
      streamCache.delete(cacheId);
      streamStartTimes.delete(cacheId);
      serverStates.delete(cacheId);

      removedCount++;
    }
  }

  // 2. Create cache entries for new API items
  let addedCount = 0;
  for (const [id, item] of newApiItems) {
    if (!streamCache.has(id)) {
      log(`🆕 Creating cache for: ${item.name} (Wait for global rotation cycle)`);
      try {
        const newCache = await createLiveWithTimestamp(item.token, item.name);
        // Align age with existing servers so it rotates at the same time
        newCache.creationTime = referenceCreationTime;
        streamCache.set(id, newCache);
        addedCount++;
        log(`✅ Created cache for ${item.name} with synchronized age.`);
      } catch (error) {
        log(`❌ Failed to create cache for ${item.name}: ${error.message}`);
        if (error.message.includes("TOKEN_ERROR")) {
          serverStates.set(id, "token_error");
        }
      }
    }
  }

  // 3. Save updated cache
  if (removedCount > 0 || addedCount > 0) {
    saveCache();
    log(`✅ Sync complete: Removed ${removedCount}, Added ${addedCount}`);

    // Update Facebook post when cache changes
    updateFacebookPost().catch((err) =>
      log(`⚠️ Error updating Facebook post after cache sync: ${err.message}`)
    );
  }

  // 4. Update global apiItems
  apiItems = newApiItems;

  // 5. Verify synchronization
  log(`📊 Final state: API=${apiItems.size}, Cache=${streamCache.size}`);

  if (apiItems.size !== streamCache.size) {
    log(`⚠️ Cache/API mismatch after sync! API: ${apiItems.size}, Cache: ${streamCache.size}`);
    // Force cleanup of any remaining orphans
    const orphanedIds = [];
    for (const [cacheId] of streamCache) {
      if (!apiItems.has(cacheId)) {
        orphanedIds.push(cacheId);
      }
    }
    if (orphanedIds.length > 0) {
      log(`🧹 Removing ${orphanedIds.length} remaining orphans`);
      orphanedIds.forEach(id => streamCache.delete(id));
      saveCache();

      // Update Facebook post after orphan cleanup
      updateFacebookPost().catch((err) =>
        log(
          `⚠️ Error updating Facebook post after orphan cleanup: ${err.message}`
        )
      );
    }
  }

  return { removedCount, addedCount };
}

/* ================= WATCHER (USES FULL SYNC) ================= */

async function watcher() {
  try {
    const syncResult = await synchronizeCacheWithApi();

    // Start streams for newly added items (with delay)
    if (syncResult.addedCount > 0) {
      log(`⏰ ${syncResult.addedCount} new items will start in ${CONFIG.newServerDelay / 1000} seconds`);
      setTimeout(() => {
        for (const [id, item] of apiItems) {
          // Only enqueue start if we have cache and not already running
          if (streamCache.has(id) && !activeStreams.has(id) && serverStates.get(id) !== "token_error") {
            // We enqueue start instead of starting synchronously to avoid bursts
            startFFmpeg(item);
          }
        }
      }, CONFIG.newServerDelay);
    }

  } catch (error) {
    log(`❌ Watcher error: ${error.message}`);
  }
}


/* ================= BOOT WITH PROPER SYNCHRONIZATION ================= */

async function boot() {
  log("🚀 Booting Stream Manager...");

  try {
    // 1. Load existing cache
    loadCache();

    // 2. Perform initial synchronization
    log(`🔄 Performing initial cache synchronization...`);
    const syncResult = await synchronizeCacheWithApi();

    log(`📋 Loaded ${apiItems.size} items from API`);
    log(`💾 Loaded ${streamCache.size} cached streams`);

    // 3. Check for old stream keys
    log(`🔍 Checking for old stream keys on startup...`);
    await checkAndRotateOldKeys();

    // 4. Send startup notification
    const delaySeconds = CONFIG.initialDelay / 1000;
    log(`🚀 Stream Manager Started. API Items: ${apiItems.size}, Cache Entries: ${streamCache.size}, Sync Status: ${syncResult.removedCount} removed, ${syncResult.addedCount} added. Checked old keys: ✅ Done.`);

    // 5. Wait before starting all servers
    log(`⏳ Waiting ${delaySeconds} seconds before starting all servers...`);
    // Update initial Facebook post
    await updateFacebookPost();

    startupTimer = setTimeout(() => {
      log(`▶ Starting ALL servers after ${delaySeconds} second delay`);

      // Start servers that have cache and no token errors
      let startedCount = 0;
      for (const [id, item] of apiItems) {
        if (streamCache.has(id) && serverStates.get(id) !== "token_error") {
          // Enqueue the start rather than starting immediately to avoid bursts
          startFFmpeg(item);
          startedCount++;
        }
      }

      log(`✅ Enqueued ${startedCount}/${apiItems.size} servers for start`);

      // 6. Start old key checker
      setInterval(checkAndRotateOldKeys, 3600000);
      log(`🔍 Old key checker started (every hour)`);

      // 8. Start periodic status report
      sendStatusReport();
      setInterval(sendStatusReport, 30000);
      log(`📊 Status reports started (every 30s)`);
    }, CONFIG.initialDelay);

  } catch (error) {
    log(`❌ Boot failed: ${error.message}`);
    setTimeout(boot, 60000);
  }
}

/* ================= OLD KEY CHECKER (unchanged) ================= */

async function checkAndRotateOldKeys() {
  log(`🔍 Checking for old stream keys (> ${CONFIG.rotationInterval / 1000 / 60 / 60} hours)...`);

  const now = Date.now();
  let needsGlobalRotation = false;

  for (const [id, cache] of streamCache) {
    const item = apiItems.get(id);
    if (!item) continue;

    const age = now - cache.creationTime;
    if (age >= CONFIG.rotationInterval) {
      log(`🔄 Found old stream key for ${item.name} (${(age / 3600000).toFixed(2)} hours old)`);
      needsGlobalRotation = true;
      break;
    }
  }

  if (needsGlobalRotation) {
    log("⏰ Triggering global rotation because at least one key is old.");
    await rotateAllStreams();
  } else {
    log("✅ All stream keys are currently within rotation interval.");
  }
}

/* ================= SHUTDOWN ================= */

async function gracefulShutdown() {
  systemState = "stopping";
  log("🛑 Shutting down gracefully...");

  if (startupTimer) {
    clearTimeout(startupTimer);
  }

  log(`🛑 Stream Manager Shutting Down. Stopping ${activeStreams.size} active streams. Cleaning up all timers.`);

  restartTimers.forEach((timer, id) => {
    clearTimeout(timer);
  });
  streamRotationTimers.forEach((timer, id) => {
    clearTimeout(timer);
  });

  // Clear group restart timers too
  for (const [token, t] of groupRestartTimers) {
    clearTimeout(t);
  }
  groupRestartTimers.clear();

  for (const [id] of activeStreams) {
    stopFFmpeg(id, true);
  }

  await new Promise((r) => setTimeout(r, 2000));

  log("👋 Shutdown complete");
  process.exit(0);
}

/* ================= START THE SYSTEM ================= */

boot();

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
