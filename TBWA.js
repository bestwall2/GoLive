import fs from 'fs';
import path from 'path';
import pino from 'pino';
import {
  makeWASocket,
  useMultiFileAuthState,
  delay,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  Browsers,
  DisconnectReason,
} from '@whiskeysockets/baileys';
import { spawn } from 'child_process';

// ===== CONFIG =====
const PHONE_NUMBER = '212629996310';
const SESSION_DIR = `./session`;
const MAX_DURATION = 4 * 60 * 60 * 1000;
const MAX_RETRIES = 30000;

const ADMIN_NUMBERS = [
  '269835950931970@lid',
  '120363422983771446@g.us',
  '115371696771153@lid',
];


const ALLOWED_CHATS_FILE = path.join(SESSION_DIR, 'allowed_chats.json');

if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

function sessionExists() {
  const credsPath = path.join(SESSION_DIR, 'creds.json');
  return fs.existsSync(credsPath);
}

function clearSession() {
  if (!fs.existsSync(SESSION_DIR)) return;
  console.log('🗑️ Clearing session data...');
  try {
    const files = fs.readdirSync(SESSION_DIR);
    for (const file of files) {
      const filePath = path.join(SESSION_DIR, file);
      if (fs.statSync(filePath).isFile()) {
        fs.unlinkSync(filePath);
      }
    }
    console.log('✅ Session data cleared');
  } catch (error) {
    console.error('❌ Error clearing session:', error);
  }
}

// ===== ALLOWED CHATS MANAGEMENT =====
function loadAllowedChats() {
  if (!fs.existsSync(ALLOWED_CHATS_FILE)) {
    const defaultChats = [
      '269835950931970@lid',
      '115371696771153@lid',
      '8044943519812@lid',
      '120363406529443583@g.us',
    ];
    fs.writeFileSync(ALLOWED_CHATS_FILE, JSON.stringify({ chats: defaultChats }, null, 2));
    return defaultChats;
  }
  const data = fs.readFileSync(ALLOWED_CHATS_FILE, 'utf-8');
  return JSON.parse(data).chats || [];
}

function saveAllowedChats(chats) {
  fs.writeFileSync(ALLOWED_CHATS_FILE, JSON.stringify({ chats }, null, 2));
}

function addAllowedChat(chatId) {
  const chats = loadAllowedChats();
  if (!chats.includes(chatId)) {
    chats.push(chatId);
    saveAllowedChats(chats);
    return true;
  }
  return false;
}

function removeAllowedChat(chatId) {
  const chats = loadAllowedChats();
  const index = chats.indexOf(chatId);
  if (index > -1) {
    chats.splice(index, 1);
    saveAllowedChats(chats);
    return true;
  }
  return false;
}

function isAdminUser(chatId) {
  return ADMIN_NUMBERS.includes(chatId);
}

function isAllowedChat(chatId) {
  const allowedChats = loadAllowedChats();
  return allowedChats.includes(chatId);
}

// ===== STREAM MANAGEMENT =====
const registeredStreams = new Map();
const runningStreams = new Map();

const stats = {
  streamsStarted: 0,
  streamsStopped: 0,
  totalStreamTime: 0,
  errors: 0,
  lastCleanup: Date.now(),
  botStartTime: Date.now()
};

// ===== UTILITIES =====
function isValidUrl(string) {
  try {
    new URL(string);
    return true;
  } catch (_) {
    return false;
  }
}

function detectSourceType(url) {
  if (/\.m3u8(\?|$)/i.test(url)) return "m3u8";
  if (/\.ts(\?|$)/i.test(url)) return "ts";
  if (/^https?:\/\//i.test(url)) return "http";
  return "unknown";
}

function getRtmpUrl(streamKey, platform = "facebook") {
  if (platform === "instagram") {
    return `rtmps://live-upload.instagram.com:443/rtmp/${streamKey}`;
  }
  return `rtmps://live-api-s.facebook.com:443/rtmp/${streamKey}`;
}

function formatDuration(milliseconds) {
  const hours = Math.floor(milliseconds / 3600000);
  const minutes = Math.floor((milliseconds % 3600000) / 60000);
  const seconds = Math.floor((milliseconds % 60000) / 1000);
  
  if (hours > 0) {
    return `${hours}س ${minutes}د`;
  } else if (minutes > 0) {
    return `${minutes}د ${seconds}ث`;
  } else {
    return `${seconds}ث`;
  }
}

// ===== FFMPEG FUNCTIONS =====
function buildCopyArgs(sourceUrl, rtmpUrl, platform = "facebook") {
  const args = [
    "-hide_banner",
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_delay_max", "15",
    "-rw_timeout", "0",
    "-timeout", "0",
    "-analyzeduration", "5000000",
    "-probesize", "5000000",
    "-fflags", "+genpts+discardcorrupt",
    "-err_detect", "ignore_err",
    "-i", sourceUrl,
    "-c:v", "copy",
    "-c:a", "copy",
    "-r", "30",
    "-f", "flv",
    "-loglevel", "error"
  ];

  if (platform === "instagram") {
    args.splice(args.indexOf("-c:v"), 0, "-vf");
    args.splice(args.indexOf("-c:v"), 0, "transpose=1");
  }

  args.push(rtmpUrl);
  return args;
}

function buildEncodeWithWatermarkArgs(sourceUrl, rtmpUrl, platform = "facebook", drawText = null) {
  const videoFilters = [];

  if (platform === "instagram") {
    videoFilters.push("transpose=1");
  }

  videoFilters.push(
    "fps=25",
    "eq=brightness=0.05:saturation=1.2",
  );

  if (platform === "instagram") {
    videoFilters.push("drawbox=x=iw-80-10:y=ih-200-10:w=80:h=200:color=black:t=fill");
  } else {
    videoFilters.push(
      "drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:" +
      "text='Matric Nejma':x=50:y=70:fontsize=62:fontcolor=white:" +
      "box=1:boxcolor=black@1:boxborderw=16",
      "drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:" +
      "text='Matric Nejma':x=w-tw-50:y=70:fontsize=58:fontcolor=white:" +
      "box=1:boxcolor=black@1:boxborderw=14",
      "drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:" +
      "text='حمل تطبيق Matric Nejma لمشاهدة مجانا':" +
      "x=(w-tw)/2:y=h-th-35:" +
      "fontsize=40:fontcolor=yellow:" +
      "box=1:boxcolor=black@0.85:boxborderw=12"
    );
  }

  const args = [
    "-hide_banner",
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_delay_max", "15",
    "-rw_timeout", "0",
    "-timeout", "0",
    "-analyzeduration", "5000000",
    "-probesize", "5000000",
    "-fflags", "+genpts+discardcorrupt",
    "-err_detect", "ignore_err",
    "-i", sourceUrl,
    "-vf", videoFilters.join(","),
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-tune", "zerolatency",
    "-g", "50",
    "-b:v", "2200k",
    "-maxrate", "2500k",
    "-bufsize", "4400k",
    "-c:a", "aac",
    "-ar", "44100",
    "-b:a", "128k",
    "-f", "flv",
    rtmpUrl
  ];

  return args;
}

// ===== STREAM FUNCTIONS =====
function cleanupExpiredStreams() {
  const now = Date.now();
  let cleanedRunning = 0;
  let cleanedRegistered = 0;

  for (const [name, stream] of runningStreams.entries()) {
    if (now - stream.startTime >= MAX_DURATION) {
      console.log(`🧹 تنظيف Stream منتهي المدة: ${name}`);
      try {
        if (stream.process) {
          stream.process.kill("SIGKILL");
        }
      } catch (e) {
        console.error(`خطأ في إيقاف ${name}:`, e);
        stats.errors++;
      }
      runningStreams.delete(name);
      cleanedRunning++;

      if (registeredStreams.has(name)) {
        registeredStreams.delete(name);
        cleanedRegistered++;
      }
      
      stats.streamsStopped++;
      stats.totalStreamTime += MAX_DURATION;
    }
  }

  return { cleanedRunning, cleanedRegistered };
}

async function startStreamByName(name, chatId, sock) {
  if (!registeredStreams.has(name)) {
    await sendMessage(chatId, `❌ *لا يوجد Stream مسجّل بالاسم:* ${name}`, sock);
    return;
  }

  const streamData = registeredStreams.get(name);

  if (streamData.owner !== chatId) {
    return;
  }

  cleanupExpiredStreams();

  if (runningStreams.has(name)) {
    const stream = runningStreams.get(name);
    const elapsed = Date.now() - stream.startTime;
    const timeLeft = Math.max(0, MAX_DURATION - elapsed);
    
    await sendMessage(chatId, 
      `⚠️ *الـ Stream "${name}" يعمل بالفعل*\n` +
      `⏰ *منذ:* ${formatDuration(elapsed)}\n` +
      `⏳ *متبقي:* ${formatDuration(timeLeft)}\n` +
      `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
    return;
  }

  const { sourceUrl, streamKey, type, drawText, platform = "facebook" } = streamData;
  const rtmpUrl = getRtmpUrl(streamKey, platform);
  const streamType = type || "normal";

  stats.streamsStarted++;

  function runFFmpegProcess(retryCount = 0) {
    let useEncoding = streamType === "encode-with-watermark";
    let args;

    if (useEncoding) {
      args = buildEncodeWithWatermarkArgs(sourceUrl, rtmpUrl, platform, drawText);
    } else {
      args = buildCopyArgs(sourceUrl, rtmpUrl, platform);
    }

    console.log(`▶ [${name}] FFmpeg start [${streamType}] → ${platform} ${streamKey}`);
    const ffmpeg = spawn("ffmpeg", args);

    let shouldSwitchToEncoding = false;
    let switchAttempted = false;

    ffmpeg.stderr.on("data", data => {
      const log = data.toString();
      console.log(`[${name}] ffmpeg:`, log.trim());

      if (streamType === "normal" && !useEncoding && !switchAttempted) {
        const encodingNeeded = log.includes("codec not supported") ||
          log.includes("Invalid argument") ||
          log.includes("Video codec") ||
          log.includes("Audio codec") ||
          log.includes("unsupported codec");

        if (encodingNeeded) {
          console.log(`⚠️ [${name}] Codec rejected → switching to ENCODE mode`);
          shouldSwitchToEncoding = true;
          switchAttempted = true;

          setTimeout(() => {
            try {
              ffmpeg.kill("SIGKILL");
            } catch (e) {
              console.error(`Error killing process:`, e);
              stats.errors++;
            }
          }, 500);
        }
      }
    });

    ffmpeg.on("close", (code, signal) => {
      console.log(`❌ [${name}] FFmpeg closed (code: ${code}, signal: ${signal})`);

      if (!runningStreams.has(name)) return;

      const stream = runningStreams.get(name);
      const now = Date.now();
      const elapsed = now - stream.startTime;

      if (elapsed >= MAX_DURATION) {
        console.log(`⏰ [${name}] Maximum duration reached, stopping...`);
        stopStreamByName(name, chatId, sock);
        sendMessage(chatId, `⏳ *مدة البث "${name}" انتهت*\n(${formatDuration(MAX_DURATION)}) وتم إيقافه.`, sock);
        return;
      }

      if (streamType === "normal" && shouldSwitchToEncoding && !useEncoding) {
        console.log(`🔄 [${name}] Restarting with ENCODING mode...`);
        useEncoding = true;
        setTimeout(() => {
          if (runningStreams.has(name)) {
            const newProcess = runFFmpegProcess(0);
            runningStreams.set(name, {
              ...stream,
              process: newProcess,
              useEncoding: true,
              startTime: stream.startTime
            });
          }
        }, 2000);
        return;
      }

      if (runningStreams.has(name)) {
        const newProcess = runFFmpegProcess(retryCount + 1);
        runningStreams.set(name, {
          ...stream,
          process: newProcess,
          useEncoding,
          startTime: stream.startTime
        });
      }
    });

    ffmpeg.on("error", (err) => {
      console.error(`[${name}] FFmpeg error:`, err);
      stats.errors++;
    });

    return ffmpeg;
  }

  const proc = runFFmpegProcess();
  runningStreams.set(name, {
    owner: chatId,
    process: proc,
    sourceUrl,
    streamKey,
    startTime: Date.now(),
    useEncoding: streamType === "encode-with-watermark",
    type: streamType,
    drawText: drawText,
    platform: platform,
    retryCount: 0
  });

  const sourceType = detectSourceType(sourceUrl);
  let message = `✅ *تم تشغيل البث باسم:* ${name}\n\n`;
  message += `🌐 *المنصة:* ${platform === "instagram" ? "Instagram" : "Facebook"}\n`;
  message += `🔑 *Stream Key:* \`${streamKey.substring(0, 10)}...\`\n`;
  message += `📡 *المصدر:* ${sourceUrl.length > 10 ? sourceUrl.substring(0,10) + '...' : sourceUrl}\n`;
  message += `📊 *النوع:* ${sourceType}\n`;

  if (streamType === "encode-with-watermark") {
    message += `🎬 *وضع:* ENCODE مع Watermark\n`;
    if (drawText) message += `📝 *Watermark:* ${drawText.substring(0, 30)}...\n`;
  } else {
    message += `🔧 *وضع:* ${streamType === "normal" ? "COPY (تلقائي)" : "ENCODE"}\n`;
  }

  if (platform === "instagram") {
    message += `🔄 *الفيديو:* تم تدويره 90°\n`;
  }

  message += `⏱ *أقصى مدة:* ${formatDuration(MAX_DURATION)}\n\n`;
  message += `لإيقاف: */stop ${name}*`;

  await sendMessage(chatId, message, sock);
}

async function stopStreamByName(name, chatId, sock) {
  cleanupExpiredStreams();

  if (!runningStreams.has(name)) {
    await sendMessage(chatId, `⚠️ *لا يوجد بث جارٍ بالاسم:* ${name}`, sock);
    return;
  }

  const stream = runningStreams.get(name);

  if (stream.owner !== chatId) {
    return;
  }

  try {
    if (stream.process) {
      stream.process.kill("SIGKILL");
    }
  } catch (e) {
    console.error(`خطأ في إيقاف ${name}:`, e);
    stats.errors++;
  }

  const duration = Date.now() - stream.startTime;
  
  stats.streamsStopped++;
  stats.totalStreamTime += duration;

  runningStreams.delete(name);

  let message = `🛑 *تم إيقاف البث:* ${name}\n\n`;
  message += `🌐 *المنصة:* ${stream.platform === "instagram" ? "Instagram" : "Facebook"}\n`;
  message += `⏱ *المدة الكلية:* ${formatDuration(duration)}\n`;

  if (stream.type === "encode-with-watermark" && stream.drawText) {
    message += `📝 *Watermark كان:* ${stream.drawText.substring(0, 30)}...`;
  } else if (stream.type === "normal") {
    message += `🔧 *وضع:* ${stream.useEncoding ? 'ENCODING' : 'COPY'}`;
  } else {
    message += `🎬 *نوع:* ${stream.type}`;
  }

  await sendMessage(chatId, message, sock);
}

// ===== WHATSAPP MESSAGE HANDLER =====
async function sendMessage(chatId, text, sock) {
  try {
    await sock.sendMessage(chatId, { text });
  } catch (error) {
    console.error('Error sending message:', error);
  }
}

async function handleCommand(message, sock) {
  const text = message.message?.conversation || message.message?.extendedTextMessage?.text || '';
  const chatId = message.key.remoteJid;
  
  console.log(`🔧 handleCommand called`);
  console.log(`   ChatID: ${chatId}`);
  console.log(`   Text: "${text}"`);
  
  if (!text.trim()) {
    console.log(`⚠️ Empty message, skipping`);
    return;
  }

  console.log(`📩 Processing command from ${chatId}: ${text}`);

  // ===== PING =====
  if (text.startsWith('/ping')) {
    await sendMessage(chatId, 
      `🏓 *Pong!*\n\n` +
      `✅ البوت يعمل بشكل صحيح\n` +
      `🆔 Chat ID: \`${chatId}\`\n` +
      `👤 Admin: ${isAdminUser(chatId) ? 'نعم ✅' : 'لا ❌'}\n` +
      `🔐 Allowed: ${isAllowedChat(chatId) ? 'نعم ✅' : 'لا ❌'}`, sock);
    return;
  }

  // ===== ALLOW =====
  if (text.startsWith('/allow') && isAdminUser(chatId)) {
    const input = text.substring(6).trim();
    
    if (!input) {
      await sendMessage(chatId, `❌ *صيغة /allow غير صحيحة*\n\n*/allow CHAT_ID*`, sock);
      return;
    }

    const added = addAllowedChat(input);
    const chats = loadAllowedChats();
    await sendMessage(chatId,
      added ? `✅ *تم إضافة* \`${input}\`\n📊 الآن: ${chats.length}` : `⚠️ *موجود بالفعل*`,
      sock);
    return;
  }

  // ===== UNALLOW =====
  if (text.startsWith('/unallow') && isAdminUser(chatId)) {
    const input = text.substring(8).trim();
    const removed = removeAllowedChat(input);
    const chats = loadAllowedChats();
    await sendMessage(chatId,
      removed ? `✅ *تم حذف* \`${input}\`\n📊 الآن: ${chats.length}` : `⚠️ *غير موجود*`,
      sock);
    return;
  }

  // ===== CHATID =====
  if (text.startsWith('/chatid')) {
    await sendMessage(chatId,
      `🆔 *Chat ID:* \`${chatId}\`\n` +
      `👤 Admin: ${isAdminUser(chatId) ? '✅' : '❌'}\n` +
      `🔐 Allowed: ${isAllowedChat(chatId) ? '✅' : '❌'}`, sock);
    return;
  }

  // ===== LISTCHATS =====
  if (text.startsWith('/listchats') && isAdminUser(chatId)) {
    const chats = loadAllowedChats();
    if (chats.length === 0) {
      await sendMessage(chatId, "📭 *لا توجد Chat IDs*", sock);
      return;
    }

    let msg = `📋 *Chat IDs (${chats.length}):*\n\n`;
    chats.forEach((c, i) => msg += `${i + 1}. \`${c}\`\n`);
    await sendMessage(chatId, msg, sock);
    return;
  }

  // Check permission
  if (!isAdminUser(chatId) && !isAllowedChat(chatId)) {
    return;
  }

  // ===== HELP =====
  if (text.startsWith('/help')) {
    let help = `🎥 *أوامر البوت*\n\n` +
      `/ping - Test bot\n` +
      `/chatid - Your Chat ID\n` +
      `/add NAME | URL | KEY - Add stream\n` +
      `/start NAME - Start stream\n` +
      `/stop NAME - Stop stream\n` +
      `/list - Your streams\n` +
      `/stats - Statistics\n` +
      `/clean - Cleanup\n` +
      `/encode NAME | URL | KEY | TEXT - With watermark\n` +
      `/ig NAME | URL | KEY - Instagram`;
    
    if (isAdminUser(chatId)) {
      help += `\n\n👨‍💼 Admin:\n/allow ID\n/unallow ID\n/listchats`;
    }
    
    await sendMessage(chatId, help, sock);
    return;
  }

  // ===== ADD =====
  if (text.startsWith('/add')) {
    const parts = text.substring(4).trim().split("|").map(s => s.trim());
    
    if (parts.length !== 3) {
      await sendMessage(chatId, `❌ */add NAME | URL | KEY*`, sock);
      return;
    }
    
    const [name, url, key] = parts;
    
    if (!isValidUrl(url)) {
      await sendMessage(chatId, `❌ *رابط خاطئ*`, sock);
      return;
    }
    
    registeredStreams.set(name, { owner: chatId, sourceUrl: url, streamKey: key, type: "normal", platform: "facebook" });
    await sendMessage(chatId, `✅ *Stream:* ${name}\n📡 ${url.substring(0, 30)}...\n\n*/start ${name}*`, sock);
    return;
  }

  // ===== START =====
  if (text.startsWith('/start')) {
    const name = text.substring(6).trim();
    if (name) {
      await startStreamByName(name, chatId, sock);
    }
    return;
  }

  // ===== STOP =====
  if (text.startsWith('/stop')) {
    const name = text.substring(5).trim();
    await stopStreamByName(name, chatId, sock);
    return;
  }

  // ===== LIST =====
  if (text.startsWith('/list')) {
    cleanupExpiredStreams();
    const running = Array.from(runningStreams.entries()).filter(([_, s]) => s.owner === chatId);
    const registered = Array.from(registeredStreams.entries()).filter(([_, s]) => s.owner === chatId);

    let msg = '';
    if (running.length > 0) {
      msg += `▶️ *Streams الجارية (${running.length}):*\n`;
      running.forEach(([n, s]) => {
        const elapsed = Date.now() - s.startTime;
        const timeLeft = formatDuration(Math.max(0, MAX_DURATION - elapsed));
        msg += `• ${n} (متبقي: ${timeLeft})\n`;
      });
      msg += '\n';
    }
    
    if (registered.length > 0) {
      msg += `📝 *Streams المسجلة (${registered.length}):*\n`;
      registered.forEach(([n]) => msg += `• ${n}\n`);
    }
    
    msg = msg || `📭 *لا توجد streams*`;
    await sendMessage(chatId, msg, sock);
    return;
  }

  // ===== STATS =====
  if (text.startsWith('/stats')) {
    const uptime = Date.now() - stats.botStartTime;
    const userRunning = Array.from(runningStreams.values()).filter(s => s.owner === chatId).length;
    const userRegistered = Array.from(registeredStreams.values()).filter(s => s.owner === chatId).length;
    
    await sendMessage(chatId,
      `📈 *Statistics*\n\n` +
      `⏱ *Uptime:* ${formatDuration(uptime)}\n` +
      `🚀 *Started:* ${stats.streamsStarted}\n` +
      `🛑 *Stopped:* ${stats.streamsStopped}\n` +
      `⚠️ *Errors:* ${stats.errors}\n\n` +
      `📊 *Your Stats:*\n` +
      `▶ Running: ${userRunning}\n` +
      `📝 Registered: ${userRegistered}`, sock);
    return;
  }

  // ===== CLEAN =====
  if (text.startsWith('/clean')) {
    const { cleanedRunning } = cleanupExpiredStreams();
    await sendMessage(chatId, `🧹 *Cleaned:* ${cleanedRunning} streams`, sock);
    return;
  }

  // ===== ENCODE =====
  if (text.startsWith('/encode')) {
    const parts = text.substring(7).trim().split("|").map(s => s.trim());
    if (parts.length < 4) {
      await sendMessage(chatId, `❌ */encode NAME | URL | KEY | TEXT*`, sock);
      return;
    }
    const [name, url, key, txt] = parts;
    if (!isValidUrl(url)) {
      await sendMessage(chatId, `❌ *رابط خاطئ*`, sock);
      return;
    }
    registeredStreams.set(name, {
      owner: chatId, sourceUrl: url, streamKey: key, type: "encode-with-watermark",
      drawText: txt, platform: "facebook"
    });
    await sendMessage(chatId, `🎬 *ENCODE Started...*``, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }

  // ===== INSTAGRAM =====
  if (text.startsWith('/ig')) {
    const parts = text.substring(3).trim().split("|").map(s => s.trim());
    if (parts.length !== 3) {
      await sendMessage(chatId, `❌ */ig NAME | URL | KEY*`, sock);
      return;
    }
    const [name, url, key] = parts;
    if (!isValidUrl(url)) {
      await sendMessage(chatId, `❌ *رابط خاطئ*`, sock);
      return;
    }
    registeredStreams.set(name, {
      owner: chatId, sourceUrl: url, streamKey: key,
      type: "encode-with-watermark", platform: "instagram"
    });
    await sendMessage(chatId, `📱 *Instagram Stream Starting...*`, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }
}

// ===== MAIN BOT =====
async function startBot() {
  const hasSession = sessionExists();
  console.log(hasSession ? `📂 Session exists` : `🆕 No session - QR will appear`);

  try {
    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const { version } = await fetchLatestBaileysVersion();

    const bot = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }).child({ level: 'fatal' })),
      },
      printQRInTerminal: true,
      logger: pino({ level: 'fatal' }).child({ level: 'fatal' }),
      browser: Browsers.windows('Chrome'),
      markOnlineOnConnect: false,
      generateHighQualityLinkPreview: false,
      defaultQueryTimeoutMs: 60000,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 30000,
    });

    bot.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;
      
      if (qr) console.log(`\n📱 Scan QR Code\n`);
      
      if (connection === 'open') {
        console.log(`\n✅ Connected!\n🤖 Bot Ready!\n⚙️ Max: ${formatDuration(MAX_DURATION)}\n`);
        const chats = loadAllowedChats();
        console.log(`📊 Allowed Chats: ${chats.length}\n✅ Ready!\n`);
      }

      if (connection === 'close') {
        const shouldReconnect = (lastDisconnect?.error)?.statusCode !== DisconnectReason.loggedOut;
        if ((lastDisconnect?.error)?.statusCode === DisconnectReason.loggedOut) {
          console.log('❌ Logged out');
          clearSession();
        } else if (shouldReconnect) {
          await delay(3000);
          startBot();
        }
      }
    });

    bot.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        console.log('💾 Session saved');
      } catch (e) {
        console.error('❌ Save error:', e);
      }
    });

    bot.ev.on('messages.upsert', async ({ messages }) => {
      for (const m of messages) {
        if (!m.message || m.key.fromMe) continue;
        const from = m.key.remoteJid;
        if (isAdminUser(from) || isAllowedChat(from)) {
          await handleCommand(m, bot);
        }
      }
    });

    setInterval(cleanupExpiredStreams, 60000);

  } catch (error) {
    console.error('❌ Error:', error);
    clearSession();
    await delay(5000);
    startBot();
  }
}

console.log('🚀 Starting Streaming Bot (Baileys v7.x.x - QR Code)\n');
startBot();

process.on('SIGINT', () => {
  console.log('\n🛑 Stopping...');
  for (const [name, stream] of runningStreams.entries()) {
    try {
      if (stream.process) stream.process.kill("SIGKILL");
    } catch (e) {}
  }
  console.log('✅ Stopped');
  process.exit(0);
});
