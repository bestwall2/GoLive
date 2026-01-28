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
} from '@whiskeysockets/baileys';
import pn from 'awesome-phonenumber';
import { spawn } from 'child_process';

// ===== CONFIG =====
const PHONE_NUMBER = '212629996310'; // Replace with your number
const SESSION_DIR = `./session`;
const MAX_DURATION = 4 * 60 * 60 * 1000; // 4 hours default
const MAX_RETRIES = 30000;

// Admins who can manage allowed chat IDs
const ADMIN_NUMBERS = [
  '269835950931970@lid',
  '115371696771153@lid',
];

// File to store allowed chat IDs
const ALLOWED_CHATS_FILE = path.join(SESSION_DIR, 'allowed_chats.json');

// Ensure session folder exists
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

// Check if session exists
function sessionExists() {
  const credsPath = path.join(SESSION_DIR, 'creds.json');
  return fs.existsSync(credsPath);
}

// Remove session folder completely
function removeSessionFolder() {
  if (!fs.existsSync(SESSION_DIR)) return;
  
  console.log('🧹 Removing session folder...');
  try {
    const files = fs.readdirSync(SESSION_DIR);
    for (const file of files) {
      fs.unlinkSync(path.join(SESSION_DIR, file));
    }
    fs.rmdirSync(SESSION_DIR);
    console.log('✅ Session folder removed');
  } catch (error) {
    console.error('❌ Error removing session:', error);
  }
}

// Clear session but keep folder structure
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
// Structure: { "streamName": { owner: "chatId", sourceUrl, streamKey, type, platform, process, startTime, useEncoding, retryCount } }
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
  // Check if stream is registered
  if (!registeredStreams.has(name)) {
    await sendMessage(chatId, `❌ *لا يوجد Stream مسجّل بالاسم:* ${name}`, sock);
    return;
  }

  const streamData = registeredStreams.get(name);

  // Check ownership - only the owner can start
  if (streamData.owner !== chatId) {
    // Silently ignore - per user request
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

      // Retry
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

  // Check ownership - only the owner can stop
  if (stream.owner !== chatId) {
    // Silently ignore - per user request
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
  
  if (!text.trim()) return;

  console.log(`📩 Received from ${chatId}: ${text}`);

  // ===== ADMIN COMMANDS =====
  // /allow command
  if ((text.startsWith('/allow') || text.startsWith('!allow') || text.startsWith('.allow')) && isAdminUser(chatId)) {
    const input = text.substring(6).trim();
    
    if (!input) {
      await sendMessage(chatId, 
        `❌ *صيغة /allow غير صحيحة*\n\n` +
        `📝 *الصيغة الصحيحة:*\n` +
        `*/allow CHAT_ID*\n\n` +
        `*مثال:*\n` +
        `*/allow 269835950931970@lid*\n` +
        `*/allow 120363406529443583@g.us*`, sock);
      return;
    }

    const chatIdToAllow = input;
    const added = addAllowedChat(chatIdToAllow);

    if (added) {
      const allowedChats = loadAllowedChats();
      await sendMessage(chatId,
        `✅ *تم إضافة Chat ID للقائمة المسموحة*\n\n` +
        `🆔 *Chat ID:* \`${chatIdToAllow}\`\n` +
        `📊 *عدد Chat IDs المسموحة الآن:* ${allowedChats.length}`, sock);
    } else {
      await sendMessage(chatId,
        `⚠️ *هذا Chat ID موجود بالفعل في القائمة المسموحة*\n\n` +
        `🆔 *Chat ID:* \`${chatIdToAllow}\``, sock);
    }
    return;
  }

  // /unallow command
  if ((text.startsWith('/unallow') || text.startsWith('!unallow') || text.startsWith('.unallow')) && isAdminUser(chatId)) {
    const input = text.substring(8).trim();
    
    if (!input) {
      await sendMessage(chatId, 
        `❌ *صيغة /unallow غير صحيحة*\n\n` +
        `📝 *الصيغة الصحيحة:*\n` +
        `*/unallow CHAT_ID*\n\n` +
        `*مثال:*\n` +
        `*/unallow 269835950931970@lid*\n` +
        `*/unallow 120363406529443583@g.us*`, sock);
      return;
    }

    const chatIdToRemove = input;
    const removed = removeAllowedChat(chatIdToRemove);

    if (removed) {
      const allowedChats = loadAllowedChats();
      await sendMessage(chatId,
        `✅ *تم حذف Chat ID من القائمة المسموحة*\n\n` +
        `🆔 *Chat ID:* \`${chatIdToRemove}\`\n` +
        `📊 *عدد Chat IDs المسموحة الآن:* ${allowedChats.length}`, sock);
    } else {
      await sendMessage(chatId,
        `⚠️ *هذا Chat ID غير موجود في القائمة المسموحة*\n\n` +
        `🆔 *Chat ID:* \`${chatIdToRemove}\``, sock);
    }
    return;
  }

  // /chatid command - shows current chat ID
  if (text.startsWith('/chatid') || text.startsWith('!chatid') || text.startsWith('.chatid')) {
    const isAdmin = isAdminUser(chatId);
    const adminStatus = isAdmin ? "✅ *أنت Admin*" : "❌ *أنت لست Admin*";
    const isAllowed = isAllowedChat(chatId);
    const allowedStatus = isAllowed ? "✅ *مسموح*" : "❌ *غير مسموح*";

    await sendMessage(chatId,
      `🆔 *معلومات Chat ID الخاص بك:*\n\n` +
      `📍 *Chat ID:* \`${chatId}\`\n` +
      `👤 ${adminStatus}\n` +
      `🔐 ${allowedStatus}\n\n` +
      `📋 *الأوامر المتاحة للـ Admin فقط:*\n` +
      `• */allow CHAT_ID* - إضافة Chat ID\n` +
      `• */unallow CHAT_ID* - حذف Chat ID\n` +
      `• */listchats* - عرض كل Chat IDs المسموحة`, sock);
    return;
  }

  // /listchats command - shows all allowed chats (admin only)
  if ((text.startsWith('/listchats') || text.startsWith('!listchats') || text.startsWith('.listchats')) && isAdminUser(chatId)) {
    const allowedChats = loadAllowedChats();

    if (allowedChats.length === 0) {
      await sendMessage(chatId, "📭 *لا توجد Chat IDs مسموحة*", sock);
      return;
    }

    let message = `📋 *قائمة Chat IDs المسموحة:*\n\n`;
    
    for (let i = 0; i < allowedChats.length; i++) {
      const chatIdItem = allowedChats[i];
      const isGroup = chatIdItem.includes('@g.us');
      const icon = isGroup ? "👥" : "👤";
      message += `${i + 1}. ${icon} \`${chatIdItem}\`\n`;
    }

    message += `\n📊 *الإجمالي:* ${allowedChats.length}`;
    
    await sendMessage(chatId, message, sock);
    return;
  }

  // Check if user is allowed before processing other commands
  // Admins bypass this check
  if (!isAdminUser(chatId) && !isAllowedChat(chatId)) {
    // Silently ignore - per user request
    return;
  }

  // Help command
  if (text.startsWith('/help') || text.startsWith('!help') || text.startsWith('.help')) {
    let helpMessage = `🎥 *أوامر بوت البث على الواتساب* 🎥\n\n`;
    
    helpMessage += `/help              → View all commands\n`;
    helpMessage += `/chatid            → Get your Chat ID\n`;
    helpMessage += `/add               → Register a stream\n`;
    helpMessage += `/start             → Start a stream\n`;
    helpMessage += `/stop              → Stop a stream\n`;
    helpMessage += `/list              → List your streams\n`;
    helpMessage += `/stats             → View statistics\n`;
    helpMessage += `/clean             → Cleanup old streams\n`;
    helpMessage += `/encode            → Start with watermark\n`;
    helpMessage += `/ig                → Start on Instagram\n`;
    
    // Add admin commands if user is admin
    if (isAdminUser(chatId)) {
      helpMessage += `\n👨‍💼 *أوامر Admin:*\n\n`;
      helpMessage += `/allow             → Add Chat ID to whitelist\n`;
      helpMessage += `/unallow           → Remove Chat ID from whitelist\n`;
      helpMessage += `/listchats         → View all allowed chats\n`;
    }
    
    await sendMessage(chatId, helpMessage, sock);
    return;
  }

  // Add command
  if (text.startsWith('/add') || text.startsWith('!add') || text.startsWith('.add')) {
    const raw = text.substring(4).trim();
    const parts = raw.split("|").map(s => s.trim());
    
    if (parts.length !== 3) {
      await sendMessage(chatId, 
        `❌ *صيغة /add غير صحيحة*\n\n` +
        `📝 *الصيغة الصحيحة:*\n` +
        `*/add NAME | SOURCE_URL | STREAM_KEY*\n\n` +
        `*مثال:*\n` +
        `*/add مباراة1 | https://example.com/stream.m3u8 | FB_123456789*`, sock);
      return;
    }
    
    const [name, sourceUrl, streamKey] = parts;
    
    if (!isValidUrl(sourceUrl)) {
      await sendMessage(chatId, "❌ *رابط المصدر غير صالح*", sock);
      return;
    }
    
    registeredStreams.set(name, { 
      owner: chatId,
      sourceUrl, 
      streamKey, 
      type: "normal", 
      platform: "facebook" 
    });
    
    await sendMessage(chatId,
      `✅ *تم تسجيل Stream بنجاح*\n\n` +
      `📍 *الاسم:* ${name}\n` +
      `🌐 *المنصة:* Facebook\n` +
      `📡 *المصدر:* ${sourceUrl.length > 20 ? sourceUrl.substring(0, 20) + '...' : sourceUrl}\n` +
      `🔑 *المفتاح:* \`${streamKey.substring(0, 10)}...\`\n\n` +
      `استخدم */start ${name}* لتشغيله`, sock);
    return;
  }

  // Start command
  if (text.startsWith('/start') || text.startsWith('!start') || text.startsWith('.start')) {
    const name = text.substring(6).trim();
    if (name) {
      await startStreamByName(name, chatId, sock);
    } else {
      await sendMessage(chatId, 
        `🎥 *مرحباً بكم في بوت البث*\n\n` +
        `استخدم */help* لرؤية جميع الأوامر\n\n` +
        `*أمثلة:*\n` +
        `- */start مباراة1*\n` +
        `- */add اسم | رابط | مفتاح*\n` +
        `- */list*`, sock);
    }
    return;
  }

  // Stop command
  if (text.startsWith('/stop') || text.startsWith('!stop') || text.startsWith('.stop')) {
    const name = text.substring(5).trim();
    await stopStreamByName(name, chatId, sock);
    return;
  }

  // List command - ONLY show user's own streams
  if (text.startsWith('/list') || text.startsWith('!list') || text.startsWith('.list')) {
    cleanupExpiredStreams();

    // Get streams owned by this user
    const userRunningStreams = Array.from(runningStreams.entries())
      .filter(([_, stream]) => stream.owner === chatId);
    
    const userRegisteredStreams = Array.from(registeredStreams.entries())
      .filter(([_, stream]) => stream.owner === chatId);

    if (userRunningStreams.length === 0 && userRegisteredStreams.length === 0) {
      await sendMessage(chatId, "📭 *ليس لديك Streams مسجلة أو جارية*", sock);
      return;
    }

    let message = "";

    if (userRunningStreams.length > 0) {
      message += "▶️ *Streams الجارية الخاصة بك:*\n\n";
      
      for (const [name, stream] of userRunningStreams) {
        const elapsed = Date.now() - stream.startTime;
        const timeLeft = Math.max(0, MAX_DURATION - elapsed);
        const sourceType = detectSourceType(stream.sourceUrl);
        
        message += `📍 *${name}*\n`;
        message += `   🌐 ${stream.platform === "instagram" ? "Instagram" : "Facebook"}\n`;
        message += `   📡 ${stream.sourceUrl.length > 30 ? stream.sourceUrl.substring(0, 30) + '...' : stream.sourceUrl}\n`;
        message += `   📊 ${sourceType}\n`;
        message += `   ⏱ تشغل منذ: ${formatDuration(elapsed)}\n`;
        message += `   ⏳ متبقي: ${formatDuration(timeLeft)}\n`;
        
        if (stream.type === "encode-with-watermark") {
          message += `   🎬 نوع: ENCODE + Watermark\n`;
        } else if (stream.type === "normal") {
          message += `   🔧 وضع: ${stream.useEncoding ? 'ENCODING' : 'COPY'}\n`;
        }
        
        if (stream.platform === "instagram") {
          message += `   🔄 الفيديو: مقلوب 90°\n`;
        }
        
        message += `   🔑 المفتاح: \`${stream.streamKey.substring(0, 8)}...\`\n\n`;
      }
    }

    if (userRegisteredStreams.length > 0) {
      message += "📝 *Streams المسجلة الخاصة بك (غير جارية):*\n\n";
      
      for (const [name, info] of userRegisteredStreams) {
        message += `📍 *${name}*\n`;
        message += `   🌐 ${info.platform === "instagram" ? "Instagram" : "Facebook"}\n`;
        message += `   📡 ${info.sourceUrl.length > 30 ? info.sourceUrl.substring(0, 30) + '...' : info.sourceUrl}\n`;
        
        if (info.type === "encode-with-watermark" && info.drawText) {
          message += `   📝 Watermark: ${info.drawText.substring(0, 20)}...\n`;
        }
        
        message += `   🔧 نوع: ${info.type || "normal"}\n`;
        message += `   🔑 المفتاح: \`${info.streamKey.substring(0, 8)}...\`\n\n`;
      }
    }

    message += `📊 *إحصائياتك:*\n`;
    message += `▶ عدد Streams الجارية: ${userRunningStreams.length}\n`;
    message += `📝 عدد Streams المسجلة: ${userRegisteredStreams.length}\n`;
    message += `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}`;

    await sendMessage(chatId, message, sock);
    return;
  }

  // Clean command
  if (text.startsWith('/clean') || text.startsWith('!clean') || text.startsWith('.clean')) {
    const beforeRunning = runningStreams.size;
    const beforeRegistered = registeredStreams.size;

    const { cleanedRunning, cleanedRegistered } = cleanupExpiredStreams();

    const userRunningCount = Array.from(runningStreams.values()).filter(s => s.owner === chatId).length;
    const userRegisteredCount = Array.from(registeredStreams.values()).filter(s => s.owner === chatId).length;

    await sendMessage(chatId,
      `🧹 *تم تنظيف الـ Streams منتهية المدة*\n\n` +
      `✅ تم تنظيف ${cleanedRunning} من الـ Streams الجارية\n` +
      `✅ تم تنظيف ${cleanedRegistered} من الـ Streams المسجلة\n\n` +
      `📊 *حالتك الحالية:*\n` +
      `▶ Streams جارية: ${userRunningCount}\n` +
      `📝 Streams مسجلة: ${userRegisteredCount}`, sock);
    return;
  }

  // Stats command
  if (text.startsWith('/stats') || text.startsWith('!stats') || text.startsWith('.stats')) {
    const now = Date.now();
    const uptime = now - stats.botStartTime;

    const userRunningCount = Array.from(runningStreams.values()).filter(s => s.owner === chatId).length;
    const userRegisteredCount = Array.from(registeredStreams.values()).filter(s => s.owner === chatId).length;
    
    let message = "📈 *إحصائيات النظام*\n\n";
    message += `⏱ *مدة تشغيل البوت:* ${formatDuration(uptime)}\n`;
    message += `🚀 *Streams بدأت:* ${stats.streamsStarted}\n`;
    message += `🛑 *Streams توقفت:* ${stats.streamsStopped}\n`;
    message += `⏳ *إجمالي وقت البث:* ${formatDuration(stats.totalStreamTime)}\n`;
    message += `⚠️ *الأخطاء:* ${stats.errors}\n\n`;
    
    message += `📊 *حالتك الخاصة:*\n`;
    message += `▶ Streams جارية: ${userRunningCount}\n`;
    message += `📝 Streams مسجلة: ${userRegisteredCount}\n\n`;
    
    message += `⚙️ *الإعدادات:*\n`;
    message += `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}\n`;
    message += `🔄 المحاولات القصوى: ${MAX_RETRIES}`;
    
    await sendMessage(chatId, message, sock);
    return;
  }

  // Encode command
  if (text.startsWith('/encode') || text.startsWith('!encode') || text.startsWith('.encode')) {
    const input = text.substring(7).trim();
    const parts = input.split("|").map(s => s.trim());

    if (parts.length < 4) {
      await sendMessage(chatId,
        `❌ *الصيغة الصحيحة:*\n\n` +
        `*/encode NAME | SOURCE_URL | STREAM_KEY | DRAW_TEXT*\n\n` +
        `*مثال:*\n` +
        `*/encode مباراة1 | https://example.com/stream.m3u8 | FB_123456789 | LIVE TV*`, sock);
      return;
    }

    const [name, sourceUrl, streamKey, drawText] = parts;

    if (!isValidUrl(sourceUrl)) {
      await sendMessage(chatId, "❌ *رابط المصدر غير صالح*", sock);
      return;
    }

    if (runningStreams.has(name) && runningStreams.get(name).owner === chatId) {
      await sendMessage(chatId,
        `⚠️ *البث "${name}" يعمل بالفعل*\n` +
        `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
      return;
    }

    cleanupExpiredStreams();

    registeredStreams.set(name, {
      owner: chatId,
      sourceUrl,
      streamKey,
      type: "encode-with-watermark",
      drawText: drawText,
      platform: "facebook"
    });

    await sendMessage(chatId, `🎬 *جار بدء البث (ENCODE + WATERMARK)...*`, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }

  // Instagram command
  if (text.startsWith('/ig') || text.startsWith('!ig') || text.startsWith('.ig')) {
    const input = text.substring(3).trim();
    const parts = input.split("|").map(s => s.trim());

    if (parts.length !== 3) {
      await sendMessage(chatId,
        `❌ *الصيغة الصحيحة:*\n\n` +
        `*/ig NAME | SOURCE_URL | STREAM_KEY*\n\n` +
        `*مثال:*\n` +
        `*/ig مباراة1 | https://example.com/stream.m3u8 | IG_123456789*\n\n` +
        `📱 *ملاحظات Instagram:*\n` +
        `• سيقوم تلقائياً بتدوير الفيديو 90°\n` +
        `• يستخدم ENCODING مع Watermark\n` +
        `• المدة القصوى: ${formatDuration(MAX_DURATION)}`, sock);
      return;
    }

    const [name, sourceUrl, streamKey] = parts;

    if (!isValidUrl(sourceUrl)) {
      await sendMessage(chatId, "❌ *رابط المصدر غير صالح*", sock);
      return;
    }

    if (runningStreams.has(name) && runningStreams.get(name).owner === chatId) {
      await sendMessage(chatId,
        `⚠️ *البث "${name}" يعمل بالفعل*\n` +
        `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
      return;
    }

    cleanupExpiredStreams();

    registeredStreams.set(name, {
      owner: chatId,
      sourceUrl,
      streamKey,
      type: "encode-with-watermark",
      platform: "instagram"
    });

    await sendMessage(chatId,
      `📱 *جار بدء البث على Instagram...*\n\n` +
      `⚙️ *الإعدادات:*\n` +
      `• تدوير الفيديو 90° تلقائياً\n` +
      `• ENCODING مع Watermark\n` +
      `• المدة القصوى: ${formatDuration(MAX_DURATION)}\n` +
      `• المحاولات القصوى: ${MAX_RETRIES}`, sock);

    await startStreamByName(name, chatId, sock);
    return;
  }

  // Quick add+start via "NAME | SOURCE_URL | STREAM_KEY"
  const parts = text.split("|").map(x => x.trim());

  if (parts.length === 3) {
    const [name, sourceUrl, streamKey] = parts;
    
    if (!isValidUrl(sourceUrl)) {
      await sendMessage(chatId, "❌ *رابط المصدر غير صالح*", sock);
      return;
    }
    
    registeredStreams.set(name, { 
      owner: chatId,
      sourceUrl, 
      streamKey, 
      type: "normal", 
      platform: "facebook" 
    });
    await sendMessage(chatId,
      `✅ *تم التسجيل والتشغيل*\n\n` +
      `📍 *الاسم:* ${name}\n` +
      `🌐 *المنصة:* Facebook\n` +
      `⏳ *جار التشغيل...*`, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }

  if (parts.length === 2) {
    const [sourceUrl, streamKey] = parts;
    
    if (!isValidUrl(sourceUrl)) {
      await sendMessage(chatId, "❌ *رابط المصدر غير صالح*", sock);
      return;
    }
    
    const name = `S-${Date.now()}`;
    registeredStreams.set(name, { 
      owner: chatId,
      sourceUrl, 
      streamKey, 
      type: "normal", 
      platform: "facebook" 
    });
    await sendMessage(chatId,
      `✅ *تم تشغيل البث*\n\n` +
      `📍 *الاسم:* ${name}\n` +
      `🌐 *المنصة:* Facebook\n` +
      `📡 *المصدر:* ${sourceUrl.length > 30 ? sourceUrl.substring(0, 30) + '...' : sourceUrl}`, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }
}

// ===== MAIN BOT =====
async function startBot() {
  // Check session status
  const hasExistingSession = sessionExists();
  
  if (hasExistingSession) {
    console.log(`📂 جلسة موجودة في: ${SESSION_DIR}`);
    console.log(`🔄 محاولة الاتصال باستخدام الجلسة المحفوظة...`);
  } else {
    console.log(`🆕 لا توجد جلسة سابقة، جاري إنشاء جلسة جديدة...`);
  }

  try {
    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);

    const phone = pn('+' + PHONE_NUMBER);
    if (!phone.isValid()) {
      console.error('❌ رقم الهاتف غير صالح');
      return process.exit(1);
    }

    const { version } = await fetchLatestBaileysVersion();

    const KnightBot = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(
          state.keys,
          pino({ level: 'fatal' }).child({ level: 'fatal' })
        ),
      },
      printQRInTerminal: !hasExistingSession,
      logger: pino({ level: 'fatal' }).child({ level: 'fatal' }),
      browser: Browsers.windows('Chrome'),
      markOnlineOnConnect: false,
      generateHighQualityLinkPreview: false,
      defaultQueryTimeoutMs: 60000,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 30000,
      retryRequestDelayMs: 250,
      maxRetries: 5,
    });

    // ===== CONNECTION EVENTS =====
    KnightBot.ev.on('connection.update', async ({ connection, lastDisconnect }) => {
      if (connection === 'open') {
        console.log(`✅ اتصل بنجاح باسم ${PHONE_NUMBER}`);
        console.log('🤖 بوت البث جاهز الآن!');
        console.log(`⚙️ الإعدادات: المدة القصوى=${formatDuration(MAX_DURATION)}, المحاولات القصوى=${MAX_RETRIES}`);
        
        const allowedChats = loadAllowedChats();
        console.log(`📊 عدد Chat IDs المسموحة: ${allowedChats.length}`);
      }

      if (connection === 'close') {
        const code = lastDisconnect?.error?.output?.statusCode;
        console.log(`🔌 انقطع الاتصال، الكود: ${code}`);
        
        if (code === 401) {
          console.log('❌ تم تسجيل الخروج (401). جاري مسح الجلسة...');
          clearSession();
          console.log('🔄 إعادة تشغيل البوت...');
          await delay(5000);
          startBot();
        } else if (code === 403) {
          console.log('🚫 تم حظر الجهاز (403). جاري مسح الجلسة...');
          clearSession();
          console.log('🔄 إعادة تشغيل البوت...');
          await delay(5000);
          startBot();
        } else {
          console.log('🔁 الاتصال انقطع بشكل غير متوقع. جاري إعادة الاتصال...');
          await delay(3000);
          startBot();
        }
      }
      
      if (connection === 'connecting') {
        console.log('🔄 جاري الاتصال...');
      }
    });

    // ===== CREDENTIALS UPDATE =====
    KnightBot.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        console.log('💾 تم حفظ بيانات الجلسة');
      } catch (error) {
        console.error('❌ فشل في حفظ بيانات الجلسة:', error);
      }
    });

    // ===== MESSAGE HANDLER =====
    KnightBot.ev.on('messages.upsert', async ({ messages }) => {
      for (const m of messages) {
        if (!m.message || m.key.fromMe) continue;
    
        const from = m.key.remoteJid;
        console.log(`📩 Message from: ${from}`);

        // Always allow admin commands
        if (isAdminUser(from)) {
          await handleCommand(m, KnightBot);
          continue;
        }

        // Check if user/group is allowed
        if (!isAllowedChat(from)) {
          console.log(`❌ Blocked message from: ${from}`);
          continue;
        }

        // ✅ allowed users/groups reach the bot
        await handleCommand(m, KnightBot);
      }
    });

    // ===== PAIRING CODE IF NOT REGISTERED =====
    if (!KnightBot.authState.creds.registered && !hasExistingSession) {
      console.log('📱 جاري طلب رمز الاقتران...');
      await delay(3000);
      try {
        let code = await KnightBot.requestPairingCode(PHONE_NUMBER);
        code = code?.match(/.{1,4}/g)?.join('-') || code;
        console.log(`📌 رمز الاقتران لـ ${PHONE_NUMBER}: ${code}`);
        console.log('💡 يمكنك أيضًا مسح QR code من التطبيق');
      } catch (err) {
        console.error('❌ فشل في طلب رمز الاقتران:', err);
      }
    } else if (hasExistingSession) {
      console.log('🔐 استخدام الجلسة المحفوظة...');
    }

    // ===== AUTO CLEANUP =====
    setInterval(() => {
      cleanupExpiredStreams();
    }, 60 * 1000);

    setInterval(() => {
      const runningBefore = runningStreams.size;
      const registeredBefore = registeredStreams.size;

      const { cleanedRunning, cleanedRegistered } = cleanupExpiredStreams();

      const runningAfter = runningStreams.size;
      const registeredAfter = registeredStreams.size;

      if (runningBefore !== runningAfter || registeredBefore !== registeredAfter) {
        console.log(`🔄 تنظيف تلقائي: الجارية: ${runningBefore} → ${runningAfter}, المسجلة: ${registeredBefore} → ${registeredAfter}`);
      }
    }, 10 * 60 * 1000);

  } catch (error) {
    console.error('❌ خطأ في بدء البوت:', error);
    
    if (error.message.includes('session') || error.message.includes('auth')) {
      console.log('🔄 جلسة معطلة، جاري إنشاء جلسة جديدة...');
      clearSession();
      await delay(5000);
      startBot();
    }
  }
}

// ===== START BOT =====
console.log('🚀 بدء تشغيل بوت البث على WhatsApp...');
console.log(`📱 الرقم: ${PHONE_NUMBER}`);
console.log(`📂 مجلد الجلسة: ${SESSION_DIR}`);
console.log(`👨‍💼 Admins: ${ADMIN_NUMBERS.join(', ')}`);

startBot().catch(err => {
  console.error('❌ تحطم البوت:', err);
  console.log('🔄 إعادة تشغيل خلال 10 ثواني...');
  setTimeout(() => {
    startBot();
  }, 10000);
});

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log("\n🛑 إيقاف البوت بشكل آمن...");
  
  console.log('🛑 إيقاف جميع البثوث الجارية...');
  for (const [name, stream] of runningStreams.entries()) {
    try {
      console.log(`   إيقاف ${name}...`);
      if (stream.process) {
        stream.process.kill("SIGKILL");
      }
    } catch (e) {
      console.error(`خطأ في إيقاف ${name}:`, e);
    }
  }
  
  console.log("✅ تم إيقاف جميع البثوث. مع السلامة!");
  console.log("💾 تم حفظ الجلسة للمرة القادمة");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log("\n🔚 تلقي إشارة الإنهاء...");
  
  for (const [name, stream] of runningStreams.entries()) {
    try {
      if (stream.process) {
        stream.process.kill("SIGKILL");
      }
    } catch (e) {
      console.error(`خطأ في إيقاف ${name}:`, e);
    }
  }
  
  console.log("✅ إنهاء نظيف");
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  console.error('❌ خطأ غير معالج:', error);
  stats.errors++;
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ رفض غير معالج:', reason);
  stats.errors++;
});
