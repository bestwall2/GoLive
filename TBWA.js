import fs from 'fs';
import pino from 'pino';
import {
  makeWASocket,
  useMultiFileAuthState,
  delay,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  Browsers,
  proto
} from '@whiskeysockets/baileys';
import pn from 'awesome-phonenumber';
import { spawn } from 'child_process';

// ===== CONFIG =====
const PHONE_NUMBER = '212600000000'; // Replace with your number
const SESSION_DIR = `./session-${PHONE_NUMBER}`;
const MAX_DURATION = 4 * 60 * 60 * 1000; // 4 hours default
const MAX_RETRIES = 30000;

// Ensure session folder exists
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

// Remove old session if needed
function removeSession(folder) {
  if (!fs.existsSync(folder)) return;
  fs.rmSync(folder, { recursive: true, force: true });
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
    if (sock) await sendMessage(chatId, `❌ *لا يوجد Stream مسجّل بالاسم:* ${name}`, sock);
    return;
  }

  cleanupExpiredStreams();

  if (runningStreams.has(name)) {
    const stream = runningStreams.get(name);
    const elapsed = Date.now() - stream.startTime;
    const timeLeft = Math.max(0, MAX_DURATION - elapsed);
    
    if (sock) {
      await sendMessage(chatId, 
        `⚠️ *الـ Stream "${name}" يعمل بالفعل*\n` +
        `⏰ *منذ:* ${formatDuration(elapsed)}\n` +
        `⏳ *متبقي:* ${formatDuration(timeLeft)}\n` +
        `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
    }
    return;
  }

  const { sourceUrl, streamKey, type, drawText, platform = "facebook" } = registeredStreams.get(name);
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
        if (sock) {
          sendMessage(chatId, `⏳ *مدة البث "${name}" انتهت*\n(${formatDuration(MAX_DURATION)}) وتم إيقافه.`, sock);
        }
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

      if (elapsed < MAX_DURATION) {
        const nextRetry = Math.min(3000 * (retryCount + 1), 10000);
        console.log(`[${name}] إعادة تشغيل FFmpeg تلقائيًا بعد ${nextRetry}ms (محاولة ${retryCount + 1}/${MAX_RETRIES})...`);
        
        setTimeout(() => {
          if (runningStreams.has(name)) {
            const newProcess = runFFmpegProcess(retryCount + 1);
            runningStreams.set(name, {
              ...stream,
              process: newProcess,
              useEncoding,
              startTime: stream.startTime
            });
          }
        }, nextRetry);
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

  if (sock) {
    const sourceType = detectSourceType(sourceUrl);
    let message = `✅ *تم تشغيل البث باسم:* ${name}\n\n`;
    message += `🌐 *المنصة:* ${platform === "instagram" ? "Instagram" : "Facebook"}\n`;
    message += `🔑 *Stream Key:* \`${streamKey.substring(0, 10)}...\`\n`;
    message += `📡 *المصدر:* ${sourceUrl.length > 40 ? sourceUrl.substring(0, 40) + '...' : sourceUrl}\n`;
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
}

async function stopStreamByName(name, chatId, sock) {
  cleanupExpiredStreams();

  const stream = runningStreams.get(name);
  if (!stream) {
    if (sock) await sendMessage(chatId, `⚠️ *لا يوجد بث جارٍ بالاسم:* ${name}`, sock);
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

  if (sock) {
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

  // Help command
  if (text.startsWith('/help') || text.startsWith('!help') || text.startsWith('.help')) {
    const helpMessage = `🎥 *أوامر بوت البث على الواتساب* 🎥\n\n` +
      `📝 *التسجيل:*\n` +
      `*/add NAME | SOURCE_URL | STREAM_KEY*  - سجّل Stream (لا يبدأ تلقائياً)\n\n` +
      `▶️ *التشغيل والإيقاف:*\n` +
      `*/start NAME*  - شغّل Stream مسجّل بالاسم\n` +
      `*/stop NAME*   - أوقف Stream جاري بالاسم\n\n` +
      `📋 *المعاينة والإدارة:*\n` +
      `*/list*        - عرض المسجّلة والجارية\n` +
      `*/clean*       - تنظيف الـ Streams منتهية المدة\n` +
      `*/stats*       - إحصائيات النظام\n\n` +
      `🎬 *أنواع البث:*\n` +
      `*/encode NAME | SOURCE_URL | STREAM_KEY | DRAW_TEXT* - بث مع Watermark\n` +
      `*/ig NAME | SOURCE_URL | STREAM_KEY* - بث على Instagram مع تدوير الفيديو\n\n` +
      `⚡ *الطريقة السريعة:*\n` +
      `يمكنك إرسال مباشرة:\n` +
      `*NAME | SOURCE_URL | STREAM_KEY*\n` +
      `وسيتم تسجيله وتشغيله مباشرة على Facebook.\n\n` +
      `⚙️ *الإعدادات:*\n` +
      `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}\n` +
      `🔄 المحاولات القصوى: ${MAX_RETRIES}`;
    
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
    
    registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
    
    await sendMessage(chatId,
      `✅ *تم تسجيل Stream بنجاح*\n\n` +
      `📍 *الاسم:* ${name}\n` +
      `🌐 *المنصة:* Facebook\n` +
      `📡 *المصدر:* ${sourceUrl.length > 40 ? sourceUrl.substring(0, 40) + '...' : sourceUrl}\n` +
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

  // List command
  if (text.startsWith('/list') || text.startsWith('!list') || text.startsWith('.list')) {
    cleanupExpiredStreams();

    if (runningStreams.size === 0 && registeredStreams.size === 0) {
      await sendMessage(chatId, "📭 *لا توجد Streams مسجلة أو جارية*", sock);
      return;
    }

    let message = "";

    if (runningStreams.size > 0) {
      message += "▶️ *Streams الجارية:*\n\n";
      
      for (const [name, stream] of runningStreams.entries()) {
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

    const inactiveStreams = Array.from(registeredStreams.entries())
      .filter(([name]) => !runningStreams.has(name));

    if (inactiveStreams.length > 0) {
      message += "📝 *Streams المسجلة (غير جارية):*\n\n";
      
      for (const [name, info] of inactiveStreams) {
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

    message += `📊 *الإحصائيات:*\n`;
    message += `▶ عدد Streams الجارية: ${runningStreams.size}\n`;
    message += `📝 عدد Streams المسجلة: ${registeredStreams.size}\n`;
    message += `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}`;

    await sendMessage(chatId, message, sock);
    return;
  }

  // Clean command
  if (text.startsWith('/clean') || text.startsWith('!clean') || text.startsWith('.clean')) {
    const beforeRunning = runningStreams.size;
    const beforeRegistered = registeredStreams.size;

    const { cleanedRunning, cleanedRegistered } = cleanupExpiredStreams();

    await sendMessage(chatId,
      `🧹 *تم تنظيف الـ Streams منتهية المدة*\n\n` +
      `✅ تم تنظيف ${cleanedRunning} من الـ Streams الجارية\n` +
      `✅ تم تنظيف ${cleanedRegistered} من الـ Streams المسجلة\n\n` +
      `📊 *الحالة الحالية:*\n` +
      `▶ Streams جارية: ${runningStreams.size}\n` +
      `📝 Streams مسجلة: ${registeredStreams.size}`, sock);
    return;
  }

  // Stats command
  if (text.startsWith('/stats') || text.startsWith('!stats') || text.startsWith('.stats')) {
    const now = Date.now();
    const uptime = now - stats.botStartTime;
    
    let message = "📈 *إحصائيات النظام*\n\n";
    message += `⏱ *مدة تشغيل البوت:* ${formatDuration(uptime)}\n`;
    message += `🚀 *Streams بدأت:* ${stats.streamsStarted}\n`;
    message += `🛑 *Streams توقفت:* ${stats.streamsStopped}\n`;
    message += `⏳ *إجمالي وقت البث:* ${formatDuration(stats.totalStreamTime)}\n`;
    message += `⚠️ *الأخطاء:* ${stats.errors}\n\n`;
    
    message += `📊 *الحالة الحالية:*\n`;
    message += `▶ Streams جارية: ${runningStreams.size}\n`;
    message += `📝 Streams مسجلة: ${registeredStreams.size}\n\n`;
    
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

    if (runningStreams.has(name)) {
      await sendMessage(chatId,
        `⚠️ *البث "${name}" يعمل بالفعل*\n` +
        `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
      return;
    }

    cleanupExpiredStreams();

    registeredStreams.set(name, {
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

    if (runningStreams.has(name)) {
      await sendMessage(chatId,
        `⚠️ *البث "${name}" يعمل بالفعل*\n` +
        `استخدم */stop ${name}* لإيقافه أولاً.`, sock);
      return;
    }

    cleanupExpiredStreams();

    registeredStreams.set(name, {
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
    
    registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
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
    registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
    await sendMessage(chatId,
      `✅ *تم تشغيل البث*\n\n` +
      `📍 *الاسم:* ${name}\n` +
      `🌐 *المنصة:* Facebook\n` +
      `📡 *المصدر:* ${sourceUrl.length > 30 ? sourceUrl.substring(0, 30) + '...' : sourceUrl}`, sock);
    await startStreamByName(name, chatId, sock);
    return;
  }

  // Default response for unknown commands
  await sendMessage(chatId,
    `❓ *لم أفهم الأمر*\n\n` +
    `استخدم */help* لرؤية جميع الأوامر المتاحة\n\n` +
    `*أو أرسل:*\n` +
    `*اسم | رابط | مفتاح*\n` +
    `لبدء بث سريع على Facebook`, sock);
}

// ===== MAIN BOT =====
async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);

  const phone = pn('+' + PHONE_NUMBER);
  if (!phone.isValid()) {
    console.error('❌ Invalid phone number');
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
    printQRInTerminal: true,
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
      console.log(`✅ Connected successfully as ${PHONE_NUMBER}`);
      console.log('🤖 Stream Bot is now ready!');
      console.log(`⚙️ Settings: Max Duration=${formatDuration(MAX_DURATION)}, Max Retries=${MAX_RETRIES}`);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === 401) {
        console.log('❌ Logged out. Removing session...');
        removeSession(SESSION_DIR);
      } else {
        console.log('🔁 Connection closed unexpectedly. Reconnecting...');
        await delay(3000);
        startBot();
      }
    }
  });

  // ===== MESSAGE HANDLER =====
  KnightBot.ev.on('messages.upsert', async ({ messages }) => {
    for (const m of messages) {
      if (!m.message || m.key.fromMe) continue;

      const from = m.key.remoteJid;
      
      // Only handle private chats (not groups)
      if (!from.endsWith('@g.us')) {
        await handleCommand(m, KnightBot);
      }
    }
  });

  KnightBot.ev.on('creds.update', saveCreds);

  // ===== PAIRING CODE IF NOT REGISTERED =====
  if (!KnightBot.authState.creds.registered) {
    await delay(3000);
    try {
      let code = await KnightBot.requestPairingCode(PHONE_NUMBER);
      code = code?.match(/.{1,4}/g)?.join('-') || code;
      console.log(`📌 Pairing code for ${PHONE_NUMBER}: ${code}`);
    } catch (err) {
      console.error('❌ Failed to request pairing code:', err);
    }
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
      console.log(`🔄 Auto-cleanup: Running: ${runningBefore} → ${runningAfter}, Registered: ${registeredBefore} → ${registeredAfter}`);
    }
  }, 10 * 60 * 1000);
}

// ===== START BOT =====
startBot().catch(err => console.error('❌ Bot crashed:', err));

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log("\n🛑 Shutting down gracefully...");
  
  // Kill all running streams
  for (const [name, stream] of runningStreams.entries()) {
    try {
      if (stream.process) {
        stream.process.kill("SIGKILL");
      }
    } catch (e) {
      console.error(`Error killing stream ${name}:`, e);
    }
  }
  
  console.log("✅ All streams stopped. Goodbye!");
  process.exit(0);
});
