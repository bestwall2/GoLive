import { Bot, Context, InputFile } from "grammy";
import { spawn } from "child_process";
import dotenv from 'dotenv';

// تحميل متغيرات البيئة
dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const MAX_DURATION = (parseInt(process.env.MAX_DURATION_HOURS) || 4) * 60 * 60 * 1000; // 4 ساعات افتراضياً
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 30000;

const bot = new Bot(BOT_TOKEN);

// Maps:
// registeredStreams: name -> { sourceUrl, streamKey, type?, drawText?, platform? }
// runningStreams: name -> { process, sourceUrl, streamKey, startTime, useEncoding, type?, drawText?, platform?, retryCount }
const registeredStreams = new Map();
const runningStreams = new Map();

// إحصائيات
const stats = {
  streamsStarted: 0,
  streamsStopped: 0,
  totalStreamTime: 0,
  errors: 0,
  lastCleanup: Date.now(),
  botStartTime: Date.now()
};

// ===============================
// Utils
// ===============================
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
  // Facebook (default)
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

// ===============================
// FFmpeg Args
// ===============================
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

// ===============================
// Cleanup Expired Streams
// ===============================
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
      
      // تحديث الإحصائيات
      stats.streamsStopped++;
      stats.totalStreamTime += MAX_DURATION;
    }
  }

  return { cleanedRunning, cleanedRegistered };
}

// ===============================
// Stream Runner
// ===============================
async function startStreamByName(name, ctx) {
  if (!registeredStreams.has(name)) {
    if (ctx) await ctx.reply(`❌ لا يوجد Stream مسجّل بالاسم: ${name}`);
    return;
  }

  // تنظيف أولاً قبل البدء
  cleanupExpiredStreams();

  if (runningStreams.has(name)) {
    const stream = runningStreams.get(name);
    const elapsed = Date.now() - stream.startTime;
    const timeLeft = Math.max(0, MAX_DURATION - elapsed);
    
    if (ctx) {
      await ctx.reply(
        `⚠️ الـ Stream "${name}" يعمل بالفعل منذ ${formatDuration(elapsed)}\n` +
        `⏳ متبقي: ${formatDuration(timeLeft)}\n` +
        `استخدم /stop ${name} لإيقافه أولاً.`
      );
    }
    return;
  }

  const { sourceUrl, streamKey, type, drawText, platform = "facebook" } = registeredStreams.get(name);
  const rtmpUrl = getRtmpUrl(streamKey, platform);
  const streamType = type || "normal";

  // تحديث الإحصائيات
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

    // Track encoding switching (فقط للنوع العادي)
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

      // Check if maximum duration reached
      if (elapsed >= MAX_DURATION) {
        console.log(`⏰ [${name}] Maximum duration reached, stopping...`);
        stopStreamByName(name, ctx);
        if (ctx) {
          ctx.reply(`⏳ مدة البث "${name}" انتهت (${formatDuration(MAX_DURATION)}) وتم إيقافه.`);
        }
        return;
      }

      // Check if we need to switch to encoding mode
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

      /*
      // Check max retries
      if (retryCount >= MAX_RETRIES) {
        console.log(`❌ [${name}] تجاوز الحد الأقصى لمحاولات إعادة التشغيل`);
        stopStreamByName(name, ctx);
        if (ctx) {
          ctx.reply(`❌ فشل تشغيل "${name}" بعد ${MAX_RETRIES} محاولات`);
        }
        return;
      }*/

      // Normal restart logic
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

  if (ctx) {
    const sourceType = detectSourceType(sourceUrl);
    let message = `✅ تم تشغيل البث باسم: ${name}\n`;
    message += `🌐 المنصة: ${platform === "instagram" ? "Instagram" : "Facebook"}\n`;
    message += `🔑 Stream Key: ${streamKey.substring(0, 10)}...\n`;
    message += `📡 المصدر: ${sourceUrl.length > 50 ? sourceUrl.substring(0, 50) + '...' : sourceUrl}\n`;
    message += `📊 النوع: ${sourceType}\n`;

    if (streamType === "encode-with-watermark") {
      message += `🎬 وضع: ENCODE مع Watermark\n`;
      if (drawText) message += `📝 Watermark: ${drawText.substring(0, 30)}...\n`;
    } else {
      message += `🔧 وضع: ${streamType === "normal" ? "COPY (تلقائي)" : "ENCODE"}\n`;
    }

    if (platform === "instagram") {
      message += `🔄 الفيديو: تم تدويره 90°\n`;
    }

    message += `⏱ أقصى مدة: ${formatDuration(MAX_DURATION)}\n`;
    message += `لإيقاف: /stop ${name}`;

    await ctx.reply(message);
  }
}

// ===============================
// Stop Stream
// ===============================
async function stopStreamByName(name, ctx) {
  cleanupExpiredStreams();

  const stream = runningStreams.get(name);
  if (!stream) {
    if (ctx) await ctx.reply(`⚠️ لا يوجد بث جارٍ بالاسم: ${name}`);
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
  
  // تحديث الإحصائيات
  stats.streamsStopped++;
  stats.totalStreamTime += duration;

  runningStreams.delete(name);

  if (ctx) {
    let message = `🛑 تم إيقاف البث: ${name}\n`;
    message += `🌐 المنصة: ${stream.platform === "instagram" ? "Instagram" : "Facebook"}\n`;
    message += `⏱ المدة الكلية: ${formatDuration(duration)}\n`;

    if (stream.type === "encode-with-watermark" && stream.drawText) {
      message += `📝 Watermark كان: ${stream.drawText.substring(0, 30)}...`;
    } else if (stream.type === "normal") {
      message += `🔧 وضع: ${stream.useEncoding ? 'ENCODING' : 'COPY'}`;
    } else {
      message += `🎬 نوع: ${stream.type}`;
    }

    await ctx.reply(message);
  }
}

// ===============================
// Command Handlers
// ===============================

// Start command
bot.command("start", async (ctx) => {
  const args = ctx.message?.text?.split(" ").slice(1);
  
  if (args && args.length > 0) {
    // If there's an argument, start a specific stream
    const name = args.join(" ").trim();
    await startStreamByName(name, ctx);
  } else {
    // Show help message - FIXED: Using HTML parse_mode which is more reliable
    await ctx.reply(
      `<b>🎥 أوامر البوت:</b>\n\n` +
      `<b>📝 التسجيل:</b>\n` +
      `<code>/add NAME | SOURCE_URL | STREAM_KEY</code>  - سجّل Stream (لا يبدأ تلقائياً)\n\n` +
      `<b>▶️ التشغيل والإيقاف:</b>\n` +
      `<code>/start NAME</code>                         - شغّل Stream مسجّل بالاسم\n` +
      `<code>/stop NAME</code>                          - أوقف Stream جاري بالاسم\n\n` +
      `<b>📋 المعاينة والإدارة:</b>\n` +
      `<code>/list</code>                               - عرض المسجّلة والجارية\n` +
      `<code>/clean</code>                              - تنظيف الـ Streams منتهية المدة\n` +
      `<code>/stats</code>                              - إحصائيات النظام\n\n` +
      `<b>🎬 أنواع البث:</b>\n` +
      `<code>/encode NAME | SOURCE_URL | STREAM_KEY | DRAW_TEXT</code> - بث مع Watermark\n` +
      `<code>/ig NAME | SOURCE_URL | STREAM_KEY</code> - بث على Instagram مع تدوير الفيديو\n\n` +
      `<b>⚡ الطريقة السريعة:</b>\n` +
      `يمكنك إرسال مباشرة:\n` +
      `<code>NAME | SOURCE_URL | STREAM_KEY</code>\n` +
      `وسيتم تسجيله وتشغيله مباشرة على Facebook.\n\n` +
      `<b>⏱ المدة القصوى:</b> ${formatDuration(MAX_DURATION)}\n` +
      `<b>🔄 المحاولات القصوى:</b> ${MAX_RETRIES}`,
      { parse_mode: "HTML" }
    );
  }
});

// Add command
bot.command("add", async (ctx) => {
  const text = ctx.message?.text;
  if (!text) return;
  
  const raw = text.substring(5).trim();
  const parts = raw.split("|").map(s => s.trim());
  
  if (parts.length !== 3) {
    return await ctx.reply(
      "❌ <b>صيغة /add غير صحيحة</b>\n\n" +
      "<b>📝 الصيغة الصحيحة:</b>\n" +
      "<code>/add NAME | SOURCE_URL | STREAM_KEY</code>\n\n" +
      "<b>مثال:</b>\n" +
      "<code>/add مباراة1 | https://example.com/stream.m3u8 | FB_123456789</code>",
      { parse_mode: "HTML" }
    );
  }
  
  const [name, sourceUrl, streamKey] = parts;
  
  if (!isValidUrl(sourceUrl)) {
    return await ctx.reply("❌ رابط المصدر غير صالح");
  }
  
  registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
  
  await ctx.reply(
    `✅ <b>تم تسجيل Stream بنجاح</b>\n\n` +
    `📍 <b>الاسم:</b> ${name}\n` +
    `🌐 <b>المنصة:</b> Facebook\n` +
    `📡 <b>المصدر:</b> ${sourceUrl.length > 40 ? sourceUrl.substring(0, 40) + '...' : sourceUrl}\n` +
    `🔑 <b>المفتاح:</b> ${streamKey.substring(0, 10)}...\n\n` +
    `استخدم <code>/start ${name}</code> لتشغيله`,
    { parse_mode: "HTML" }
  );
});

// Stop command
bot.command("stop", async (ctx) => {
  const text = ctx.message?.text;
  if (!text) return;
  
  const name = text.substring(5).trim();
  await stopStreamByName(name, ctx);
});

// List command
bot.command("list", async (ctx) => {
  cleanupExpiredStreams();

  if (runningStreams.size === 0 && registeredStreams.size === 0) {
    return await ctx.reply("📭 لا توجد Streams مسجلة أو جارية");
  }

  let message = "";

  // عرض الـ Streams الجارية
  if (runningStreams.size > 0) {
    message += "<b>▶️ Streams الجارية:</b>\n\n";
    
    for (const [name, stream] of runningStreams.entries()) {
      const elapsed = Date.now() - stream.startTime;
      const timeLeft = Math.max(0, MAX_DURATION - elapsed);
      const sourceType = detectSourceType(stream.sourceUrl);
      
      message += `📍 <b>${name}</b>\n`;
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
      
      message += `   🔑 المفتاح: ${stream.streamKey.substring(0, 8)}...\n\n`;
    }
  }

  // عرض الـ Streams المسجلة فقط (غير جارية)
  const inactiveStreams = Array.from(registeredStreams.entries())
    .filter(([name]) => !runningStreams.has(name));

  if (inactiveStreams.length > 0) {
    message += "<b>📝 Streams المسجلة (غير جارية):</b>\n\n";
    
    for (const [name, info] of inactiveStreams) {
      message += `📍 <b>${name}</b>\n`;
      message += `   🌐 ${info.platform === "instagram" ? "Instagram" : "Facebook"}\n`;
      message += `   📡 ${info.sourceUrl.length > 30 ? info.sourceUrl.substring(0, 30) + '...' : info.sourceUrl}\n`;
      
      if (info.type === "encode-with-watermark" && info.drawText) {
        message += `   📝 Watermark: ${info.drawText.substring(0, 20)}...\n`;
      }
      
      message += `   🔧 نوع: ${info.type || "normal"}\n`;
      message += `   🔑 المفتاح: ${info.streamKey.substring(0, 8)}...\n\n`;
    }
  }

  // إضافة الإحصائيات
  message += `<b>📊 الإحصائيات:</b>\n`;
  message += `▶ عدد Streams الجارية: ${runningStreams.size}\n`;
  message += `📝 عدد Streams المسجلة: ${registeredStreams.size}\n`;
  message += `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}`;

  await ctx.reply(message, { parse_mode: "HTML" });
});

// Clean command
bot.command("clean", async (ctx) => {
  const beforeRunning = runningStreams.size;
  const beforeRegistered = registeredStreams.size;

  const { cleanedRunning, cleanedRegistered } = cleanupExpiredStreams();

  await ctx.reply(
    `🧹 <b>تم تنظيف الـ Streams منتهية المدة</b>\n\n` +
    `✅ تم تنظيف ${cleanedRunning} من الـ Streams الجارية\n` +
    `✅ تم تنظيف ${cleanedRegistered} من الـ Streams المسجلة\n\n` +
    `<b>📊 الحالة الحالية:</b>\n` +
    `▶ Streams جارية: ${runningStreams.size}\n` +
    `📝 Streams مسجلة: ${registeredStreams.size}`,
    { parse_mode: "HTML" }
  );
});

// Stats command
bot.command("stats", async (ctx) => {
  const now = Date.now();
  const uptime = now - stats.botStartTime;
  
  let message = "<b>📈 إحصائيات النظام</b>\n\n";
  message += `<b>⏱ مدة تشغيل البوت:</b> ${formatDuration(uptime)}\n`;
  message += `<b>🚀 Streams بدأت:</b> ${stats.streamsStarted}\n`;
  message += `<b>🛑 Streams توقفت:</b> ${stats.streamsStopped}\n`;
  message += `<b>⏳ إجمالي وقت البث:</b> ${formatDuration(stats.totalStreamTime)}\n`;
  message += `<b>⚠️ الأخطاء:</b> ${stats.errors}\n\n`;
  
  message += `<b>📊 الحالة الحالية:</b>\n`;
  message += `▶ Streams جارية: ${runningStreams.size}\n`;
  message += `📝 Streams مسجلة: ${registeredStreams.size}\n\n`;
  
  message += `<b>⚙️ الإعدادات:</b>\n`;
  message += `⏱ المدة القصوى: ${formatDuration(MAX_DURATION)}\n`;
  message += `🔄 المحاولات القصوى: ${MAX_RETRIES}`;
  
  await ctx.reply(message, { parse_mode: "HTML" });
});

// Encode command
bot.command("encode", async (ctx) => {
  const text = ctx.message?.text;
  if (!text) return;
  
  const input = text.substring(8).trim();
  const parts = input.split("|").map(s => s.trim());

  if (parts.length < 4) {
    return await ctx.reply(
      "❌ <b>الصيغة الصحيحة:</b>\n\n" +
      "<code>/encode NAME | SOURCE_URL | STREAM_KEY | DRAW_TEXT</code>\n\n" +
      "<b>مثال:</b>\n" +
      "<code>/encode مباراة1 | https://example.com/stream.m3u8 | FB_123456789 | drawtext=text='LIVE TV':x=W-w-20:y=20:fontsize=28:fontcolor=white@0.7</code>\n\n" +
      "<b>📝 ملاحظة:</b>\n" +
      `يمكنك استخدام الاسم نفسه مع <code>/stop NAME</code> لإيقافه`,
      { parse_mode: "HTML" }
    );
  }

  const [name, sourceUrl, streamKey, drawText] = parts;

  if (!isValidUrl(sourceUrl)) {
    return await ctx.reply("❌ رابط المصدر غير صالح");
  }

  if (runningStreams.has(name)) {
    return await ctx.reply(
      `⚠️ البث "${name}" يعمل بالفعل.\n` +
      `استخدم /stop ${name} لإيقافه أولاً.`
    );
  }

  cleanupExpiredStreams();

  registeredStreams.set(name, {
    sourceUrl,
    streamKey,
    type: "encode-with-watermark",
    drawText: drawText,
    platform: "facebook"
  });

  await ctx.reply(`🎬 جار بدء البث (ENCODE + WATERMARK)...`);
  await startStreamByName(name, ctx);
});

// Instagram command
bot.command("ig", async (ctx) => {
  const text = ctx.message?.text;
  if (!text) return;
  
  const input = text.substring(4).trim();
  const parts = input.split("|").map(s => s.trim());

  if (parts.length !== 3) {
    return await ctx.reply(
      "❌ <b>الصيغة الصحيحة:</b>\n\n" +
      "<code>/ig NAME | SOURCE_URL | STREAM_KEY</code>\n\n" +
      "<b>مثال:</b>\n" +
      "<code>/ig مباراة1 | https://example.com/stream.m3u8 | IG_123456789</code>\n\n" +
      "<b>📱 ملاحظات Instagram:</b>\n" +
      "• سيقوم تلقائياً بتدوير الفيديو 90°\n" +
      "• يستخدم ENCODING مع Watermark\n" +
      `• المدة القصوى: ${formatDuration(MAX_DURATION)}`,
      { parse_mode: "HTML" }
    );
  }

  const [name, sourceUrl, streamKey] = parts;

  if (!isValidUrl(sourceUrl)) {
    return await ctx.reply("❌ رابط المصدر غير صالح");
  }

  if (runningStreams.has(name)) {
    return await ctx.reply(
      `⚠️ البث "${name}" يعمل بالفعل.\n` +
      `استخدم /stop ${name} لإيقافه أولاً.`
    );
  }

  cleanupExpiredStreams();

  registeredStreams.set(name, {
    sourceUrl,
    streamKey,
    type: "encode-with-watermark",
    platform: "instagram"
  });

  await ctx.reply(
    `<b>📱 جار بدء البث على Instagram...</b>\n\n` +
    `<b>⚙️ الإعدادات:</b>\n` +
    `• تدوير الفيديو 90° تلقائياً\n` +
    `• ENCODING مع Watermark\n` +
    `• المدة القصوى: ${formatDuration(MAX_DURATION)}\n` +
    `• المحاولات القصوى: ${MAX_RETRIES}`,
    { parse_mode: "HTML" }
  );

  await startStreamByName(name, ctx);
});

// Quick add+start via "NAME | SOURCE_URL | STREAM_KEY"
bot.on("message", async (ctx) => {
  const text = ctx.message?.text;
  if (!text || text.startsWith("/")) return;
  
  const parts = text.split("|").map(x => x.trim());

  if (parts.length === 3) {
    const [name, sourceUrl, streamKey] = parts;
    
    if (!isValidUrl(sourceUrl)) {
      return await ctx.reply("❌ رابط المصدر غير صالح");
    }
    
    registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
    await ctx.reply(
      `✅ <b>تم التسجيل والتشغيل</b>\n\n` +
      `📍 <b>الاسم:</b> ${name}\n` +
      `🌐 <b>المنصة:</b> Facebook\n` +
      `⏳ جار التشغيل...`,
      { parse_mode: "HTML" }
    );
    await startStreamByName(name, ctx);
    return;
  }

  if (parts.length === 2) {
    const [sourceUrl, streamKey] = parts;
    
    if (!isValidUrl(sourceUrl)) {
      return await ctx.reply("❌ رابط المصدر غير صالح");
    }
    
    const name = `S-${Date.now()}`;
    registeredStreams.set(name, { sourceUrl, streamKey, type: "normal", platform: "facebook" });
    await ctx.reply(
      `✅ <b>تم تشغيل البث</b>\n\n` +
      `📍 <b>الاسم:</b> ${name}\n` +
      `🌐 <b>المنصة:</b> Facebook\n` +
      `📡 <b>المصدر:</b> ${sourceUrl.length > 30 ? sourceUrl.substring(0, 30) + '...' : sourceUrl}`,
      { parse_mode: "HTML" }
    );
    await startStreamByName(name, ctx);
    return;
  }
});

// ===============================
// Error Handler
// ===============================
bot.catch((err) => {
  console.error("❌ Bot error:", err);
  stats.errors++;
});

// ===============================
// Auto Cleanup Intervals
// ===============================
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

// ===============================
// Error Handling
// ===============================
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  stats.errors++;
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  stats.errors++;
});

// ===============================
// Start the Bot
// ===============================
bot.start().then(() => {
  console.log("✅ Bot started with Facebook & Instagram support.");
  console.log(`⚙️ Settings: Max Duration=${formatDuration(MAX_DURATION)}, Max Retries=${MAX_RETRIES}`);
}).catch((error) => {
  console.error("❌ Failed to start bot:", error);
  process.exit(1);
});

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
