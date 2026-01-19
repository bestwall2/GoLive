import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} from "@whiskeysockets/baileys";
import Pino from "pino";

async function startBot() {
  console.log("🚀 Starting bot...");

  const { state, saveCreds } = await useMultiFileAuthState("auth");

  // اجلب أحدث بروتوكول واتساب
  const { version } = await fetchLatestBaileysVersion();
  console.log("Using WhatsApp Version:", version.join("."));

  const sock = makeWASocket({
    version,
    auth: state,
    logger: Pino({ level: "info" }),
    printQRInTerminal: false,
  });

  sock.ev.on("creds.update", saveCreds);

  let codeSent = false;

  sock.ev.on("connection.update", async (update) => {
    console.log("🔄 connection.update:", update);

    const { connection } = update;

    if (connection === "open") {
      console.log("✅ Connected to WhatsApp");
    }

    // عندما يبدأ الاتصال، اطلب Pairing Code مرة واحدة
    if ((update.connection === "connecting" || !!update.qr) && !codeSent) {
      codeSent = true;
      const phone = "212629996310"; // رقمك بدون +
      console.log("📲 Requesting pairing code...");
      const code = await sock.requestPairingCode(phone);
      console.log("🔑 Pairing Code:", code);
      console.log("ادخل الكود في واتساب (الإعدادات -> الأجهزة المرتبطة -> ربط جهاز)");
    }

    if (connection === "close") {
      const reason = update.lastDisconnect?.error?.output?.statusCode;
      console.log("❌ Connection closed. Reason:", reason);

      if (reason !== DisconnectReason.loggedOut) {
        console.log("🔁 Reconnecting...");
        startBot();
      }
    }
  });

  // استقبال الرسائل والرد
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    console.log("📩 Received message:", msg.message);
    await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
  });
}

startBot();
