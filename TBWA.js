import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason
} from "@whiskeysockets/baileys";
import Pino from "pino";

let pairingRequested = false;

async function startBot() {
  console.log("🚀 Starting WhatsApp bot...");

  const { state, saveCreds } = await useMultiFileAuthState("auth");
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    logger: Pino({ level: "debug" }), // مهم للتشخيص
    printQRInTerminal: false,

    // 🔴 إعدادات إجبار الاتصال (مهمة على VPS)
    browser: ["Ubuntu VPS", "Chrome", "22.04"],
    connectTimeoutMs: 60_000,
    keepAliveIntervalMs: 15_000,
    defaultQueryTimeoutMs: 60_000,
    markOnlineOnConnect: false,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    console.log("📡 connection.update event:", update);

    // ✅ اطلب Pairing Code فقط بعد OPEN
    if (
      connection === "open" &&
      !sock.authState.creds.registered &&
      !pairingRequested
    ) {
      pairingRequested = true;
      const phone = "212629996310";
      console.log("📲 Requesting pairing code...");
      const code = await sock.requestPairingCode(phone);
      console.log("🔑 PAIRING CODE:", code);
      console.log("➡️ أدخل الكود فوراً في واتساب");
    }

    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.statusCode;
      console.log("❌ Connection closed. Reason:", reason);

      if (reason !== DisconnectReason.loggedOut) {
        console.log("⏳ Reconnecting in 15s...");
        setTimeout(startBot, 15_000);
      }
    }
  });

  // 📩 استقبال الرسائل
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    console.log("📩 Message received");
    await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
  });
}

startBot();
