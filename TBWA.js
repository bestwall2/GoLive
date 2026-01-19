import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} from "@whiskeysockets/baileys";
import Pino from "pino";

let sock;
let pairingRequested = false;

async function startBot() {
  console.log("🚀 Starting WhatsApp bot...");

  const { state, saveCreds } = await useMultiFileAuthState("auth");
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    auth: state,
    logger: Pino({ level: "silent" }),
    printQRInTerminal: false,
    browser: ["Ubuntu VPS", "Chrome", "22.04"]
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    console.log("🔄 connection:", connection);

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
      console.log("❌ Closed. Reason:", reason);

      if (reason !== DisconnectReason.loggedOut) {
        console.log("⏳ Waiting before reconnect...");
        setTimeout(startBot, 10_000); // delay مهم
      }
    }
  });

  // 📩 استقبال الرسائل والرد
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
  });
}

startBot();
