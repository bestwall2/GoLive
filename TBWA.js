import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason
} from "@whiskeysockets/baileys";
import Pino from "pino";

async function startBot() {
  // حفظ الجلسة
  const { state, saveCreds } = await useMultiFileAuthState("auth");

  const sock = makeWASocket({
    auth: state,
    logger: Pino({ level: "silent" }),
    printQRInTerminal: false // لا نحتاج QR
  });

  // طلب Pairing Code (مرة واحدة فقط)
  if (!sock.authState.creds.registered) {
    const phoneNumber = "212629996310"; // رقمك مع كود الدولة بدون +
    const code = await sock.requestPairingCode(phoneNumber);
    console.log("PAIRING CODE:", code);
  }

  // حفظ بيانات الجلسة
  sock.ev.on("creds.update", saveCreds);

  // مراقبة الاتصال
  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === "open") {
      console.log("✅ WhatsApp connected successfully");
    }

    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.statusCode;
      if (reason !== DisconnectReason.loggedOut) {
        startBot();
      }
    }
  });

  // استقبال الرسائل والرد
  sock.ev.on("messages.upsert", async (m) => {
    const msg = m.messages[0];
    if (!msg.message || msg.key.fromMe) return;

    const jid = msg.key.remoteJid;

    await sock.sendMessage(jid, { text: "hi" });
  });
}

startBot();
