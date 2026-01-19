import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason
} from "@whiskeysockets/baileys";
import Pino from "pino";

async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState("auth");

  const sock = makeWASocket({
    auth: state,
    logger: Pino({ level: "silent" }),
    printQRInTerminal: false
  });

  sock.ev.on("creds.update", saveCreds);

  let pairingRequested = false;

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === "open") {
      console.log("✅ Connected to WhatsApp");

      // 🔑 طلب Pairing Code بعد الاتصال
      if (!sock.authState.creds.registered && !pairingRequested) {
        pairingRequested = true;

        const phoneNumber = "212629996310"; // بدون +
        const code = await sock.requestPairingCode(phoneNumber);
        console.log("📲 PAIRING CODE:", code);
      }
    }

    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.statusCode;
      if (reason !== DisconnectReason.loggedOut) {
        console.log("🔁 Reconnecting...");
        startBot();
      }
    }
  });

  // استقبال الرسائل
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    const jid = msg.key.remoteJid;
    await sock.sendMessage(jid, { text: "hi" });
  });
}

startBot();
