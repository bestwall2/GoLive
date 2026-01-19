import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason
} from "@whiskeysockets/baileys";
import Pino from "pino";

let pairingRequested = false;

async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState("auth");
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    logger: Pino({ level: "debug" }),
    printQRInTerminal: false,
    browser: ["Ubuntu VPS", "Chrome", "22.04"]
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;
    console.log("🔄 connection.update:", connection);

    if (
      connection === "open" &&
      !sock.authState.creds.registered &&
      !pairingRequested
    ) {
      pairingRequested = true;
      const phoneNumber = "212629996310"; // your number without +
      const code = await sock.requestPairingCode(phoneNumber);
      console.log("🔑 Pairing Code:", code);
      console.log("➡️ Enter it on your phone under WhatsApp → Linked Devices → Pair New Device");
    }

    if (connection === "close") {
      const reason = lastDisconnect?.error?.output?.statusCode;
      console.log("❌ Connection closed. Reason:", reason);
      if (reason !== DisconnectReason.loggedOut) {
        setTimeout(startBot, 15000);
      }
    }
  });

  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;
    await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
  });
}

startBot();
