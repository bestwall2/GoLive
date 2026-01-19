import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers
} from "@whiskeysockets/baileys";

let pairingRequested = false;

async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState("auth");
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    browser: Browsers.macOS("Google Chrome") // ✅ required for Pairing Code
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;
    console.log("🔄 connection.update:", connection);

    if (connection === "open" && !sock.authState.creds.registered && !pairingRequested) {
      pairingRequested = true;
      const phoneNumber = "212629996310"; // without '+'
      const code = await sock.requestPairingCode(phoneNumber);
      console.log("🔑 Pairing Code:", code);
      console.log("➡️ Enter it in WhatsApp → Linked Devices → Pair New Device");
    }

    if (connection === "close") {
      console.log("❌ Connection closed:", lastDisconnect?.error?.output?.statusCode);
      setTimeout(startBot, 15000);
    }
  });

  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;
    await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
  });
}

startBot();
