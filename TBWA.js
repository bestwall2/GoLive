import makeWASocket, { useMultiFileAuthState, fetchLatestBaileysVersion } from "@whiskeysockets/baileys";
import pino from "pino";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";

// ----------------------- CONFIG -----------------------
const config = {
  session: "auth",
  status: { terminal: true },
  setPair: true,
  pairingDelay: 5000 // ms
};
// ------------------------------------------------------

const rl = readline.createInterface({ input, output });

const clientstart = async () => {
  // 1️⃣ Auth state + version
  const { state, saveCreds } = await useMultiFileAuthState(`./${config.session}`);
  const { version } = await fetchLatestBaileysVersion();

  // 2️⃣ Create socket with your browser config
  const sock = makeWASocket({
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    auth: state,
    version,
    browser: ["Chrome (Linux)", "", ""]
  });

  // 3️⃣ Save credentials automatically
  sock.ev.on("creds.update", saveCreds);

  // 4️⃣ Wait a bit before requesting pairing code
  if (config.status.terminal && !sock.authState.creds.registered) {
    console.log(`⏳ Waiting ${config.pairingDelay}ms before requesting Pairing Code...`);
    await new Promise(res => setTimeout(res, config.pairingDelay));

    const phoneNumber = await rl.question(
      "📲 Please enter your WhatsApp number (e.g. 2126xxxxxxx):\n> "
    );

    try {
      const code = await sock.requestPairingCode(phoneNumber, config.setPair);
      console.log(`🔑 Your Pairing Code: ${code}`);
      console.log("➡️ Enter this code on WhatsApp → Linked Devices → Pair New Device");
    } catch (err) {
      console.error("❌ Failed to request Pairing Code:", err.message);
    }

    rl.close();
  }

  // 5️⃣ Auto-reply
  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    try {
      await sock.sendMessage(msg.key.remoteJid, { text: "مشغل" });
      console.log(`✅ Replied to ${msg.key.remoteJid}`);
    } catch (err) {
      console.error("❌ Failed to send message:", err.message);
    }
  });

  // 6️⃣ Connection updates
  sock.ev.on("connection.update", ({ connection, lastDisconnect }) => {
    console.log("🔄 connection.update:", connection);

    if (connection === "close") {
      console.log("❌ Connection closed:", lastDisconnect?.error?.output?.statusCode);
      setTimeout(clientstart, 15000); // reconnect after 15s
    }
  });
};

// Start bot
clientstart();
