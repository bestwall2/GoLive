import { makeWASocket, useMultiFileAuthState, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, DisconnectReason, delay, Browsers } from "@whiskeysockets/baileys";
import pino from "pino";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";
import { Boom } from "@hapi/boom";

// ------------------- CONFIG -------------------
const SESSION_FOLDER = "@OpenWA";
const PAIRING_DELAY = 5000; // ms
// ----------------------------------------------

const rl = readline.createInterface({ input, output });

async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_FOLDER);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    browser: Browsers.ubuntu("Chrome"),
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, pino({ level: "silent" }))
    }
  });

  sock.ev.on("creds.update", saveCreds);

  // Ask for phone number first
  if (!sock.authState.creds.registered) {
    const phoneNumber = await rl.question("📲 Enter your WhatsApp number (e.g., 2126xxxxxxx):\n> ");
    console.log(`⏳ Waiting ${PAIRING_DELAY}ms before requesting Pairing Code...`);
    await new Promise(res => setTimeout(res, PAIRING_DELAY));

    try {
      const code = await sock.requestPairingCode(phoneNumber.replace(/[^0-9]/g, ""), true);
      console.log(`🔑 Pairing Code: ${code.match(/.{1,4}/g).join("-")}`);
      console.log("➡️ Enter this code on WhatsApp → Linked Devices → Pair New Device");
    } catch (err) {
      console.error("❌ Failed to request Pairing Code:", err.message);
    }
  }

  rl.close();

  // Auto-reply "مشغل"
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

  // Connection updates
  sock.ev.on("connection.update", ({ connection, lastDisconnect }) => {
    console.log("🔄 connection.update:", connection);
    if (connection === "close") {
      const status = new Boom(lastDisconnect?.error)?.output?.statusCode;
      console.log(`❌ Connection closed, code: ${status}. Reconnecting...`);
      setTimeout(startBot, 15000);
    }
    if (connection === "open") {
      console.log(`✅ Logged in as ${sock.user?.name} (${sock.user?.id.split(":")[0]})`);
    }
  });
}

startBot();
