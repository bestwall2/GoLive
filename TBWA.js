import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeInMemoryStore
} from "@whiskeysockets/baileys";
import pino from "pino";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";

// ----------------------- CONFIG -----------------------
const config = {
  session: "auth",
  status: {
    terminal: true
  },
  setPair: true
};
// ------------------------------------------------------

const rl = readline.createInterface({ input, output });

const clientstart = async () => {
  const store = makeInMemoryStore({
    logger: pino().child({ level: "silent" })
  });

  const { state, saveCreds } = await useMultiFileAuthState(`./${config.session}`);
  const { version } = await fetchLatestBaileysVersion();

  const client = makeWASocket({
    logger: pino({ level: "silent" }),
    printQRInTerminal: false,
    auth: state,
    version,
    browser: ["Ubuntu", "Chrome", "20.0.00"] // 🔹 your requested browser
  });

  client.ev.on("creds.update", saveCreds);
  store.bind(client.ev);

  if (config.status.terminal && !client.authState.creds.registered) {
    const phoneNumber = await rl.question(
      "📲 Please enter your WhatsApp number (e.g. 2126xxxxxxx):\n> "
    );

    try {
      const code = await client.requestPairingCode(phoneNumber, config.setPair);
      console.log(`🔑 Your Pairing Code: ${code}`);
      console.log("➡️ Enter this code on WhatsApp → Linked Devices → Pair New Device");
    } catch (err) {
      console.error("❌ Failed to request Pairing Code:", err.message);
    }

    rl.close();
  }

  client.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.message || msg.key.fromMe) return;

    try {
      await client.sendMessage(msg.key.remoteJid, { text: "مشغل" });
      console.log(`✅ Replied to ${msg.key.remoteJid}`);
    } catch (err) {
      console.error("❌ Failed to send message:", err.message);
    }
  });

  client.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect } = update;
    console.log("🔄 connection.update:", connection);

    if (connection === "close") {
      console.log("❌ Connection closed:", lastDisconnect?.error?.output?.statusCode);
      setTimeout(clientstart, 15000); // reconnect after 15s
    }
  });
};

clientstart();
