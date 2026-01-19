import fs from 'fs';
import pino from 'pino';
import {
  makeWASocket,
  useMultiFileAuthState,
  delay,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  Browsers,
  jidNormalizedUser
} from '@whiskeysockets/baileys';
import pn from 'awesome-phonenumber';

// ===== CONFIG =====
const PHONE_NUMBER = '212629996310'; // Replace with your number
const SESSION_DIR = `./session-${PHONE_NUMBER}`;

// Ensure session folder exists
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR, { recursive: true });

// Remove old session
function removeSession(folder) {
  if (!fs.existsSync(folder)) return;
  fs.rmSync(folder, { recursive: true, force: true });
}

// ===== MAIN BOT =====
async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);

  const phone = pn('+' + PHONE_NUMBER);
  if (!phone.isValid()) {
    console.error('❌ Invalid phone number');
    return process.exit(1);
  }

  const { version } = await fetchLatestBaileysVersion();

  const KnightBot = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(
        state.keys,
        pino({ level: 'fatal' }).child({ level: 'fatal' })
      ),
    },
    printQRInTerminal: true, // Terminal pairing code
    logger: pino({ level: 'fatal' }).child({ level: 'fatal' }),
    browser: Browsers.windows('Chrome'),
    markOnlineOnConnect: false,
    generateHighQualityLinkPreview: false,
    defaultQueryTimeoutMs: 60000,
    connectTimeoutMs: 60000,
    keepAliveIntervalMs: 30000,
    retryRequestDelayMs: 250,
    maxRetries: 5,
  });

  // ===== CONNECTION EVENTS =====
  KnightBot.ev.on('connection.update', async ({ connection, lastDisconnect, isNewLogin, isOnline }) => {
    if (connection === 'open') {
      console.log(`✅ Connected successfully as ${PHONE_NUMBER}`);
    }

    if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === 401) {
        console.log('❌ Logged out. Removing session...');
        removeSession(SESSION_DIR);
      } else {
        console.log('🔁 Connection closed unexpectedly. Reconnecting...');
        await delay(3000);
        startBot();
      }
    }

    if (isNewLogin) console.log('🔐 New login via pairing code');
    if (isOnline) console.log('📶 Client is online');
  });

  // ===== AUTO-REPLY "hi" =====
  KnightBot.ev.on('messages.upsert', async ({ messages }) => {
    for (const m of messages) {
      if (!m.message || m.key.fromMe) continue;
      const from = m.key.remoteJid;
      await KnightBot.sendMessage(from, { text: 'hi' });
      console.log(`📩 Replied "hi" to ${from}`);
    }
  });

  KnightBot.ev.on('creds.update', saveCreds);

  // ===== PAIRING CODE IF NOT REGISTERED =====
  if (!KnightBot.authState.creds.registered) {
    await delay(3000);
    try {
      let code = await KnightBot.requestPairingCode(PHONE_NUMBER);
      code = code?.match(/.{1,4}/g)?.join('-') || code;
      console.log(`📌 Pairing code for ${PHONE_NUMBER}: ${code}`);
    } catch (err) {
      console.error('❌ Failed to request pairing code:', err);
    }
  }
}

// ===== START BOT =====
startBot().catch(err => console.error('❌ Bot crashed:', err));
