import { makeWASocket, useMultiFileAuthState, DisconnectReason } from 'baileys';
import { Boom } from '@hapi/boom';
import QRCode from 'qrcode';

async function connectToWhatsApp() {
    // 1. Setup Authentication State (FOR DEMO ONLY)
    // ⚠️ WARNING: As per docs, DO NOT use `useMultiFileAuthState` in production.
    const { state, saveCreds } = await useMultiFileAuthState('./auth_info_pairing');

    // 2. Create the WhatsApp Socket
    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false, // We'll handle QR/connection manually
    });

    // 3. Save credentials whenever they update
    sock.ev.on('creds.update', saveCreds);

    // 4. 👇 YOUR PHONE NUMBER HERE (in E.164 format, no '+')
    const phoneNumber = '212629996310'; // Format: 12345678901

    // 5. Handle Connection & Pairing Code Request
    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        // --- Handle Disconnection & Reconnection ---
        if (connection === 'close') {
            const statusCode = (lastDisconnect?.error)?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

            console.log('Connection closed. Status:', statusCode, '. Reconnecting:', shouldReconnect);

            if (shouldReconnect) {
                // Reconnect logic can go here
                // connectToWhatsApp();
            }
        }

        // --- Request Pairing Code when connecting or QR is received ---
        // The docs say: wait for "connecting" state OR when a `qr` event exists
        if (connection === 'connecting' || qr) {
            console.log('Requesting pairing code for', phoneNumber, '...');

            try {
                // This is the key function from the documentation
                const pairingCode = await sock.requestPairingCode(phoneNumber);
                console.log('✅ Pairing Code:', pairingCode);
                console.log('-> Enter this code in your phone\'s WhatsApp Linked Devices section.');
            } catch (err) {
                console.error('Failed to get pairing code:', err);
            }
        }

        // --- Confirm Successful Connection ---
        if (connection === 'open') {
            console.log('✅ Bot is online and ready!');
        }
    });

    // 6. Listen for Messages and Respond with "hi"
    sock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        // Ignore if the message is from yourself or has no content
        if (!msg.message || msg.key.fromMe) {
            return;
        }

        const sender = msg.key.remoteJid; // The chat ID
        const text = msg.message.conversation || msg.message.extendedTextMessage?.text || '';

        console.log(`📩 Message from ${sender}: ${text}`);

        // Auto-reply with "hi"
        await sock.sendMessage(sender, { text: 'hi' });
        console.log(`✅ Replied "hi" to ${sender}`);
    });
}

// Start the bot
connectToWhatsApp();
