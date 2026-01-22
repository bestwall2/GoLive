import express from 'express';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import session from 'express-session';
import { spawn, exec } from 'child_process';
import https from 'https';
import http from 'http';
import { fileURLToPath } from 'url';  // Add this import

// Create __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
dotenv.config();

// JSON file paths (now __dirname works)
const DATA_FILE = path.join(__dirname, 'channels.json');
const CACHE_FILE = path.join(__dirname, '..', 'streams_cache.json');

/* ================= STABLE ID GENERATION ================= */
function generateStableId(streamData) {
    const str = `${streamData.name}|${streamData.source}`;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return `item_${Math.abs(hash).toString(16).substring(0, 8)}`;
}

// External API configuration
const EXTERNAL_API_URL = process.env.EXTERNAL_API_URL || 'https://ani-box-nine.vercel.app/api/grok-chat';

// Script management
let managedProcess = null;

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// Session configuration
app.use(session({
    secret: process.env.SESSION_SECRET || 'your-secret-key-change-this',
    resave: false,
    saveUninitialized: false,
    cookie: {
        secure: false, // Set to true if using HTTPS
        httpOnly: true,
        maxAge: 24 * 60 * 60 * 1000 // 24 hours
    }
}));

// Authentication endpoint
app.post('/api/login', (req, res) => {
    const { username, password } = req.body;

    // Get credentials from environment variables
    const validUsername = process.env.ADMIN_USERNAME;
    const validPassword = process.env.ADMIN_PASSWORD;

    if (!validUsername || !validPassword) {
        return res.status(500).json({
            success: false,
            message: 'Server configuration error'
        });
    }

    if (username === validUsername && password === validPassword) {
        req.session.authenticated = true;
        req.session.username = username;
        return res.json({
            success: true,
            message: 'Login successful'
        });
    } else {
        return res.status(401).json({
            success: false,
            message: 'Invalid username or password'
        });
    }
});

// Logout endpoint
app.post('/api/logout', (req, res) => {
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({
                success: false,
                message: 'Logout failed'
            });
        }
        res.json({
            success: true,
            message: 'Logged out successfully'
        });
    });
});

// Check authentication status
app.get('/api/auth/status', (req, res) => {
    res.json({
        authenticated: req.session.authenticated === true
    });
});

// Helper function to read channels from JSON file
function readChannels() {
    try {
        if (fs.existsSync(DATA_FILE)) {
            const data = fs.readFileSync(DATA_FILE, 'utf8');
            return JSON.parse(data);
        }
        return [];
    } catch (error) {
        console.error('Error reading channels file:', error);
        return [];
    }
}

// Helper function to write channels to JSON file
function writeChannels(channels) {
    try {
        fs.writeFileSync(DATA_FILE, JSON.stringify(channels, null, 2), 'utf8');
        return true;
    } catch (error) {
        console.error('Error writing channels file:', error);
        return false;
    }
}

// Get all channels
app.get('/api/channels', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const channels = readChannels();

    // Load cache to get DASH URLs
    let cache = {};
    try {
        if (fs.existsSync(CACHE_FILE)) {
            cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
        }
    } catch (e) {
        console.error('Error reading cache file:', e);
    }

    // Attach DASH URLs to channels
    const enhancedChannels = channels.map(channel => {
        const stableId = generateStableId({
            name: channel.channelName,
            source: channel.channelSource
        });
        return {
            ...channel,
            dashUrl: cache[stableId] ? cache[stableId].dash : null
        };
    });

    res.json({ success: true, data: enhancedChannels });
});

// Add new channel
app.post('/api/channels', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const { pageToken, channelName, channelSource, imageUrl } = req.body;

    if (!pageToken || !channelName || !channelSource || !imageUrl) {
        return res.status(400).json({ success: false, message: 'All fields are required' });
    }

    const channels = readChannels();
    const newChannel = {
        id: Date.now(),
        pageToken,
        channelName,
        channelSource,
        imageUrl,
        createdAt: new Date().toISOString()
    };

    channels.push(newChannel);

    if (writeChannels(channels)) {
        res.json({ success: true, data: newChannel });
    } else {
        res.status(500).json({ success: false, message: 'Failed to save channel' });
    }
});

// Update channel
app.put('/api/channels/:id', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const channelId = parseInt(req.params.id);
    const { pageToken, channelName, channelSource, imageUrl } = req.body;

    if (!pageToken || !channelName || !channelSource || !imageUrl) {
        return res.status(400).json({ success: false, message: 'All fields are required' });
    }

    const channels = readChannels();
    const index = channels.findIndex(c => c.id === channelId);

    if (index === -1) {
        return res.status(404).json({ success: false, message: 'Channel not found' });
    }

    channels[index] = {
        ...channels[index],
        pageToken,
        channelName,
        channelSource,
        imageUrl,
        updatedAt: new Date().toISOString()
    };

    if (writeChannels(channels)) {
        res.json({ success: true, data: channels[index] });
    } else {
        res.status(500).json({ success: false, message: 'Failed to update channel' });
    }
});

// Delete channel
app.delete('/api/channels/:id', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const channelId = parseInt(req.params.id);
    const channels = readChannels();
    const index = channels.findIndex(c => c.id === channelId);

    if (index === -1) {
        return res.status(404).json({ success: false, message: 'Channel not found' });
    }

    channels.splice(index, 1);

    if (writeChannels(channels)) {
        res.json({ success: true, message: 'Channel deleted' });
    } else {
        res.status(500).json({ success: false, message: 'Failed to delete channel' });
    }
});

// Helper to get PM2 status
function getPM2Status() {
    return new Promise((resolve) => {
        exec('pm2 jlist', (error, stdout) => {
            if (error) {
                return resolve({ running: false, status: 'not_found', message: 'PM2 error' });
            }
            try {
                const processes = JSON.parse(stdout);
                const botProcess = processes.find(p => p.name === 'ChatBot');
                if (botProcess) {
                    const status = botProcess.pm2_env.status;
                    // PM2 statuses: online, stopping, stopped, launching, errored, one-launch-status
                    return resolve({ running: status === 'online', status: status });
                }
                return resolve({ running: false, status: 'not_found', message: 'Process not found' });
            } catch (e) {
                return resolve({ running: false, status: 'error', message: 'Error parsing PM2 output' });
            }
        });
    });
}

// Get script status
app.get('/api/script/status', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const result = await getPM2Status();
    res.json({ success: true, ...result });
});

// Start script
app.post('/api/script/start', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const current = await getPM2Status();
    // PM2 statuses: online, stopping, stopped, launching, errored, one-launch-status
    if (current.running || current.status === 'launching') {
        return res.status(400).json({ success: false, message: 'السكربت يعمل بالفعل أو جاري التشغيل' });
    }

    const rawScriptPath = process.env.MANAGED_SCRIPT_PATH || '../ChaBot.js';
    const scriptPath = path.resolve(__dirname, rawScriptPath);

    // Check if pm2 is installed
    exec('command -v pm2', (pm2Error) => {
        if (pm2Error) {
            console.log('PM2 not found. Attempting to install...');
            exec('npm install -g pm2', (installError) => {
                if (installError) {
                    return res.status(500).json({ success: false, message: 'PM2 not found and installation failed' });
                }
                proceedWithStart();
            });
        } else {
            proceedWithStart();
        }
    });

    function proceedWithStart() {
        if (current.status === 'not_found') {
            exec(`pm2 start ${scriptPath} --name ChatBot`, (err, stdout) => {
                if (err) {
                    return res.status(500).json({ success: false, message: err.message });
                }
                res.json({ success: true, message: 'Script initialized and started via PM2' });
            });
        } else {
            exec('pm2 start ChatBot', (err, stdout) => {
                if (err) {
                    return res.status(500).json({ success: false, message: err.message });
                }
                res.json({ success: true, message: 'Script started via PM2' });
            });
        }
    }
});

// Stop script
app.post('/api/script/stop', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const current = await getPM2Status();
    if (current.status === 'not_found') {
        return res.status(400).json({ success: false, message: 'السكربت غير موجود ليتم إيقافه' });
    }

    if (!current.running && current.status !== 'errored' && current.status !== 'launching') {
        return res.status(400).json({ success: false, message: 'السكربت متوقف بالفعل' });
    }

    exec('pm2 stop ChatBot', (error, stdout, stderr) => {
        if (error) {
            return res.status(500).json({ success: false, message: error.message });
        }
        res.json({ success: true, message: 'Script stopped via PM2' });
    });
});

// Restart script
app.post('/api/script/restart', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const current = await getPM2Status();

    const rawScriptPath = process.env.MANAGED_SCRIPT_PATH || '../ChaBot.js';
    const scriptPath = path.resolve(__dirname, rawScriptPath);

    if (current.status === 'not_found') {
        // If not found, start it (fulfill "if not, start it")
        exec(`pm2 start ${scriptPath} --name ChatBot`, (err, stdout) => {
            if (err) {
                return res.status(500).json({ success: false, message: err.message });
            }
            res.json({ success: true, message: 'Script initialized and started via PM2' });
        });
    } else {
        // If exists (running or stopped), restart it
        // pm2 restart will start it if it's stopped
        exec('pm2 restart ChatBot', (error, stdout, stderr) => {
            if (error) {
                return res.status(500).json({ success: false, message: error.message });
            }
            res.json({ success: true, message: 'Script restarted via PM2' });
        });
    }
});

// Get logs
app.get('/api/script/logs', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    // Fetch last 15 lines of PM2 logs
    // Using --nostream and --raw to get clean output
    exec('pm2 logs ChatBot --lines 15 --nostream --raw', (error, stdout, stderr) => {
        const combined = (stdout || '') + (stderr || '');
        const timestamp = new Date().toISOString();

        if (!combined.trim()) {
            return res.json({
                success: true,
                logs: [{ timestamp, message: 'لا توجد سجلات حالياً', type: 'stdout' }]
            });
        }

        // Split by newline and filter empty lines
        const lines = combined.split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);

        const logs = lines.map(line => ({
            timestamp,
            message: line,
            type: 'stdout'
        }));

        res.json({ success: true, logs });
    });
});

// Clear logs
app.post('/api/script/logs/clear', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    exec('pm2 flush ChatBot', (error) => {
        if (error) {
            return res.status(500).json({ success: false, message: 'Failed to clear logs' });
        }
        res.json({ success: true, message: 'Logs cleared' });
    });
});

// Get status report from file
app.get('/api/script/status-report', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    const statusFile = path.join(__dirname, 'status.txt');
    if (fs.existsSync(statusFile)) {
        try {
            const report = fs.readFileSync(statusFile, 'utf8');
            res.json({ success: true, report });
        } catch (error) {
            res.status(500).json({ success: false, message: 'Error reading status file' });
        }
    } else {
        res.json({ success: true, report: 'في انتظار بيانات الحالة...' });
    }
});


// Root route - redirect to login
app.get('/', (req, res) => {
    if (req.session.authenticated) {
        res.redirect('/dashboard.html');
    } else {
        res.redirect('/login.html');
    }
});

// Protect dashboard route
app.get('/dashboard.html', (req, res, next) => {
    if (!req.session.authenticated) {
        return res.redirect('/login.html');
    }
    next();
});

// Allow login page and static files
app.get('*', (req, res, next) => {
    // Allow access to login page, API routes, and static files
    if (req.path === '/login.html' || req.path.startsWith('/api/') || req.path.includes('.')) {
        return next();
    }
    next();
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log('Make sure your .env file is configured with ADMIN_USERNAME and ADMIN_PASSWORD');
});
