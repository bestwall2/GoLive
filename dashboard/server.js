const express = require('express');
const path = require('path');
const fs = require('fs');
const dotenv = require('dotenv');
const session = require('express-session');
const { spawn, exec } = require('child_process');
const https = require('https');
const http = require('http');

// Load environment variables
dotenv.config();

// JSON file path
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
let scriptLogs = [];
const MAX_LOGS = 1000;

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

// Helper function to add log
function addLog(message, type = 'info') {
    const timestamp = new Date().toISOString();
    scriptLogs.push({ timestamp, message, type });
    if (scriptLogs.length > MAX_LOGS) {
        scriptLogs.shift();
    }
}

// Helper to get PM2 status
function getPM2Status() {
    return new Promise((resolve) => {
        exec('pm2 show ChatBot --json', (error, stdout) => {
            if (error) {
                return resolve({ running: false, status: 'not_found', message: 'PM2 or process not found' });
            }
            try {
                const data = JSON.parse(stdout);
                if (data.length > 0) {
                    const status = data[0].pm2_env.status;
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
    if (current.running) {
        return res.status(400).json({ success: false, message: 'السكربت يعمل بالفعل' });
    }

    const scriptPath = process.env.MANAGED_SCRIPT_PATH || '../ChaBot.js';

    // Check if pm2 is installed
    exec('command -v pm2', (pm2Error) => {
        if (pm2Error) {
            addLog('PM2 not found. Attempting to install...', 'warning');
            exec('npm install -g pm2', (installError) => {
                if (installError) {
                    addLog(`Failed to install PM2: ${installError.message}`, 'error');
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
            addLog('Starting new ChatBot process with PM2...', 'info');
            exec(`pm2 start ${scriptPath} --name ChatBot`, (err, stdout) => {
                if (err) {
                    addLog(`PM2 Start Error: ${err.message}`, 'error');
                    return res.status(500).json({ success: false, message: err.message });
                }
                res.json({ success: true, message: 'Script initialized and started via PM2' });
            });
        } else {
            addLog('Starting existing ChatBot process...', 'info');
            exec('pm2 start ChatBot', (err, stdout) => {
                if (err) {
                    addLog(`PM2 Start Error: ${err.message}`, 'error');
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
    if (!current.running && current.status !== 'errored') {
        return res.status(400).json({ success: false, message: 'السكربت متوقف بالفعل' });
    }

    addLog('Stopping script with PM2...', 'info');
    exec('pm2 stop ChatBot', (error, stdout, stderr) => {
        if (error) {
            addLog(`PM2 Stop Error: ${error.message}`, 'error');
            return res.status(500).json({ success: false, message: error.message });
        }
        addLog(`PM2 Stop Output: ${stdout}`, 'info');
        res.json({ success: true, message: 'Script stopped via PM2' });
    });
});

// Restart script
app.post('/api/script/restart', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    const current = await getPM2Status();
    if (current.status === 'not_found') {
        return res.status(400).json({ success: false, message: 'السكربت غير موجود للبدء، يرجى الضغط على تشغيل أولاً' });
    }

    addLog('Restarting script with PM2...', 'info');
    exec('pm2 restart ChatBot', (error, stdout, stderr) => {
        if (error) {
            addLog(`PM2 Restart Error: ${error.message}`, 'error');
            return res.status(500).json({ success: false, message: error.message });
        }
        addLog(`PM2 Restart Output: ${stdout}`, 'info');
        res.json({ success: true, message: 'Script restarted via PM2' });
    });
});

// Get logs
app.get('/api/script/logs', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    // Fetch last 15 lines of PM2 logs
    exec('pm2 logs ChatBot --lines 15 --nostream', (error, stdout, stderr) => {
        // PM2 logs command output might contain ANSI colors, but we'll return it as is
        // Dashboard replaces \n with <br> already.

        const timestamp = new Date().toISOString();
        const logs = [{
            timestamp,
            message: stdout || stderr || 'No logs available',
            type: error ? 'error' : 'stdout'
        }];

        res.json({ success: true, logs });
    });
});

// Clear logs
app.post('/api/script/logs/clear', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }

    scriptLogs = [];
    addLog('Logs cleared', 'info');
    res.json({ success: true, message: 'Logs cleared' });
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
