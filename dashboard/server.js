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
    res.json({ success: true, data: channels });
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

// Get script status
app.get('/api/script/status', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    const isRunning = managedProcess !== null && managedProcess.killed === false;
    res.json({ 
        success: true, 
        running: isRunning,
        pid: isRunning ? managedProcess.pid : null
    });
});

// Start script
app.post('/api/script/start', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    if (managedProcess && managedProcess.killed === false) {
        return res.status(400).json({ success: false, message: 'Script is already running' });
    }
    
    try {
        // Get script path from environment
        const scriptToRun = process.env.MANAGED_SCRIPT_PATH;
        
        if (!scriptToRun) {
            return res.status(400).json({ 
                success: false, 
                message: 'MANAGED_SCRIPT_PATH not configured in .env file' 
            });
        }
        
        // Check if script file exists
        if (!fs.existsSync(scriptToRun)) {
            return res.status(404).json({ 
                success: false, 
                message: `Script file not found: ${scriptToRun}` 
            });
        }
        
        addLog(`Starting script: ${scriptToRun}`, 'info');
        
        managedProcess = spawn('node', [scriptToRun], {
            cwd: __dirname,
            stdio: ['ignore', 'pipe', 'pipe']
        });
        
        managedProcess.stdout.on('data', (data) => {
            const output = data.toString();
            addLog(output, 'stdout');
        });
        
        managedProcess.stderr.on('data', (data) => {
            const output = data.toString();
            addLog(output, 'stderr');
        });
        
        managedProcess.on('exit', (code) => {
            addLog(`Script exited with code ${code}`, code === 0 ? 'info' : 'error');
            managedProcess = null;
        });
        
        managedProcess.on('error', (error) => {
            addLog(`Script error: ${error.message}`, 'error');
            managedProcess = null;
        });
        
        res.json({ success: true, message: 'Script started', pid: managedProcess.pid });
    } catch (error) {
        addLog(`Failed to start script: ${error.message}`, 'error');
        res.status(500).json({ success: false, message: error.message });
    }
});

// Stop script
app.post('/api/script/stop', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    if (!managedProcess || managedProcess.killed) {
        return res.status(400).json({ success: false, message: 'Script is not running' });
    }
    
    try {
        addLog('Stopping script...', 'info');
        managedProcess.kill('SIGTERM');
        
        // Force kill after 5 seconds if still running
        setTimeout(() => {
            if (managedProcess && !managedProcess.killed) {
                managedProcess.kill('SIGKILL');
                addLog('Script force killed', 'warning');
            }
        }, 5000);
        
        res.json({ success: true, message: 'Script stop signal sent' });
    } catch (error) {
        addLog(`Failed to stop script: ${error.message}`, 'error');
        res.status(500).json({ success: false, message: error.message });
    }
});

// Restart script
app.post('/api/script/restart', async (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    try {
        addLog('Restarting script...', 'info');
        
        // Stop if running
        if (managedProcess && !managedProcess.killed) {
            managedProcess.kill('SIGTERM');
            await new Promise(resolve => {
                const checkInterval = setInterval(() => {
                    if (managedProcess.killed || !managedProcess) {
                        clearInterval(checkInterval);
                        resolve();
                    }
                }, 100);
                
                // Timeout after 5 seconds
                setTimeout(() => {
                    clearInterval(checkInterval);
                    if (managedProcess && !managedProcess.killed) {
                        managedProcess.kill('SIGKILL');
                    }
                    resolve();
                }, 5000);
            });
        }
        
        // Wait a bit before starting
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // Start script
        const scriptToRun = process.env.MANAGED_SCRIPT_PATH;
        
        if (!scriptToRun) {
            return res.status(400).json({ 
                success: false, 
                message: 'MANAGED_SCRIPT_PATH not configured in .env file' 
            });
        }
        
        if (!fs.existsSync(scriptToRun)) {
            return res.status(404).json({ 
                success: false, 
                message: `Script file not found: ${scriptToRun}` 
            });
        }
        
        addLog(`Starting script: ${scriptToRun}`, 'info');
        
        managedProcess = spawn('node', [scriptToRun], {
            cwd: __dirname,
            stdio: ['ignore', 'pipe', 'pipe']
        });
        
        managedProcess.stdout.on('data', (data) => {
            const output = data.toString();
            addLog(output, 'stdout');
        });
        
        managedProcess.stderr.on('data', (data) => {
            const output = data.toString();
            addLog(output, 'stderr');
        });
        
        managedProcess.on('exit', (code) => {
            addLog(`Script exited with code ${code}`, code === 0 ? 'info' : 'error');
            managedProcess = null;
        });
        
        managedProcess.on('error', (error) => {
            addLog(`Script error: ${error.message}`, 'error');
            managedProcess = null;
        });
        
        res.json({ success: true, message: 'Script restarted', pid: managedProcess.pid });
    } catch (error) {
        addLog(`Failed to restart script: ${error.message}`, 'error');
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get logs
app.get('/api/script/logs', (req, res) => {
    if (!req.session.authenticated) {
        return res.status(401).json({ success: false, message: 'Unauthorized' });
    }
    
    const limit = parseInt(req.query.limit) || 100;
    const logs = scriptLogs.slice(-limit);
    
    res.json({ success: true, logs });
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

