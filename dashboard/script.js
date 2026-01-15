// API Base URL
const API_BASE = window.location.origin;

// State management
let entries = [];
let editingIndex = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    initializeEventListeners();
    loadEntries();
});

// Authentication
async function checkAuth() {
    try {
        const response = await fetch(`${API_BASE}/api/auth/status`);
        const data = await response.json();
        
        if (data.authenticated) {
            showDashboard();
        } else {
            showLogin();
        }
    } catch (error) {
        console.error('Auth check failed:', error);
        showLogin();
    }
}

function showLogin() {
    document.getElementById('loginPage').classList.add('active');
    document.getElementById('dashboardContainer').classList.add('hidden');
}

function showDashboard() {
    document.getElementById('loginPage').classList.remove('active');
    document.getElementById('dashboardContainer').classList.remove('hidden');
    renderEntries();
}

// Login form handler
function initializeEventListeners() {
    // Login form
    document.getElementById('loginForm').addEventListener('submit', handleLogin);
    
    // Logout button
    document.getElementById('logoutBtn').addEventListener('click', handleLogout);
    
    // Navigation buttons
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const page = e.target.dataset.page;
            if (page) {
                switchPage(page);
            }
        });
    });
    
    // Entry form
    document.getElementById('entryForm').addEventListener('submit', handleFormSubmit);
    document.getElementById('cancelEditBtn').addEventListener('click', cancelEdit);
    
    // Copy JSON button
    document.getElementById('copyJsonBtn').addEventListener('click', copyJson);
}

async function handleLogin(e) {
    e.preventDefault();
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const errorDiv = document.getElementById('loginError');
    const submitBtn = e.target.querySelector('button[type="submit"]');
    
    // Disable button during request
    submitBtn.disabled = true;
    submitBtn.textContent = 'Logging in...';
    errorDiv.textContent = '';
    
    try {
        const response = await fetch(`${API_BASE}/api/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            credentials: 'include', // Important for session cookies
            body: JSON.stringify({ username, password })
        });
        
        const data = await response.json();
        
        if (data.success) {
            errorDiv.textContent = '';
            showDashboard();
            showToast('Login successful!', 'success');
        } else {
            errorDiv.textContent = data.message || 'Invalid username or password';
            showToast('Login failed. Please check your credentials.', 'error');
        }
    } catch (error) {
        console.error('Login error:', error);
        errorDiv.textContent = 'Connection error. Please try again.';
        showToast('Connection error. Please try again.', 'error');
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Login';
    }
}

async function handleLogout() {
    try {
        const response = await fetch(`${API_BASE}/api/logout`, {
            method: 'POST',
            credentials: 'include'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showLogin();
            document.getElementById('loginForm').reset();
            showToast('Logged out successfully', 'success');
        } else {
            showToast('Logout failed', 'error');
        }
    } catch (error) {
        console.error('Logout error:', error);
        // Still show login page even if logout request fails
        showLogin();
        document.getElementById('loginForm').reset();
    }
}

// Navigation
function switchPage(pageId) {
    // Update active page
    document.querySelectorAll('.page').forEach(page => {
        page.classList.remove('active');
    });
    document.getElementById(pageId).classList.add('active');
    
    // Update active nav button
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-page="${pageId}"]`).classList.add('active');
    
    // Render entries when switching to page 2
    if (pageId === 'page2') {
        renderEntries();
    }
}

// Entry management
function handleFormSubmit(e) {
    e.preventDefault();
    
    const formData = {
        pageToken: document.getElementById('pageToken').value,
        channelName: document.getElementById('channelName').value,
        channelSource: document.getElementById('channelSource').value,
        imageUrl: document.getElementById('imageUrl').value,
        id: Date.now() // Simple ID generation
    };
    
    if (editingIndex !== null) {
        // Update existing entry
        entries[editingIndex] = formData;
        showToast('Entry updated successfully!', 'success');
        cancelEdit();
    } else {
        // Add new entry
        entries.push(formData);
        showToast('Entry added successfully!', 'success');
    }
    
    saveEntries();
    document.getElementById('entryForm').reset();
    
    // Switch to page 2 to show the updated list
    switchPage('page2');
}

function cancelEdit() {
    editingIndex = null;
    document.getElementById('entryForm').reset();
    document.getElementById('submitBtn').textContent = 'Add Entry';
    document.getElementById('cancelEditBtn').style.display = 'none';
    document.querySelector('#page1 h2').textContent = 'Add New Entry';
}

function editEntry(index) {
    editingIndex = index;
    const entry = entries[index];
    
    document.getElementById('pageToken').value = entry.pageToken;
    document.getElementById('channelName').value = entry.channelName;
    document.getElementById('channelSource').value = entry.channelSource;
    document.getElementById('imageUrl').value = entry.imageUrl;
    
    document.getElementById('submitBtn').textContent = 'Update Entry';
    document.getElementById('cancelEditBtn').style.display = 'inline-flex';
    document.querySelector('#page1 h2').textContent = 'Edit Entry';
    
    switchPage('page1');
    document.getElementById('pageToken').focus();
}

function deleteEntry(index) {
    if (confirm('Are you sure you want to delete this entry?')) {
        entries.splice(index, 1);
        saveEntries();
        renderEntries();
        showToast('Entry deleted successfully!', 'success');
    }
}

function renderEntries() {
    const entriesList = document.getElementById('entriesList');
    
    if (entries.length === 0) {
        entriesList.innerHTML = '<p class="empty-message">No entries yet. Add some entries from the "Add Entry" page.</p>';
        return;
    }
    
    entriesList.innerHTML = entries.map((entry, index) => `
        <div class="entry-card">
            <div class="entry-header">
                <div>
                    <div class="entry-title">${escapeHtml(entry.channelName)}</div>
                </div>
                <div class="entry-actions">
                    <button class="btn btn-success" onclick="editEntry(${index})">Edit</button>
                    <button class="btn btn-danger" onclick="deleteEntry(${index})">Delete</button>
                </div>
            </div>
            <div class="entry-details">
                <div class="entry-detail">
                    <span class="entry-detail-label">Page Token</span>
                    <span class="entry-detail-value">${escapeHtml(entry.pageToken)}</span>
                </div>
                <div class="entry-detail">
                    <span class="entry-detail-label">Channel Source</span>
                    <span class="entry-detail-value">${escapeHtml(entry.channelSource)}</span>
                </div>
                <div class="entry-detail">
                    <span class="entry-detail-label">Image URL</span>
                    <span class="entry-detail-value">${escapeHtml(entry.imageUrl)}</span>
                </div>
                ${entry.imageUrl ? `
                    <div class="entry-image">
                        <img src="${escapeHtml(entry.imageUrl)}" alt="Channel Image" onerror="this.style.display='none'">
                    </div>
                ` : ''}
            </div>
        </div>
    `).join('');
}

// JSON Export
function copyJson() {
    const jsonString = JSON.stringify(entries, null, 2);
    
    navigator.clipboard.writeText(jsonString).then(() => {
        showToast('JSON copied to clipboard!', 'success');
    }).catch(err => {
        // Fallback for older browsers
        const textarea = document.createElement('textarea');
        textarea.value = jsonString;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        try {
            document.execCommand('copy');
            showToast('JSON copied to clipboard!', 'success');
        } catch (err) {
            showToast('Failed to copy JSON. Please try again.', 'error');
        }
        document.body.removeChild(textarea);
    });
}

// Local Storage
function saveEntries() {
    localStorage.setItem('dashboardEntries', JSON.stringify(entries));
}

function loadEntries() {
    const saved = localStorage.getItem('dashboardEntries');
    if (saved) {
        try {
            entries = JSON.parse(saved);
        } catch (e) {
            console.error('Error loading entries:', e);
            entries = [];
        }
    }
}

// Utility functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showToast(message, type = 'success') {
    // Remove existing toast
    const existingToast = document.querySelector('.toast');
    if (existingToast) {
        existingToast.remove();
    }
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'slideIn 0.3s ease-out reverse';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Make functions available globally for onclick handlers
window.editEntry = editEntry;
window.deleteEntry = deleteEntry;

