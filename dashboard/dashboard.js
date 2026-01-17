// API Base URL
const API_BASE = window.location.origin;

// State management
let entries = [];
let editingId = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    initializeEventListeners();
    loadEntries();
    initializeScriptManagement();
    startLogsPolling();
});

// Authentication check
async function checkAuth() {
    try {
        const response = await fetch(`${API_BASE}/api/auth/status`, {
            credentials: 'include'
        });
        const data = await response.json();
        
        if (!data.authenticated) {
            window.location.href = '/login.html';
        }
    } catch (error) {
        console.error('Auth check failed:', error);
        window.location.href = '/login.html';
    }
}

// Initialize event listeners
function initializeEventListeners() {
    // Logout button
    document.getElementById('logoutBtn').addEventListener('click', handleLogout);
    
    // Entry form
    document.getElementById('entryForm').addEventListener('submit', handleFormSubmit);
    document.getElementById('cancelEditBtn').addEventListener('click', cancelEdit);
    
    // Script management buttons
    document.getElementById('startScriptBtn').addEventListener('click', startScript);
    document.getElementById('stopScriptBtn').addEventListener('click', stopScript);
    document.getElementById('restartScriptBtn').addEventListener('click', restartScript);
    document.getElementById('clearLogsBtn').addEventListener('click', clearLogs);
    
    // Dark mode toggle
    document.getElementById('darkModeToggle').addEventListener('click', toggleDarkMode);
    
    // Initialize dark mode from localStorage
    initializeDarkMode();
}

// Logout handler
async function handleLogout() {
    try {
        const response = await fetch(`${API_BASE}/api/logout`, {
            method: 'POST',
            credentials: 'include'
        });
        
        const data = await response.json();
        
        if (data.success) {
            window.location.href = '/login.html';
        }
    } catch (error) {
        console.error('Logout error:', error);
        window.location.href = '/login.html';
    }
}

// Load entries from server
async function loadEntries() {
    try {
        const response = await fetch(`${API_BASE}/api/channels`, {
            credentials: 'include'
        });
        
        const data = await response.json();
        
        if (data.success) {
            entries = data.data;
            renderEntries();
        } else {
            showToast('فشل تحميل القنوات', 'error');
        }
    } catch (error) {
        console.error('Error loading entries:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

// Handle form submit
async function handleFormSubmit(e) {
    e.preventDefault();
    
    const formData = {
        pageToken: document.getElementById('pageToken').value.trim(),
        channelName: document.getElementById('channelName').value.trim(),
        channelSource: document.getElementById('channelSource').value.trim(),
        imageUrl: document.getElementById('imageUrl').value.trim()
    };
    
    // Validation
    if (!formData.pageToken || !formData.channelName || !formData.channelSource || !formData.imageUrl) {
        showToast('يرجى ملء جميع الحقول', 'error');
        return;
    }
    
    try {
        let response;
        
        if (editingId !== null) {
            // Update existing entry
            response = await fetch(`${API_BASE}/api/channels/${editingId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify(formData)
            });
        } else {
            // Add new entry
            response = await fetch(`${API_BASE}/api/channels`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                credentials: 'include',
                body: JSON.stringify(formData)
            });
        }
        
        const data = await response.json();
        
        if (data.success) {
            showToast(editingId ? 'تم تحديث القناة بنجاح' : 'تم إضافة القناة بنجاح', 'success');
            document.getElementById('entryForm').reset();
            cancelEdit();
            loadEntries();
        } else {
            showToast(data.message || 'فشلت العملية', 'error');
        }
    } catch (error) {
        console.error('Error saving entry:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

// Cancel edit
function cancelEdit() {
    editingId = null;
    document.getElementById('entryForm').reset();
    document.getElementById('submitBtn').textContent = 'إضافة';
    document.getElementById('cancelEditBtn').style.display = 'none';
}

// Edit entry
function editEntry(id) {
    const entry = entries.find(e => e.id === id);
    if (!entry) return;
    
    editingId = id;
    document.getElementById('pageToken').value = entry.pageToken;
    document.getElementById('channelName').value = entry.channelName;
    document.getElementById('channelSource').value = entry.channelSource;
    document.getElementById('imageUrl').value = entry.imageUrl;
    
    document.getElementById('submitBtn').textContent = 'تحديث';
    document.getElementById('cancelEditBtn').style.display = 'inline-flex';
    
    // Scroll to form
    document.querySelector('.form-section').scrollIntoView({ behavior: 'smooth' });
}

// Delete entry
async function deleteEntry(id) {
    if (!confirm('هل أنت متأكد من حذف هذه القناة؟')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/api/channels/${id}`, {
            method: 'DELETE',
            credentials: 'include'
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('تم حذف القناة بنجاح', 'success');
            loadEntries();
        } else {
            showToast(data.message || 'فشل حذف القناة', 'error');
        }
    } catch (error) {
        console.error('Error deleting entry:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

// Render entries
function renderEntries() {
    const entriesList = document.getElementById('entriesList');
    
    if (entries.length === 0) {
        entriesList.innerHTML = '<p class="empty-message">لا توجد قنوات مسجلة بعد</p>';
        return;
    }
    
    // Sort entries: newest first (by updatedAt or createdAt)
    const sortedEntries = [...entries].sort((a, b) => {
        const dateA = new Date(a.updatedAt || a.createdAt || 0);
        const dateB = new Date(b.updatedAt || b.createdAt || 0);
        return dateB - dateA; // Descending order (newest first)
    });
    
    entriesList.innerHTML = sortedEntries.map(entry => `
        <div class="entry-card">
            <div class="entry-header">
                <div class="entry-title">${escapeHtml(entry.channelName)}</div>
                <div class="entry-actions">
                    ${entry.dashUrl ? `<button class="btn btn-primary" onclick="copyStreamUrl('${escapeHtml(entry.dashUrl)}')">نسخ رابط البث</button>` : ''}
                    <button class="btn btn-success" onclick="editEntry(${entry.id})">تعديل</button>
                    <button class="btn btn-danger" onclick="deleteEntry(${entry.id})">حذف</button>
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

// Dark Mode Functions
function initializeDarkMode() {
    const isDark = localStorage.getItem('darkMode') === 'true';
    if (isDark) {
        document.body.classList.add('dark-mode');
        updateDarkModeIcon(true);
    }
}

function toggleDarkMode() {
    const isDark = document.body.classList.toggle('dark-mode');
    localStorage.setItem('darkMode', isDark.toString());
    updateDarkModeIcon(isDark);
}

function updateDarkModeIcon(isDark) {
    const icon = document.getElementById('darkModeToggle');
    icon.textContent = isDark ? '☀️' : '🌙';
}

// Utility functions
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showToast(message, type = 'success') {
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

// Script Management Functions
let logsPollingInterval = null;
let statusPollingInterval = null;

function initializeScriptManagement() {
    checkScriptStatus();
    loadStatusReport();
    // Check status every 5 seconds
    setInterval(checkScriptStatus, 5000);
    // Poll status report every 10 seconds
    statusPollingInterval = setInterval(loadStatusReport, 10000);
}

async function checkScriptStatus() {
    try {
        const response = await fetch(`${API_BASE}/api/script/status`, {
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            const statusBadge = document.getElementById('scriptStatus');
            const status = data.status; // Get raw PM2 status

            if (status === 'online') {
                statusBadge.textContent = 'يعمل';
                statusBadge.className = 'status-badge status-running';
            } else if (status === 'stopping') {
                statusBadge.textContent = 'جاري الإيقاف';
                statusBadge.className = 'status-badge status-stopped';
            } else if (status === 'stopped') {
                statusBadge.textContent = 'متوقف';
                statusBadge.className = 'status-badge status-stopped';
            } else if (status === 'errored') {
                statusBadge.textContent = 'خطأ في التشغيل';
                statusBadge.className = 'status-badge status-stopped';
            } else if (status === 'launching' || status === 'one-launch-status') {
                statusBadge.textContent = 'جاري التشغيل';
                statusBadge.className = 'status-badge status-running';
            } else if (status === 'not_found' || data.message === 'Process not found' || data.message === 'PM2 or process not found') {
                statusBadge.textContent = 'غير موجود / متوقف';
                statusBadge.className = 'status-badge status-stopped';
            } else {
                statusBadge.textContent = 'غير معروف';
                statusBadge.className = 'status-badge status-unknown';
            }
        }
    } catch (error) {
        console.error('Error checking script status:', error);
    }
}

async function startScript() {
    try {
        const response = await fetch(`${API_BASE}/api/script/start`, {
            method: 'POST',
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('تم بدء السكربت بنجاح', 'success');
            checkScriptStatus();
        } else {
            showToast(data.message || 'فشل بدء السكربت', 'error');
        }
    } catch (error) {
        console.error('Error starting script:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

async function stopScript() {
    if (!confirm('هل أنت متأكد من إيقاف السكربت؟')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/api/script/stop`, {
            method: 'POST',
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('تم إيقاف السكربت بنجاح', 'success');
            checkScriptStatus();
        } else {
            showToast(data.message || 'فشل إيقاف السكربت', 'error');
        }
    } catch (error) {
        console.error('Error stopping script:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

async function restartScript() {
    if (!confirm('هل أنت متأكد من إعادة تشغيل السكربت؟')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/api/script/restart`, {
            method: 'POST',
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('تم إعادة تشغيل السكربت بنجاح', 'success');
            checkScriptStatus();
        } else {
            showToast(data.message || 'فشل إعادة تشغيل السكربت', 'error');
        }
    } catch (error) {
        console.error('Error restarting script:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

function startLogsPolling() {
    loadLogs();
    // Poll logs every 1 minute
    if (logsPollingInterval) {
        clearInterval(logsPollingInterval);
    }
    logsPollingInterval = setInterval(loadLogs, 60000);
}

async function loadLogs() {
    try {
        const response = await fetch(`${API_BASE}/api/script/logs?limit=100`, {
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            renderLogs(data.logs);
        }
    } catch (error) {
        console.error('Error loading logs:', error);
    }
}

async function loadStatusReport() {
    try {
        const response = await fetch(`${API_BASE}/api/script/status-report`, {
            credentials: 'include'
        });
        const data = await response.json();

        if (data.success) {
            const statusContainer = document.getElementById('statusReport');
            if (data.report) {
                // Replace newlines with <br> for display
                statusContainer.innerHTML = data.report.replace(/\n/g, '<br>');
            }
        }
    } catch (error) {
        console.error('Error loading status report:', error);
    }
}

function renderLogs(logs) {
    const logsContainer = document.getElementById('logsContainer');
    const wasScrolledToBottom = logsContainer.scrollHeight - logsContainer.scrollTop <= logsContainer.clientHeight + 50;
    
    if (logs.length === 0) {
        logsContainer.innerHTML = '<div class="log-entry">لا توجد سجلات</div>';
        return;
    }
    
    logsContainer.innerHTML = logs.map(log => {
        const time = new Date(log.timestamp).toLocaleTimeString('ar-SA');
        const logClass = `log-entry log-${log.type}`;
        // Replace newlines with <br> to preserve formatting in HTML
        const messageWithBr = log.message.replace(/\n/g, '<br>');
        return `<div class="${logClass}">
            <span class="log-time">[${time}]</span>
            <span class="log-message">${messageWithBr}</span>
        </div>`;
    }).join('');
    
    // Auto scroll to bottom only if user was already at bottom
    if (wasScrolledToBottom) {
        requestAnimationFrame(() => {
            logsContainer.scrollTop = logsContainer.scrollHeight;
        });
    }
}

async function clearLogs() {
    if (!confirm('هل أنت متأكد من مسح جميع السجلات؟')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/api/script/logs/clear`, {
            method: 'POST',
            credentials: 'include'
        });
        const data = await response.json();
        
        if (data.success) {
            showToast('تم مسح السجلات بنجاح', 'success');
            loadLogs();
        } else {
            showToast('فشل مسح السجلات', 'error');
        }
    } catch (error) {
        console.error('Error clearing logs:', error);
        showToast('خطأ في الاتصال بالخادم', 'error');
    }
}

// Copy Stream URL
function copyStreamUrl(url) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(() => {
            showToast('تم نسخ رابط DASH بنجاح', 'success');
        }).catch(err => {
            fallbackCopyTextToClipboard(url);
        });
    } else {
        fallbackCopyTextToClipboard(url);
    }
}

function fallbackCopyTextToClipboard(text) {
    var textArea = document.createElement("textarea");
    textArea.value = text;

    // Avoid scrolling to bottom
    textArea.style.top = "0";
    textArea.style.left = "0";
    textArea.style.position = "fixed";
    textArea.style.opacity = "0";

    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        var successful = document.execCommand('copy');
        if (successful) {
            showToast('تم نسخ رابط DASH بنجاح', 'success');
        } else {
            showToast('فشل نسخ الرابط', 'error');
        }
    } catch (err) {
        showToast('فشل نسخ الرابط', 'error');
    }

    document.body.removeChild(textArea);
}

// Make functions available globally
window.editEntry = editEntry;
window.deleteEntry = deleteEntry;
window.copyStreamUrl = copyStreamUrl;
