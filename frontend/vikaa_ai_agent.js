// ----------------------- GLOBAL STATE -----------------------

let chatSessions = [];
let activeSessionIndex = null;
let pendingAttachments = [];  // ✅ REQUIRED: Holds files/images before sending
let canExecute = false;

// Track last sent query for Regenerate button
let _lastUserQuery = "";
let _lastModelType = "gemini";
let _lastStyleType = "balanced";

async function fetchAccessMode() {
  const raw = localStorage.getItem("authData");
  let token = null;
  try { token = raw ? JSON.parse(raw)?.accessToken : null; } catch (_) { token = null; }
  const isLocal =
    typeof vikaaIsTrustedDevFrontend === "function"
      ? vikaaIsTrustedDevFrontend()
      : (() => {
          const h = (window.location.hostname || "").toLowerCase();
          return h === "localhost" || h === "127.0.0.1";
        })();
  if (!token) {
    if (isLocal) return { can_execute: true, mode: "execute", acl_status: "local-dev" };
    return { can_execute: false, mode: "read-only" };
  }
  try {
    const base = (typeof CONFIG !== 'undefined') ? CONFIG.API_BASE_URL : "https://app-wtiw.onrender.com";
    const res = await fetch(`${base}/auth/access-mode`, {
      headers: { "Authorization": `Bearer ${token}` }
    });
    if (!res.ok) {
      if (isLocal) return { can_execute: true, mode: "execute", acl_status: "local-dev" };
      return { can_execute: false, mode: "read-only" };
    }
    return await res.json();
  } catch (_) {
    if (isLocal) return { can_execute: true, mode: "execute", acl_status: "local-dev" };
    return { can_execute: false, mode: "read-only" };
  }
}

function renderAccessBanner(canRun) {
  const legacyBar = document.getElementById("chatAccessBanner");
  if (legacyBar) legacyBar.remove();

  const tag = document.getElementById("chatAccessTag");
  if (!tag) return;
  tag.style.display = "inline-flex";
  tag.style.alignItems = "center";
  if (canRun) {
    tag.className = "header-access-tag execute";
    tag.textContent = "Open for Execution";
  } else {
    tag.className = "header-access-tag readonly";
    tag.textContent = "Cannot Run/Execute. Contact ADMIN for Permission";
  }
}

function applyRunControls(canRun) {
  const ids = ["sendButton", "voiceButton", "cameraButton", "attachmentButton", "user-input"];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.disabled = !canRun;
  });
}

// ============= added on 08-May
function renderPendingAttachments() {
  const container = document.getElementById("pendingAttachmentsPreview");
  container.innerHTML = "";

  pendingAttachments.forEach((att, index) => {
    const div = document.createElement("div");
    div.className = "pending-attachment";

    if (att.dataUrl.startsWith("data:image")) {
      const img = document.createElement("img");
      img.src = att.dataUrl;
      img.style.maxWidth = "30px";
      img.style.maxHeight = "30px";
      img.style.borderRadius = "6px";
      div.appendChild(img);
    }

    const nameSpan = document.createElement("span");
    nameSpan.textContent = att.filename;
    div.appendChild(nameSpan);

    const removeBtn = document.createElement("button");
    removeBtn.textContent = "Remove";
    removeBtn.onclick = () => removePendingAttachment(index);
    div.appendChild(removeBtn);

    container.appendChild(div);
  });

  container.style.display = pendingAttachments.length > 0 ? "flex" : "none";
}

function removePendingAttachment(index) {
  pendingAttachments.splice(index, 1);
  renderPendingAttachments();
}

// -------------------- Load from localStorage on Page Load --------------------
window.addEventListener("load", () => {
  loadChatSessions();

  // Drop any empty sessions left over from previous visits
  const before = chatSessions.length;
  chatSessions = chatSessions.filter(s => s.messages && s.messages.length > 0);
  if (chatSessions.length !== before) saveChatSessions();

  if (chatSessions.length === 0) {
    startNewChat();
  } else {
    renderChatHistory();
    activeSessionIndex = chatSessions.length - 1;
    loadSessionMessages(activeSessionIndex);
  }
});

function saveChatSessions() {
  localStorage.setItem("chatSessions", JSON.stringify(chatSessions));
}

function loadChatSessions() {
  const saved = localStorage.getItem("chatSessions");
  if (saved) {
      chatSessions = JSON.parse(saved);
  }
}

// ----------------------- UTILS -----------------------
function getSelectedModel() {
  const select = document.querySelector('select[name="llm"]');
  return select ? select.value : "gemini";
}

function getSelectedStyle() {
  const select = document.querySelector('select[name="llmStyle"]');
  return select ? select.value : "balanced";
}

function getSessionId() {
  let sessionId = sessionStorage.getItem("agent_session_id");
  if (!sessionId) {
    sessionId = crypto.randomUUID();
    sessionStorage.setItem("agent_session_id", sessionId);
  }
  return sessionId;
}

function clearMessages() {
  document.getElementById("messages").innerHTML = "";
}

// ----------------------- SESSION MANAGEMENT -----------------------
function startNewChat() {
  const timestamp = new Date();
  const title = `📝 New Chat at ${timestamp.getHours()}:${timestamp.getMinutes().toString().padStart(2, '0')}`;

  chatSessions.push({
    startedAt: timestamp,
    title: title,
    messages: []
  });

  activeSessionIndex = chatSessions.length - 1;
  clearMessages();
  showWelcomeScreen();
  renderChatHistory();
  // Don't save yet — persist only when the first message is sent
}

// ----------------------- MESSAGE SENDING -----------------------
async function sendMessage() {
  if (!canExecute) {
    appendMessage("Vikaa.AI", "🚫 Cannot Run/Execute. Contact ADMIN for Permission", "agent");
    return;
  }
  const inputBox = document.getElementById("user-input");
  const message = inputBox.value.trim();
  if (!message) return;

  if (activeSessionIndex === null) startNewChat();

  let combinedContent = '';

  if (pendingAttachments.length > 0) {
      pendingAttachments.forEach(att => {
          if (att.dataUrl.startsWith("data:image")) {
              combinedContent += `<img src="${att.dataUrl}" style="max-width:100px; border-radius:6px; display:block; margin:0 auto 1px;">`;
          } else {
              combinedContent += `📎 ${att.filename}<br>`;
          }
      });
  }

  combinedContent += "<br>" + message;
  const msgDiv = appendMessage("Me", combinedContent, "user", true);

  inputBox.value = "";
  setMsgTick(msgDiv, "sent");

  chatSessions[activeSessionIndex].messages.push({ sender: "user", text: message, timestamp: new Date() });
  saveChatSessions();

  if (chatSessions[activeSessionIndex].messages.length === 1) {
      const trimmed = message.length > 30 ? message.substring(0, 30) + '...' : message;
      chatSessions[activeSessionIndex].title = '📝 ' + trimmed;
      renderChatHistory();
      saveChatSessions();
  }

  const modelType = getSelectedModel();
  const styleType = getSelectedStyle();
  const sessionId = getSessionId();

  let llmStyleValue = 0.6;
  if (styleType === 'creative') {
    llmStyleValue = 0.9;
  } else if (styleType === 'precise') {
    llmStyleValue = 0.2;
  } else {
    llmStyleValue = 0.6;
  }

  const apiUrl = (typeof CONFIG !== 'undefined' ? CONFIG.API_BASE_URL : "https://app-wtiw.onrender.com") + "/agent/message";
  const tokenDataRaw = localStorage.getItem("authData");
  const accessToken = tokenDataRaw ? JSON.parse(tokenDataRaw)?.accessToken : null;
  if (!accessToken) {
    appendMessage("Vikaa.AI", "⚠️ Please login first. Execution requires an authenticated account.", "agent");
    return;
  }

  // Track for Regenerate; snapshot & clear attachments immediately
  _lastUserQuery = message;
  _lastModelType = modelType;
  _lastStyleType = styleType;
  const attachmentsToSend = [...pendingAttachments];
  pendingAttachments = [];
  renderPendingAttachments();

  try {
    const res = await fetch(apiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${accessToken}` },
      body: JSON.stringify({
        session_id: sessionId,
        query: message,
        model: modelType,
        temperature: llmStyleValue,
        attachments: attachmentsToSend,
      }),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const errMsg = typeof data.detail === "string" ? data.detail : `Request failed (${res.status})`;
      appendMessage("Vikaa.AI", `⚠️ ${errMsg}`, "agent");
      return;
    }

    // ── Async job pattern: backend returns job_id immediately ────────────────
    if (data.job_id) {
      const typingDiv = _showTypingIndicator();
      const statusUrl = `${apiUrl}/status/${data.job_id}`;
      let pollCount = 0;
      const poll = setInterval(async () => {
        if (++pollCount > 60) { // 60 × 2.5s = 150s max wait
          clearInterval(poll);
          _removeTypingIndicator(typingDiv);
          appendMessage("Vikaa.AI", "⚠️ Response is taking too long. Please try again.", "agent");
          return;
        }
        try {
          const sr = await fetch(statusUrl, { headers: { "Authorization": `Bearer ${accessToken}` } });
          if (!sr.ok) {
            clearInterval(poll);
            _removeTypingIndicator(typingDiv);
            appendMessage("Vikaa.AI", `⚠️ Status check failed (${sr.status})`, "agent");
            return;
          }
          const sd = await sr.json();
          if (sd.status === "done" || sd.status === "error") {
            clearInterval(poll);
            _removeTypingIndicator(typingDiv);
            _deliverAgentReply(sd.response ?? "No response from agent.", modelType, styleType, msgDiv);
          }
        } catch (_) {
          clearInterval(poll);
          _removeTypingIndicator(typingDiv);
          appendMessage("Vikaa.AI", "⚠️ Failed to check response status.", "agent");
        }
      }, 2500);
      return;
    }

    // ── Fallback: synchronous response (backward compat) ────────────────────
    _deliverAgentReply(data.response ?? "No response from agent.", modelType, styleType, msgDiv);

  } catch (error) {
    appendMessage("Vikaa.AI", "⚠️ **Failed to reach Agent**. API Call Error in **sendMessage()** catch", "agent");
  }
}

// ----------------------- MESSAGE UI -----------------------
/** Strip trailing " — [model/style]" from agent replies (shown in meta row instead). */
function stripAgentMetaSuffix(raw) {
    const s = String(raw);
    const m = s.match(/\s+—\s+\[([^\]]+)\]\s*$/);
    if (!m) return { body: s, tag: "" };
    return { body: s.slice(0, m.index).trimEnd(), tag: m[1] };
}

function formatMessageTime(d) {
    return d.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
    });
}

/** @param when Optional Date (or ISO string) for meta time — used when replaying saved sessions */
function appendMessage(sender, text, type, isHtml = false, when = null) {
    const messages = document.getElementById("messages");
    const div = document.createElement("div");
    div.className = `message ${type}`;

    const parsed = when != null ? new Date(when) : null;
    const at = parsed && !Number.isNaN(parsed.getTime()) ? parsed : new Date();
    const timeStr = formatMessageTime(at);

    let inner;

    if (type === "user") {
        const timeHtml = `<span class="message-time">${timeStr}<span class="msg-tick" title="Sent">&#10003;</span></span>`;
        const meta = `<div class="msg-meta-row"><span class="msg-sender-label">${sender}</span>${timeHtml}</div>`;
        if (isHtml) {
            inner = `${meta}<div class="msg-body msg-body-user">${text}</div>`;
        } else {
            inner = `${meta}<div class="msg-body msg-body-user">${marked.parseInline(text)}</div>`;
        }
    } else {
        const { body, tag } = stripAgentMetaSuffix(text);
        const timeHtml = `<span class="message-time">${timeStr}</span>`;
        const tagHtml = tag
            ? `<span class="msg-model-tag" title="Model / style">${tag.replace(/</g, "&lt;")}</span>`
            : "";
        const metaRight = `<span class="msg-meta-right">${tagHtml}${timeHtml}</span>`;
        const meta = `<div class="msg-meta-row"><strong>${sender}</strong>${metaRight}</div>`;
        if (isHtml) {
            inner = `${meta}<div class="agent-response">${body}</div>`;
        } else {
            inner = `${meta}<div class="agent-response">${marked.parse(body)}</div>`;
        }
    }

    // Sanitize LLM-generated content; user messages are self-constructed (safe)
    div.innerHTML = (type === "agent" && typeof DOMPurify !== "undefined")
      ? DOMPurify.sanitize(inner)
      : inner;
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    // ========================================================
    if (type === "agent") {
      addDownloadIfNeeded(div, text);
    }
    // ========================================================
    return div;
}

function setMsgTick(div, status) {
  const tick = div.querySelector('.msg-tick');
  if (!tick) return;
  if (status === "read") {
    tick.innerHTML = "&#10003;&#10003;";
    tick.classList.add("read");
    tick.title = "Read";
  } else {
    tick.innerHTML = "&#10003;";
    tick.classList.remove("read");
    tick.title = "Sent";
  }
}

// SVGs as strings for easy swapping
const leftArrowSVG = `
  <svg width="24" height="24" viewBox="0 0 32 32">
    <circle cx="16" cy="16" r="15" stroke="#17b3f0" stroke-width="2" fill="none"/>
    <polyline points="20,10 12,16 20,22" fill="none" stroke="#17b3f0" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>
`;
const rightArrowSVG = `
  <svg width="24" height="24" viewBox="0 0 32 32">
    <circle cx="16" cy="16" r="15" stroke="#17b3f0" stroke-width="2" fill="none"/>
    <polyline points="12,10 20,16 12,22" fill="none" stroke="#17b3f0" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>
`;
// Track collapsed state for left sidebar
let leftCollapsed = false;

function toggleSidebar(side) {
  if (side === "left") {
    const sidebar = document.getElementById("leftSidebar");
    const btn = document.getElementById("leftToggleBtn");
    const iconSpan = btn.querySelector("span");
    leftCollapsed = !leftCollapsed;
    sidebar.classList.toggle("sidebar-collapsed", leftCollapsed);
    btn.classList.toggle("left-collapsed", leftCollapsed);
    iconSpan.innerHTML = leftCollapsed ? rightArrowSVG : leftArrowSVG;
  }
}
lucide.createIcons();

// ----------------------- CHAT HISTORY RENDER -----------------------
function renderChatHistory() {
    const container = document.getElementById("chatHistoryContainer");
    container.innerHTML = "";

    document.querySelectorAll(".history-item").forEach(item => {
        item.classList.remove("active");
    });

    const groups = { Today: [], Yesterday: [], "This Week": [], Earlier: [] };
    const now = new Date();

    chatSessions.forEach((session, index) => {
        const date = new Date(session.startedAt);
        const diffDays = Math.floor((now - date) / (1000 * 60 * 60 * 24));
        let groupKey = diffDays === 0 ? "Today" : diffDays === 1 ? "Yesterday" : diffDays < 7 ? "This Week" : "Earlier";
        groups[groupKey].push({ index, title: session.title });
    });

    for (const [section, sessions] of Object.entries(groups)) {
        if (sessions.length > 0) {
            const sectionDiv = document.createElement("div");
            sectionDiv.innerHTML = `<strong>${section}</strong>`;

            sessions.forEach(sess => {
                const item = document.createElement("div");
                item.className = "history-item";
                if (sess.index === activeSessionIndex) {
                    item.classList.add("active");
                }

                item.innerHTML = `
                  <span class="history-item-title" style="flex:1 1 auto; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${sess.title}</span>
                  <span class="history-item-options" style="margin-left:auto; cursor:pointer; display:inline-block; font-weight:bold;" onclick="event.stopPropagation(); showHistoryOptions(${sess.index}, this)">⋯</span>
                `;
                item.style.display = "flex";
                item.style.alignItems = "center";

                item.querySelector('.history-item-title').onclick = () => {
                    loadSessionMessages(sess.index);
                    renderChatHistory();
                };

                sectionDiv.appendChild(item);
            });

            container.appendChild(sectionDiv);
        }
    }
}

function loadSessionMessages(index) {
  activeSessionIndex = index;
  clearMessages();

  const session = chatSessions[index];
  const messagesContainer = document.getElementById("messages");

  if (!session || !messagesContainer) return;
  if (!session.messages || session.messages.length === 0) {
    showWelcomeScreen();
    return;
  }

  session.messages.forEach(msg => {
    appendMessage(
      msg.sender === "user" ? "Me" : "Vikaa.AI",
      msg.text,
      msg.sender,
      false,
      msg.timestamp
    );
  });
}

// ================================================

// Do logout and redirect
async function doLogout() {
  const SUPABASE_URL = "https://dvawnejohsmjycxuhenu.supabase.co";
  const BACKEND_URL = typeof CONFIG !== 'undefined' ? CONFIG.API_BASE_URL : "https://app-wtiw.onrender.com";

  const tokenDataRaw = localStorage.getItem("authData");
  const accessToken = tokenDataRaw ? JSON.parse(tokenDataRaw)?.accessToken : null;

  const sessionId = localStorage.getItem("session_id");
  if (accessToken && sessionId) {
    try {
      await fetch(`${BACKEND_URL}/auth/logout`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ session_id: sessionId })
      });
    } catch (err) {
      console.error("❌ Backend logout failed:", err);
    }
  }

  // Supabase logout
  if (accessToken) {
    try {
      await fetch(`${SUPABASE_URL}/auth/v1/logout`, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${accessToken}`,
          "Content-Type": "application/json"
        }
      });
    } catch (err) {
      console.error("❌ Supabase logout failed:", err);
    }
  }

  // Clear only auth-related keys; preserve chat sessions
  localStorage.removeItem("authData");
  localStorage.removeItem("session_id");
  localStorage.removeItem("userEmail");

  const currentPath = window.location.href.substring(0, window.location.href.lastIndexOf('/'));
  window.location.href = currentPath + "/index.html";
}

// ======== Chat History Management =======================================================
function showHistoryOptions(index, button) {
  const existingMenu = document.getElementById("history-options-menu");
  if (existingMenu) existingMenu.remove();

  const menu = document.createElement("div");
  menu.id = "history-options-menu";
  menu.className = "history-options-menu";
  menu.innerHTML = `
      <div style="font-weight:normal;" onclick="renameSession(${index})">Rename</div>
      <div style="font-weight:normal;" onclick="deleteSession(${index})">Delete</div>
      <div style="font-weight:normal;" onclick="downloadSession(${index})">Download</div>
  `;

  button.parentNode.appendChild(menu);

  document.addEventListener("click", function handler(e) {
      if (!menu.contains(e.target) && e.target !== button) {
          menu.remove();
          document.removeEventListener("click", handler);
      }
  });
}

function renameSession(index) {
  const newName = prompt("Enter new chat name:", chatSessions[index].title);
  if (newName) {
      chatSessions[index].title = '📝 ' + newName;
      saveChatSessions();
      renderChatHistory();
  }
}

function downloadSession(index) {
  const session = chatSessions[index];
  if (!session) {
    alert("Chat session not found.");
    return;
  }

  let md = `# ${session.title}\n\n`;

  session.messages.forEach(msg => {
    const sender = msg.sender === "user" ? "**Me**" : "**Vikaa.AI**";
    const timestamp = new Date(msg.timestamp).toLocaleString();
    md += `### ${sender}  \n_${timestamp}_  \n${msg.text}\n\n`;
  });

  const safeTitle = session.title.replace(/[^\w\s]/gi, '').replace(/\s+/g, '_');
  const filename = `vikaa_${safeTitle || 'chat'}.md`;

  const blob = new Blob([md], { type: "text/markdown" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}


function deleteSession(index) {
  if (!confirm("Are you sure you want to delete this chat?")) return;

  chatSessions.splice(index, 1);
  saveChatSessions();
  renderChatHistory();

  if (index === activeSessionIndex) {
      clearMessages();
      activeSessionIndex = null;
  }
}

// ======================================================================
// 1️⃣ Voice Command (Microphone)
// ======================================================================

const SpeechRecognitionAPI = window.SpeechRecognition || window.webkitSpeechRecognition;
let recognition = null;
let isRecording = false;

function startVoiceRecognition() {
  if (!SpeechRecognitionAPI) {
    alert("Speech recognition is not supported in this browser. Try Chrome or Edge.");
    return;
  }

  const voiceBtn = document.getElementById("voiceButton");

  if (isRecording && recognition) {
    recognition.stop();
    return;
  }

  recognition = new SpeechRecognitionAPI();
  recognition.lang = navigator.language || 'en-US';
  recognition.interimResults = true;
  recognition.maxAlternatives = 1;
  recognition.continuous = false;

  isRecording = true;
  voiceBtn.classList.add("recording");
  voiceBtn.title = "Recording… click to stop";

  recognition.start();

  recognition.onresult = (event) => {
    let interim = '';
    let final = '';
    for (let i = event.resultIndex; i < event.results.length; i++) {
      const t = event.results[i][0].transcript;
      if (event.results[i].isFinal) final += t;
      else interim += t;
    }
    const input = document.getElementById("user-input");
    input.value = final || interim;
  };

  recognition.onend = () => {
    isRecording = false;
    voiceBtn.classList.remove("recording");
    voiceBtn.title = "Voice Input";
  };

  recognition.onerror = (event) => {
    isRecording = false;
    voiceBtn.classList.remove("recording");
    voiceBtn.title = "Voice Input";
    const friendly = {
      'no-speech':        'No speech detected. Please try again.',
      'audio-capture':    'Microphone not found or not accessible.',
      'not-allowed':      'Microphone permission denied. Please allow access in browser settings.',
      'network':          'Network error during speech recognition.',
    };
    const msg = friendly[event.error] || `Speech recognition error: ${event.error}`;
    alert(msg);
  };
}

// ======================================================================
// 2️⃣ Camera Snapshot (Webcam)
// ======================================================================

async function captureImage() {
  const cameraBtn = document.getElementById("cameraButton");
  cameraBtn.disabled = true;
  cameraBtn.title = "Opening camera…";

  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({ video: true });
  } catch (err) {
    cameraBtn.disabled = false;
    cameraBtn.title = "Capture Image";
    const msg = err.name === 'NotAllowedError'
      ? 'Camera permission denied. Please allow access in browser settings.'
      : err.name === 'NotFoundError'
      ? 'No camera found on this device.'
      : `Camera error: ${err.message}`;
    alert(msg);
    return;
  }

  try {
    const video = document.createElement('video');
    video.srcObject = stream;
    video.muted = true;
    video.playsInline = true;

    await new Promise((resolve, reject) => {
      video.onloadeddata = resolve;
      video.onerror = reject;
      video.play().catch(reject);
    });

    await new Promise(r => setTimeout(r, 150));

    const canvas = document.createElement('canvas');
    canvas.width  = video.videoWidth;
    canvas.height = video.videoHeight;
    canvas.getContext('2d').drawImage(video, 0, 0);

    const imageDataUrl = canvas.toDataURL('image/png');

    const now = new Date();
    const ts = [now.getHours(), now.getMinutes(), now.getSeconds()]
      .map(n => n.toString().padStart(2, '0')).join('');
    const filename = `snapshot_${ts}.png`;

    pendingAttachments.push({ filename, dataUrl: imageDataUrl });
    renderPendingAttachments();

  } catch (err) {
    alert(`Failed to capture image: ${err.message}`);
  } finally {
    stream.getTracks().forEach(t => t.stop());
    cameraBtn.disabled = false;
    cameraBtn.title = "Capture Image";
  }
}

// =================================================================
function addDownloadIfNeeded(div, rawText) {
  const textContent = rawText.trim();

  let detected = "txt";
  let mime = "text/plain";
  let blobContent = textContent;

  const lines = textContent.split("\n");

  // === TEXT FORMATS ===
  if (textContent.startsWith("<?xml") || textContent.includes("</")) {
    detected = "xml";
    mime = "application/xml";
  } else if (textContent.startsWith("{") || textContent.startsWith("[")) {
    detected = "json";
    mime = "application/json";
  } else if (
    lines.length >= 2 &&
    lines[0].includes(",") &&
    lines.every(line => line.split(",").length === lines[0].split(",").length)
  ) {
    detected = "csv";
    mime = "text/csv";
  }

  // === BASE64 FORMATS ===
  const base64Match = textContent.match(/^data:(.+);base64,(.+)$/);
  if (base64Match) {
    mime = base64Match[1];
    const extMap = {
      "image/png": "png",
      "image/jpeg": "jpg",
      "image/webp": "webp",
      "audio/mpeg": "mp3",
      "audio/wav": "wav",
      "video/mp4": "mp4",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
      "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx"
    };
    detected = extMap[mime] || "bin";

    const binary = atob(base64Match[2]);
    const array = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) array[i] = binary.charCodeAt(i);
    blobContent = array;
  }

  // === CREATE BLOB & BUTTONS ===
  const blob = new Blob([blobContent], { type: mime });
  const url = URL.createObjectURL(blob);

  const buttonGroup = document.createElement("div");
  buttonGroup.className = "message-agent-actions";

  const copyBtn = document.createElement("button");
  copyBtn.innerHTML = `<i class="fa-regular fa-copy"></i>`;
  copyBtn.title = "Copy response to clipboard";
  copyBtn.onclick = async () => {
    try {
      await navigator.clipboard.writeText(textContent);
      copyBtn.textContent = "✅ Copied";
      setTimeout(() => {copyBtn.innerHTML = `<i class="fa-regular fa-copy"></i>`;}, 1500);
    } catch (err) {
      alert("Clipboard copy failed.");
    }
  };

  const downloadBtn = document.createElement("button");
  downloadBtn.textContent = `⬇️`;
  downloadBtn.title = `Download this response`;
  downloadBtn.onclick = () => {
    const a = document.createElement("a");
    a.href = url;
    a.download = `response.${detected}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const regenBtn = document.createElement("button");
  regenBtn.innerHTML = `<i class="fas fa-redo-alt" style="font-size:10px;"></i>`;
  regenBtn.title = "Regenerate response";
  regenBtn.onclick = regenerateLastResponse;

  buttonGroup.appendChild(copyBtn);
  buttonGroup.appendChild(downloadBtn);
  buttonGroup.appendChild(regenBtn);
  div.appendChild(buttonGroup);
}

// =================================================================
// Pressing Enter → sends message ✅
// Pressing Shift + Enter → adds a new line ✅
// =================================================================

window.onload = function () {
  fetchAccessMode().then((access) => {
    canExecute = !!access.can_execute;
    renderAccessBanner(canExecute);
    applyRunControls(canExecute);
  });
  document.getElementById("sendButton").addEventListener('click', sendMessage);
  document.getElementById("voiceButton").addEventListener('click', startVoiceRecognition);
  document.getElementById("cameraButton").addEventListener('click', captureImage);
  document.getElementById("attachmentButton").addEventListener('click', () => {
    document.getElementById("fileInput").click();
  });

  document.getElementById("fileInput").addEventListener('change', function(event) {
    const files = event.target.files;
    if (files.length === 0) return;

    Array.from(files).forEach(file => {
      const reader = new FileReader();
      reader.onload = function(e) {
        const fileContent = e.target.result;
        pendingAttachments.push({
          filename: file.name,
          dataUrl: fileContent
        });
        renderPendingAttachments();
      };
      reader.readAsDataURL(file);
    });
  });

  // ✅ Send on Enter (not Shift+Enter)
  document.getElementById("user-input").addEventListener("keydown", function (event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendMessage();
    }
  });

  pendingAttachments = [];
  renderPendingAttachments();
};


// ======================= NEW HELPERS ============================

// ── Typing indicator ─────────────────────────────────────────────
function _showTypingIndicator() {
  const messages = document.getElementById("messages");
  const div = document.createElement("div");
  div.className = "typing-indicator";
  div.id = "vikaa-typing";
  div.innerHTML = '<span class="typing-dot"></span><span class="typing-dot"></span><span class="typing-dot"></span>';
  messages.appendChild(div);
  messages.scrollTop = messages.scrollHeight;
  return div;
}

function _removeTypingIndicator(div) {
  if (div && div.parentNode) div.parentNode.removeChild(div);
}

// ── Deliver agent reply + save to session ────────────────────────
function _deliverAgentReply(agentReply, modelType, styleType, msgDiv) {
  const tagged = agentReply + " — [" + modelType + "/" + styleType + "]";
  appendMessage("Vikaa.AI", tagged, "agent");
  if (activeSessionIndex !== null && chatSessions[activeSessionIndex]) {
    chatSessions[activeSessionIndex].messages.push({ sender: "agent", text: tagged, timestamp: new Date() });
    saveChatSessions();
  }
  setTimeout(() => setMsgTick(msgDiv, "read"), 1000);
}

// ── Regenerate last response ─────────────────────────────────────
function regenerateLastResponse() {
  if (!_lastUserQuery || !canExecute) return;
  const input = document.getElementById("user-input");
  input.value = _lastUserQuery;
  sendMessage();
}

// ── Welcome screen with starter prompts ─────────────────────────
function showWelcomeScreen() {
  const messages = document.getElementById("messages");
  if (!messages) return;

  const starters = [
    { icon: "📄", label: "Summarize a document",    prompt: "Please summarize the key points from this document." },
    { icon: "📊", label: "Analyze a dataset",        prompt: "Analyze this data and give me the key insights and trends." },
    { icon: "🎥", label: "Explain a YouTube video",  prompt: "Please summarize this video: [paste YouTube URL here]" },
    { icon: "💻", label: "Review my code",           prompt: "Please review this code and suggest improvements." },
    { icon: "🔍", label: "Research a topic",         prompt: "Give me a structured overview of: [your topic here]" },
  ];

  const btns = starters.map(s => {
    const safe = s.prompt.replace(/"/g, "&quot;");
    return `<button class="starter-btn"
      data-prompt="${safe}"
      onclick="useStarterPrompt(this.dataset.prompt)"
      style="background:#fff;border:1px solid #e0e0e0;border-radius:10px;padding:9px 15px;
             font-size:12px;font-family:Montserrat,sans-serif;color:#444;cursor:pointer;
             transition:all 0.15s;display:flex;align-items:center;gap:7px;">
      <span>${s.icon}</span><span>${s.label}</span>
    </button>`;
  }).join('');

  messages.innerHTML = `
    <div style="text-align:center;padding:44px 20px 20px;font-family:Montserrat,sans-serif;">
      <div style="font-size:30px;margin-bottom:8px;">👋</div>
      <div style="font-weight:700;color:#444;font-size:13.5px;margin-bottom:5px;">Welcome to Vikaa.AI Chat</div>
      <div style="font-size:12px;color:#aaa;margin-bottom:26px;">Ask anything — attach files, images, audio, or video.</div>
      <div style="display:flex;flex-wrap:wrap;gap:9px;justify-content:center;max-width:520px;margin:0 auto;">
        ${btns}
      </div>
    </div>`;
}

function useStarterPrompt(prompt) {
  const input = document.getElementById("user-input");
  if (!input) return;
  input.value = prompt;
  input.focus();
  input.setSelectionRange(prompt.length, prompt.length);
}

// ======================= END ============================
