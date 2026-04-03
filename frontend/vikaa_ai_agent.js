// ----------------------- GLOBAL STATE -----------------------

let chatSessions = [];
let activeSessionIndex = null;
let pendingAttachments = [];  // ✅ REQUIRED: Holds files/images before sending
let canExecute = false;

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
  // console.log("🔥 renderPendingAttachments CALLED. Items =", pendingAttachments.length);

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
    sessionId = Math.random().toString(36).substring(2);
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
  renderChatHistory();
  // Don't save yet — persist only when the first message is sent
}

function clearMessages() {
  document.getElementById("messages").innerHTML = "";
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

  /// NEW LINE
  if (activeSessionIndex === null) startNewChat();

  // const msgDiv = appendMessage("Me", message, "user");
  let combinedContent = '';

  if (pendingAttachments.length > 0) {
      pendingAttachments.forEach(att => {
          if (att.dataUrl.startsWith("data:image")) {
              // combinedContent += `<img src="${att.dataUrl}" style="max-width:100px; border-radius:6px; margin-bottom:5px;"><br>`;
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

  /// NEW LINE
  chatSessions[activeSessionIndex].messages.push({ sender: "user", text: message, timestamp: new Date() });
  saveChatSessions();
  
  // [NEW] If this is the first user message → update session title to be meaningful
  if (chatSessions[activeSessionIndex].messages.length === 1) {
      // chatSessions[activeSessionIndex].title = message.length > 30 ? message.substring(0, 30) + '...' : message;
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

  try {
    const apiUrl = (typeof CONFIG !== 'undefined' ? CONFIG.API_BASE_URL : "https://app-wtiw.onrender.com") + "/agent/message";
    const tokenDataRaw = localStorage.getItem("authData");
    const accessToken = tokenDataRaw ? JSON.parse(tokenDataRaw)?.accessToken : null;
    if (!accessToken) {
      appendMessage("Vikaa.AI", "⚠️ Please login first. Execution requires an authenticated account.", "agent");
      return;
    }

    const response = await fetch(apiUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${accessToken}`
      },
      body: JSON.stringify({
        session_id: sessionId,
        query: message,
        model: modelType,
        temperature: llmStyleValue,
        attachments: pendingAttachments, // ✅ send base64 files to backend
      }),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = typeof data.detail === "string" ? data.detail : `Request failed (${response.status})`;
      appendMessage("Vikaa.AI", `⚠️ ${msg}`, "agent");
      return;
    }
    if (data.response && data.response.startsWith("Access denied")) {
        alert(data.response); // Show popup
        return;
    }
    const agentReply = data.response ?? "No response from agent.";

    let newagentReply = agentReply + " - ["+modelType+"/"+styleType+"]"
    appendMessage("Vikaa.AI", newagentReply , "agent");

    /// NEW LINE
    chatSessions[activeSessionIndex].messages.push({ sender: "agent", text: newagentReply, timestamp: new Date() });
    saveChatSessions();
    setTimeout(() => setMsgTick(msgDiv, "read"), 1000);

    pendingAttachments = [];
    renderPendingAttachments();

  } catch (error) {
    appendMessage("Vikaa.AI", "⚠️ **Failed to reach Agent**. API Call Error in **sendMessage()** catch", "agent");
  }
}

// ----------------------- MESSAGE UI -----------------------      
function appendMessage(sender, text, type, isHtml = false) {
    const messages = document.getElementById("messages");
    const div = document.createElement("div");
    div.className = `message ${type}`;

    const now = new Date();
    const year = now.getFullYear();
    const month = (now.getMonth() + 1).toString().padStart(2, "0");
    const day = now.getDate().toString().padStart(2, "0");
    let hours = now.getHours();
    const minutes = now.getMinutes().toString().padStart(2, "0");
    const ampm = hours >= 12 ? "PM" : "AM";
    hours = hours % 12;
    hours = hours ? hours : 12; // 0 should be 12
    const hoursStr = hours.toString().padStart(2, "0");
    const fullDateTime = `${year}-${month}-${day} ${hoursStr}:${minutes} ${ampm}`;
    
    let inner;

    if (isHtml) {
        inner = type === "agent"
            ? `<strong>${sender}:</strong> ${text}`
            : `<span class="msg-sender-label">${sender}</span>${text}`;
    } else {
        if (type === "agent") {
            const formattedText = marked.parse(text);
            inner = `<strong>${sender}:</strong><div class="agent-response">${formattedText}</div>`;
        } else {
            inner = `<span class="msg-sender-label">${sender}</span>${marked.parseInline(text)}`;
        }
    }
    if (type === "user") {
        inner += `<span class="message-time">${fullDateTime}<span class="msg-tick" title="Sent">&#10003;</span></span>`;
    } else {
        inner += `<span class="message-time">${fullDateTime}</span>`;
    }
    div.innerHTML = inner;
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
    // ========================================================
    if (type === "agent") {
      addDownloadIfNeeded(div, text);  // 👈 inject download button if needed
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

    // Remove active class from all (precaution)
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

                // Make the inner HTML -> title + options (...)
                item.innerHTML = `
                  <span class="history-item-title" style="flex:1 1 auto; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${sess.title}</span>
                  <span class="history-item-options" style="margin-left:auto; cursor:pointer; display:inline-block; font-weight:bold;" onclick="event.stopPropagation(); showHistoryOptions(${sess.index}, this)">⋯</span>
                `;
                item.style.display = "flex";
                item.style.alignItems = "center";

                // Handle clicking on the title → load session
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
    messagesContainer.innerHTML = `
      <div style="text-align:center; color:#999; padding-top:60px; font-size:16px;">
        👋 Start a conversation by typing below...
      </div>`;
    return;
  }

  session.messages.forEach(msg => {
    appendMessage(msg.sender === "user" ? "Me" : "Vikaa.AI", msg.text, msg.sender);
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
  // Remove existing menu if already open
  const existingMenu = document.getElementById("history-options-menu");
  if (existingMenu) existingMenu.remove();

  // Create menu
  const menu = document.createElement("div");
  menu.id = "history-options-menu";
  menu.className = "history-options-menu";
  menu.innerHTML = `
      <div style="font-weight:normal;" onclick="renameSession(${index})">Rename</div>
      <div style="font-weight:normal;" onclick="deleteSession(${index})">Delete</div>
      <div style="font-weight:normal;" onclick="downloadSession(${index})">Download</div>      
  `;

  // Position and add
  button.parentNode.appendChild(menu);

  // Close on outside click
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
// ======================================================================
// 1️⃣ Voice Command (Microphone)
// - Toggles on/off; populates input field for review before sending
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

  // Toggle off if already recording
  if (isRecording && recognition) {
    recognition.stop();
    return;
  }

  recognition = new SpeechRecognitionAPI();
  recognition.lang = navigator.language || 'en-US';
  recognition.interimResults = true;
  recognition.maxAlternatives = 1;
  recognition.continuous = false;

  // Visual: mark button as active
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
    // Show live interim text in the input box so the user can see/edit
    const input = document.getElementById("user-input");
    input.value = final || interim;
  };

  recognition.onend = () => {
    isRecording = false;
    voiceBtn.classList.remove("recording");
    voiceBtn.title = "Voice Input";
    // Do NOT auto-send — let the user review and press Enter/Send
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
// - Waits for a real video frame before capturing
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

    // Wait for the video to have actual frame data before capturing
    await new Promise((resolve, reject) => {
      video.onloadeddata = resolve;
      video.onerror = reject;
      video.play().catch(reject);
    });

    // Small settle delay so the first frame is rendered
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
  const codeBlock = div.querySelector("code");
  const textContent = rawText.trim();  // use original rawText first

  let detected = "txt";
  let mime = "text/plain";
  let blobContent = textContent;

  const lines = textContent.split("\n");

  // === TEXT FORMATS ===
  if (textContent.startsWith("<?xml")  || textContent.includes("</")) {
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
  buttonGroup.style.marginTop = "6px";
  buttonGroup.style.display = "flex";
  buttonGroup.style.gap = "8px";

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
  downloadBtn.className = "text-xs bg-gray-100 border border-gray-300 rounded px-2 py-1";
  downloadBtn.title = `Download this response`;
  downloadBtn.onclick = () => {
    const a = document.createElement("a");
    a.href = url;
    a.download = `response.${detected}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  buttonGroup.appendChild(copyBtn);
  buttonGroup.appendChild(downloadBtn);
  div.appendChild(buttonGroup);
}

// =================================================================
// Pressing Enter → sends message ✅
// Pressing Shift + Enter → adds a new line ✅ (optional editing)
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

  // ✅ NEW: Send on Enter (not Shift+Enter)
  document.getElementById("user-input").addEventListener("keydown", function (event) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendMessage();
    }
  });

  pendingAttachments = [];
  renderPendingAttachments();
};



// ======================= END ============================


