// To Open Login Page

// Wait until the Supabase library is loaded
const SUPABASE_URL = 'https://dvawnejohsmjycxuhenu.supabase.co'; 
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR2YXduZWpvaHNtanljeHVoZW51Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDg4MzcxMTgsImV4cCI6MjA2NDQxMzExOH0.uLMiPNKawJwuMV9zg60736gQUVhS5jJJ88EnFUHuRmQ';

if (typeof window.supabaseClientInstance === 'undefined') {
    window.supabaseClientInstance = null;
}

/** Same-origin OAuth callback: loopback or LAN (not production vikaa.ai). */
function vikaaUseDynamicOAuthCallbackUrl() {
    const h = (window.location.hostname || "").toLowerCase();
    if (h === "localhost" || h === "127.0.0.1") return true;
    return /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(h);
}

document.addEventListener("DOMContentLoaded", function() {
    console.log("🛠️ LoginPage.js: DOMContentLoaded. Initializing Supabase...");
    if (window.supabase && !window.supabaseClientInstance) {
        window.supabaseClientInstance = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        console.log("✅ Supabase initialized successfully.");
    } else if (!window.supabase) {
        console.error("❌ Supabase library NOT found on window object.");
    }
});

window.loginWithProvider = async function(provider) {
    console.log(`🚀 Attempting login with provider: ${provider}`);
    try {
        localStorage.removeItem("vikaa_tour_mode");
        if (!window.supabaseClientInstance && window.supabase) {
            window.supabaseClientInstance = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
        }
        if (!window.supabaseClientInstance) {
            alert("Supabase client not initialized. Please ensure you are running on a web server (http://), not file://");
            console.error("❌ Supabase client NOT initialized.");
            return;
        }

        // ✅ Force logout to avoid cached login
        console.log("🔄 Signing out current session...");
        await window.supabaseClientInstance.auth.signOut();

        // ✅ Dynamic Redirect URL for local / LAN debugging (production uses fixed host)
        let redirectUrl;
        if (vikaaUseDynamicOAuthCallbackUrl()) {
            const currentPath = window.location.href.substring(0, window.location.href.lastIndexOf('/'));
            redirectUrl = currentPath + "/callback.html";
        } else {
            redirectUrl = "https://vikaa.ai/callback.html";
        }
        
        console.log("🎯 Redirecting to:", redirectUrl);

        const options = {
            redirectTo: redirectUrl,
            queryParams: provider === "google" ? { prompt: 'select_account' } : {}
        };

        const { data, error } = await window.supabaseClientInstance.auth.signInWithOAuth({
            provider: provider,
            options: options
        });

        if (error) {
            console.error("❌ Supabase signInWithOAuth error:", error);
            alert("Login failed. 'You are in LoginPage.html': " + error.message);
        } else {
            console.log("✅ Supabase OAuth request initiated:", data);
        }

    } catch (e) {
        console.error("❌ Unexpected error in loginWithProvider:", e);
        alert("An unexpected error occurred. 'You are in LoginPage.html catch block': " + e.message);
    }
};

function closeLogin() {
    document.getElementById('settingsLogin').style.display = 'none';
  }
  
function openLogin() {
  // Check if modal already exists
  let modal = document.getElementById('settingsLogin');
  if (!modal) {
    // Create modal HTML structure
    
    modal = document.createElement('div');
    modal.className = 'settings-modal';
    modal.id = 'settingsLogin';
    modal.style.position = 'fixed';
    modal.style.top = '0';
    modal.style.left = '0';
    modal.style.width = '100vw';
    modal.style.height = '100vh';
    modal.style.background = 'rgba(0,0,0,0.3)';
    modal.style.display = 'flex';
    modal.style.alignItems = 'center';
    modal.style.justifyContent = 'center';
    modal.style.zIndex = '9999';

    // Popup window style
    const popupStyle = `
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.25);
      width: 70vw;
      max-width: 500px;
      max-height: 90vh;
      height: auto;                
      display: flex;
      flex-direction: column;
      overflow: hidden;
      position: relative;
      animation: popupFadeIn 0.2s;
    `;

    modal.innerHTML = `
      <div class="Login-window" style="${popupStyle}">
      <div class="Login-header" style="padding:10px 15px; font-size:1.0em; font-weight:semibold; border-bottom:2px solid #17bcfe;">
        <span style="color:#17bcfe; font-weight:bold;">vikaa.ai - Login</span>
        <span onclick="closeLogin()" style="cursor:pointer; font-size:1.5em; color:#17bcfe; float:right; margin-left:auto;">&times;</span>
      </div>

      <div class="Login-body" style="display:flex; flex:1 1 auto; min-height:180px; overflow-y:auto; flex-direction:column; align-items:center; justify-content:center;">
        <div class="login-container" style="display:flex; flex-direction:column; align-items:center; width:100%;">
          <br><br>
          <button class="login-button google" onclick="loginWithProvider('google')" style="margin-bottom: 16px; ">Login with Google</button>
          <button class="login-button github" onclick="loginWithProvider('github')" style="margin-bottom: 16px; ">Login with GitHub</button>
          <br><br><br>
        </div>
      </div>
      </div>
    `;

    // Optional: Add fade-in animation
    const style = document.createElement('style');
    style.innerHTML = `
      @keyframes popupFadeIn {
        from { transform: scale(0.96); opacity: 0; }
        to { transform: scale(1); opacity: 1; }
      }
    `;
    document.head.appendChild(style);
    document.body.appendChild(modal);
  }
  modal.style.display = 'flex';
}

