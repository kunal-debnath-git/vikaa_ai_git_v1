function setAgentPicture(path, width = 32, height = 32, shape = "rectangle") {
    const img = document.getElementById('agentPicture');
    if (!img) return;
    if (path) {
        img.src = path;
        img.style.width = width + 'px';
        img.style.height = height + 'px';
        img.style.display = 'inline-block';
        img.style.borderRadius = shape === "round" ? "50%" : "10px";
        img.style.objectFit = "contain";
        img.style.backgroundColor = "#f0f0f0";
    } else {
        img.style.display = 'none';
    }
}

function closeInternals() {
    const modal = document.getElementById('settingsModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

function openInternals() {
    let modal = document.getElementById('settingsModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.className = 'settings-modal';
        modal.id = 'settingsModal';
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

        const popupStyle = `
            background: #fff;
            border-radius: 10px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.25);
            width: 90%;
            max-width: 800px;
            max-height: 90vh;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            position: relative;
            animation: popupFadeIn 0.2s ease-in-out;
            box-sizing: border-box;
        `;

        modal.innerHTML = `
            <div class="Internals-window" style="${popupStyle}">
                <div class="Internals-header" style="color:#17bcfe; font-weight:bold; padding:10px 15px; font-size:1.0em; border-bottom:2px solid #17bcfe;">
                    vikaa.ai - Internals
                    <span onclick="closeInternals()" style="cursor:pointer; font-size:1.5em; color:#17bcfe; float:right;">&times;</span>
                </div>

                <div class="Internals-body" style="flex: 1 1 auto; overflow-y: auto; overflow-x: auto; padding: 16px; box-sizing: border-box;">
                    <div style="display: flex; flex-direction: column; align-items: center; width: 100%; max-width: 100%; overflow-x: auto; text-align: center; box-sizing: border-box;">

                    <span style="color:#17bcfe; font-weight:bold;">Technical stack</span>
                    <span style="color:#808080; font-weight:normal; font-size:0.8em;">Components and services (public dataset link below).</span>
                    <table style="width:100%; max-width:720px; margin:12px auto 8px; border-collapse:collapse; font-size:0.85rem; text-align:left; box-sizing:border-box;">
                        <thead>
                            <tr style="border-bottom:2px solid #17bcfe;">
                                <th style="padding:8px 10px; color:#17bcfe; width:38%;">Area</th>
                                <th style="padding:8px 10px; color:#17bcfe;">Technologies</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">API Integration</td><td style="padding:8px 10px; color:#555;">FastAPI, Render, Uvicorn</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">Tools</td><td style="padding:8px 10px; color:#555;">Tavily, Gmail API + Google OAuth, Twilio, and more…</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">Agent runtime &amp; framework</td><td style="padding:8px 10px; color:#555;">MCP (Model Context Protocol), FastAPI, LangChain</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">Data integration</td><td style="padding:8px 10px; color:#555;">Google API, Google Drive, warehouse / lakehouse, RAG / vector stores</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">Storage</td><td style="padding:8px 10px; color:#555;">MongoDB Atlas Vector, Supabase, Google Drive</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">LLM sub-components</td><td style="padding:8px 10px; color:#555;">Text, image, audio, video</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;"><td style="padding:8px 10px; font-weight:600; color:#333;">LLM</td><td style="padding:8px 10px; color:#555;">Gemini, GPT, Claude</td></tr>
                            <tr style="border-bottom:1px solid #e0e0e0;">
                                <td style="padding:8px 10px; font-weight:600; color:rgb(11, 104, 147);">Dataset reference URL</td>
                                <td style="padding:8px 10px; color: #555;">
                                    <a href="olist_dataset_briefing.html" target="_blank" 
                                        rel="noopener noreferrer" style="color:rgb(11, 104, 147);
                                        font-weight:600; text-decoration:none;">Olist Brazilian E-Commerce (Kaggale dataset)</a>
                                </td>
                            </tr>

                            <tr style="border-bottom:1px solid #e0e0e0;">
                                <td style="padding:8px 10px; font-weight:600; color:rgb(11, 104, 147);">GitHub Pubilic Repo</td>
                                <td style="padding:8px 10px; color: #555;">
                                    <a href="https://github.com/kunal-debnath-git/vikaa_ai_git_v1" target="_blank" 
                                        rel="noopener noreferrer" style="color:rgb(11, 104, 147);
                                        font-weight:600; text-decoration:none;">https://github.com/kunal-debnath-git</a>
                                </td>
                            </tr>                            
                        </tbody>
                    </table>

                    <br>

                        <br><br><span style="color:#17bcfe; font-weight:bold;">Slide - Main Flow</span>
                        <span style="color:#808080; font-weight:normal; font-size:0.8em;">Fully expandable architecture and replaceable tools and services.</span>                        
                        <br><img src="image/main-flow.jpg" style="max-width: 100%; height: auto;" alt="Main Flow" />

                        <br><br><span style="color:#17bcfe; font-weight:bold;">Slide - Agentic RAG Flow</span>
                        <span style="color:#808080; font-weight:normal; font-size:0.8em;">Conneting all dots to manage external knowledge</span>                        
                        <br><img src="image/Agentic-RAG.jpg" style="max-width: 100%; height: auto;" alt="Agentic RAG" />
                     

                        <br><br>                        
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(modal);
    }
    modal.style.display = 'flex';
}

                        // <br><br><span style="color:#17bcfe; font-weight:bold;">Slide - Toots - Twitter/X Interface</span>
                        // <span style="color:#808080; font-weight:normal; font-size:0.8em;">A semi-automated solution to create and manage Twitter/X activity. More to come</span>
                        // <br><br><img src="image/Twitter.jpg" style="max-width: 100%; height: auto;" alt="Twitter" />   

document.addEventListener("DOMContentLoaded", () => {
    /* Home uses static Logo4 header; no banner image */
});

// Anti-copy logic
document.addEventListener("contextmenu", e => e.preventDefault());
document.onkeydown = e => {
    if (e.ctrlKey && ['u','s','c'].includes(e.key.toLowerCase())) {
        e.preventDefault();
    }
};
