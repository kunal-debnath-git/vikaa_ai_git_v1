// TO OEPN SETTINGS PAGE =====================================================    
  function closeVikaaSettings() {
    document.getElementById('settingsVikaaModal').style.display = 'none';
  }

  /// =================== Not Applicable as we have disable chat auto-save
  // function loadSettingAutoSave(item) {
  //   document.getElementById('settingsContent').innerHTML = `
  //       <strong>[ Auto Save Settings ]</strong><br><br>
  //       Set Local Disk: ${item}<br>
  //       Set Google Drive: ${item}
  //   `;
  // }

  // function loadSetting(item) {
  //   document.getElementById('settingsContent').innerText = `You selected: ${item}`;
  // }
  /// =================== Not Applicable as we have disable chat auto-save

  function saveVikaaSettings() {
    alert("Settings saved successfully.");
  }
  
  function saveAndClose() {
    saveVikaaSettings();
    closeVikaaSettings();
  }
  
  function openVikaaSettings() {
    // Check if modal already exists
    let modal = document.getElementById('settingsVikaaModal');
    if (!modal) {
      // Create modal HTML structure
      modal = document.createElement('div');
      modal.className = 'settings-modal';
      modal.id = 'settingsVikaaModal';
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
        min-width: 700px;   /* Increased */
        min-height: 400px;  /* Increased */
        max-width: 90vw;
        max-height: 90vh;
        display: flex;
        flex-direction: column;
        overflow: hidden;
        position: relative;
        animation: popupFadeIn 0.2s;
      `;
  
      modal.innerHTML = `
      <div class="settings-window" style="${popupStyle}; background:#fff;border-radius:8px;padding:10px;width:700px;height:450px;
                  display:flex;flex-direction:column;box-shadow:0 0 20px rgba(0,0,0,0.2);overflow:auto;">
        <div class="settings-header" style="color:#17bcfe;font-weight:bold;padding:10px 15px;font-size:1.0em;border-bottom:2px solid #17bcfe;">
            'vikaa.ai' - Settings
            <span onclick="closeVikaaSettings()" style="cursor:pointer;float:right;font-size:1.9em;">&times;</span>
        </div>

        <div class="settings-body" style="display:flex; flex:1 1 auto; min-height:180px;">
            <div class="settings-sidebar" style="width:200px; border-right:1px solid #eee; padding:12px 0; background:#fafbfc; font-size:12px; color:#555; line-height:1.4;">
                <div onclick="loadSetting('Item 1')" style="padding:2px 8px; cursor:pointer;">Settings Item 1</div>
                <div onclick="loadSetting('Item 2')" style="padding:2px 8px; cursor:pointer;">Settings Item 2</div>
                <div onclick="loadSetting('Item 3')" style="padding:2px 8px; cursor:pointer;">Settings Item 3</div>                
                
            </div>
            <div class="settings-content" id="settingsContent" style="flex:1 1 auto; padding:20px; font-size:12px; color:#555;">
              Please select a setting from the left panel.
            </div>
          </div>
          <div class="settings-footer" style="padding:12px 10px; border-top:1px solid #eee; text-align:right;">
            <button type="button" onclick="closeVikaaSettings()" style="margin-right:440px;" class="text-xs rounded px-2 py-1 bg-gray-100 font-semibold text-gray-700">Back</button>
            <button type="button" onclick="saveVikaaSettings()" style="margin-right:15px;" class="text-xs rounded px-2 py-1 bg-gray-100 font-semibold text-gray-700">Save</button>
            <button type="button" onclick="saveAndClose()" style="margin-right:5px;" class="text-xs rounded px-2 py-1 bg-gray-100 font-semibold text-gray-700">Save & Close</button>
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
  