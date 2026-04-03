# 🎨 Vikaa.AI Frontend Style Guide & Prompt Template

Use this template when generating new HTML, CSS, or JS files for the Vikaa.AI platform to ensure absolute visual and behavioral consistency.

---

## 🏗️ Core Architecture Patterns

### 1. Page Shell Layout
- **Container:** Always use a `flex flex-col h-screen w-screen overflow-hidden` wrapper.
- **Background:** Page background is consistently `#eaeaea` (light gray). Workspace/Dashboard areas use `#f9fafb`.
- **Header:** White background, `10px 40px` padding, flex-row with `justify-between`.
- **Accent:** A `2px` horizontal border with color `#17bcfe` must separate the header from the main content.

### 2. Typography & Colors
- **Fonts:** 
  - Primary: `'Montserrat', sans-serif` (Weights: 400, 600, 700).
  - Secondary: `'Roboto', sans-serif`.
  - Data/Code: `monospace`.
- **Color Palette:**
  - **Action Blue:** `#17bcfe` (Hover: `#0891b2`).
  - **Text Primary:** `#333333`.
  - **Text Secondary:** `#575757`.
  - **Border Gray:** `#dddddd` or `#eeeeee`.

---

## 💅 Visual Components

### Buttons & Interactive Elements
- **Mode Buttons:** Large cards (`280x180px`), white background, `15px` radius, subtle shadow (`0 10px 25px rgba(0,0,0,0.08)`).
- **Standard Buttons:** Small rounded corners (`6px`), light gray background (`#f5f5f5`), font-weight `600`.
- **Hover Effects:** Vertical lift (`translateY(-8px)`) and blue border glow for card elements.

### Sidebars (Dashboard Pattern)
- **Width:** Fixed `280px` or `20%`.
- **Transition:** `all 0.3s ease`.
- **Items:** Padding `12px 15px`, border-radius `8px`. Active state uses background `#17bcfe` with white text.

### Modals & Popups
- **Animation:** `popupFadeIn 0.2s ease-in-out` using `scale(0.96) -> scale(1)`.
- **Overlay:** `rgba(0,0,0,0.3)` or `rgba(0,0,0,0.5)` with `backdrop-filter: blur(2px)`.
- **Header:** Blue gradient or solid `#17bcfe` with a white "×" close icon.

---

## 🛠️ Technical Implementation

### Required Libraries (Head Section)
```html
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&family=Roboto:wght@300;500;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<script src="https://cdn.tailwindcss.com"></script>
```

### Security & UX Defaults
- **Anti-Copy Script:**
  ```javascript
  document.addEventListener("contextmenu", e => e.preventDefault());
  document.onkeydown = e => {
    if (e.ctrlKey && ['u','s','c'].includes(e.key.toLowerCase())) e.preventDefault();
  };
  ```
- **Cache Control:** Always include `no-store`, `no-cache` meta tags.
- **Responsiveness:** Use `@media (max-width: 768px)` to collapse sidebars or stack flex containers.

---

## 📋 Generation Prompt Template

**When asking an AI to build a new page, use this prompt:**

> "Create a new Vikaa.AI [PAGE_NAME] file. 
> 
> **Visuals:** Adhere to the `frontend_style.md` guidelines. Use Montserrat font, #17bcfe blue accents, and #eaeaea background.
> 
> **Layout:** Implement a [LAYOUT_TYPE: e.g., Dashboard with Sidebar / Simple Landing Page]. Ensure the header contains the Logo4.jpeg and standard navigation links.
> 
> **Components:** Include [COMPONENT_LIST: e.g., Tool Cards, Prompt Input, Activity Feed].
> 
> **Functionality:** 
> 1. Add the standard anti-copy/right-click prevention logic.
> 2. Implement a responsive design that handles mobile screens.
> 3. Use Font Awesome 6.5.0 for all icons.
> 
> **Context:** This page lives in the `/frontend` directory and should link correctly to index.html and AboutUs.html."
