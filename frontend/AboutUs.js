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

function setAboutMePicShape(shape = "round", width = 180, height = 180) {
    const img = document.getElementById('aboutMePic');
    img.style.width = width + "px";
    img.style.height = height + "px";
    if (shape === "round") {
        img.style.borderRadius = "50%";
    } else {
        img.style.borderRadius = "10px";
    }
}

// Run on load
document.addEventListener("DOMContentLoaded", () => {
    setAboutMePicShape("square", 480, 360);
});
