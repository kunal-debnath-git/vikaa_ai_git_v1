#!/usr/bin/env bash
set -e

echo "==> Installing system packages..."
apt-get update -y
apt-get install -y ffmpeg

echo "==> Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "==> Build complete."
