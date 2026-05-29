#!/bin/bash
# Run this script on the EC2 instance after cloning the repo.
set -e

echo "==> Installing system packages"
sudo dnf update -y
sudo dnf install python3-pip git -y

echo "==> Installing Python dependencies"
pip3 install -r requirements.txt

echo "==> Done. Start the app with:"
echo "    sudo gunicorn -w 2 -b 0.0.0.0:80 app:app"
echo ""
echo "    Or with .env file:"
echo "    export \$(cat .env | xargs) && sudo -E gunicorn -w 2 -b 0.0.0.0:80 app:app"
