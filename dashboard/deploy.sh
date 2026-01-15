#!/bin/bash

# Deployment script for Contabo VPS
# Usage: ./deploy.sh

set -e

echo "🚀 Starting deployment..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "📦 Installing Node.js..."
    curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi

# Check if PM2 is installed
if ! command -v pm2 &> /dev/null; then
    echo "📦 Installing PM2..."
    sudo npm install -g pm2
fi

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
PORT=3000
SESSION_SECRET=$(openssl rand -hex 32)
ADMIN_USERNAME=admin
ADMIN_PASSWORD=$(openssl rand -hex 16)
MANAGED_SCRIPT_PATH=
EOF
    echo -e "${GREEN}✅ .env file created. Please update ADMIN_USERNAME and ADMIN_PASSWORD!${NC}"
fi

# Start/restart with PM2
echo "🔄 Starting application with PM2..."
pm2 delete dashboard || true
pm2 start server.js --name dashboard
pm2 save
pm2 startup

echo -e "${GREEN}✅ Deployment completed!${NC}"
echo -e "${BLUE}📋 Next steps:${NC}"
echo "1. Update .env file with your credentials"
echo "2. Configure Nginx (see nginx.conf.example)"
echo "3. Set up SSL certificate (Let's Encrypt)"
echo "4. Restart: pm2 restart dashboard"

