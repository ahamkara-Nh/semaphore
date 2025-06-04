#!/bin/bash

# Semaphore Deployment Script
echo "Semaphore Deployment Script"
echo "============================"

# Check if script is run with sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run this script with sudo privileges"
  exit 1
fi

# Ask for deployment method
echo "Select deployment method:"
echo "1) Standard Python Deployment"
echo "2) Docker Deployment"
read -p "Enter your choice (1 or 2): " deployment_choice

case $deployment_choice in
  1)
    echo "Starting Standard Python Deployment..."
    
    # Check for Python
    if ! command -v python3 &> /dev/null; then
      echo "Python 3 is not installed. Installing..."
      apt update
      apt install -y python3 python3-venv python3-pip
    fi
    
    # Create and activate venv
    python3 -m venv venv
    source venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt
    
    # Check for .env file
    if [ ! -f .env ]; then
      echo "Creating .env file..."
      read -p "Enter your Telegram BOT_TOKEN: " bot_token
      echo "BOT_TOKEN=$bot_token" > .env
      echo "DATABASE_URL=./test.db" >> .env
    fi
    
    # Set up systemd service
    cp semaphore.service /etc/systemd/system/
    systemctl daemon-reload
    systemctl enable semaphore
    systemctl start semaphore
    
    echo "Deployment completed! Service is running."
    echo "Check status with: sudo systemctl status semaphore"
    ;;
    
  2)
    echo "Starting Docker Deployment..."
    
    # Check for Docker
    if ! command -v docker &> /dev/null; then
      echo "Docker is not installed. Installing Docker and Docker Compose..."
      apt update
      apt install -y docker.io docker-compose
      systemctl enable docker
      systemctl start docker
    fi
    
    # Check for .env file
    if [ ! -f .env ]; then
      echo "Creating .env file..."
      read -p "Enter your Telegram BOT_TOKEN: " bot_token
      echo "BOT_TOKEN=$bot_token" > .env
      echo "DATABASE_URL=./test.db" >> .env
    fi
    
    # Build and run with Docker Compose
    docker-compose up -d --build
    
    echo "Deployment completed! Docker containers are running."
    echo "Check status with: docker-compose ps"
    ;;
    
  *)
    echo "Invalid choice. Exiting."
    exit 1
    ;;
esac 