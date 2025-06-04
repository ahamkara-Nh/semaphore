# Semaphore
Semaphore - low-FODMAP diet tracker

## Deployment Guide for Ubuntu VPS

### Prerequisites
- Ubuntu server (tested on Ubuntu 20.04/22.04)
- Python 3.8+ installed
- Git (optional, for pulling from repository)

### Option 1: Standard Python Deployment

1. Clone or upload the project to your server
   ```bash
   git clone https://your-repository-url.git
   # or upload files manually
   ```

2. Navigate to the project directory
   ```bash
   cd semaphore
   ```

3. Create and activate a virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

4. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

5. Create or update your .env file
   ```bash
   nano .env
   ```
   Add the following:
   ```
   BOT_TOKEN=your_telegram_bot_token
   DATABASE_URL=./test.db
   ```

6. Run using systemd service
   ```bash
   sudo cp semaphore.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable semaphore
   sudo systemctl start semaphore
   ```

7. Check service status
   ```bash
   sudo systemctl status semaphore
   ```

### Option 2: Docker Deployment

1. Install Docker and Docker Compose
   ```bash
   sudo apt update
   sudo apt install docker.io docker-compose
   sudo systemctl enable docker
   sudo systemctl start docker
   ```

2. Clone or upload the project to your server
   ```bash
   git clone https://your-repository-url.git
   # or upload files manually
   ```

3. Navigate to the project directory
   ```bash
   cd semaphore
   ```

4. Create or update your .env file
   ```bash
   nano .env
   ```
   Add the following:
   ```
   BOT_TOKEN=your_telegram_bot_token
   DATABASE_URL=./test.db
   ```

5. Build and run with Docker Compose
   ```bash
   docker-compose up -d --build
   ```

6. Check running containers
   ```bash
   docker-compose ps
   ```

### Monitoring and Management

- View logs (systemd):
  ```bash
  sudo journalctl -u semaphore -f
  ```

- View logs (Docker):
  ```bash
  docker-compose logs -f
  ```

- Restart service (systemd):
  ```bash
  sudo systemctl restart semaphore
  ```

- Restart service (Docker):
  ```bash
  docker-compose restart
  ```
