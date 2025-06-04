<<<<<<< Updated upstream
# Semaphore - Low-FODMAP Diet Tracker Backend

## Deployment Instructions

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- A VPS with Ubuntu/Debian (recommended) or other Linux distribution

### Setup Steps

1. Clone the repository to your VPS:
```bash
git clone <your-repository-url>
cd semaphore
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file with the following variables:
```
BOT_TOKEN=your_telegram_bot_token
MINI_APP_URL=your_mini_app_url
GROQ_API_KEY=your_groq_api_key
```

5. Initialize the database:
```bash
python database.py
```

### Running the Application

#### Development
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

#### Production
For production deployment, it's recommended to use a process manager like systemd.

1. Create a systemd service file:
```bash
sudo nano /etc/systemd/system/semaphore.service
```

2. Add the following content (adjust paths as needed):
```ini
[Unit]
Description=Semaphore Backend
After=network.target

[Service]
User=your_user
WorkingDirectory=/path/to/semaphore
Environment="PATH=/path/to/semaphore/venv/bin"
ExecStart=/path/to/semaphore/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

[Install]
WantedBy=multi-user.target
```

3. Start and enable the service:
```bash
sudo systemctl start semaphore
sudo systemctl enable semaphore
```

### Security Considerations

1. Set up a firewall (UFW recommended):
```bash
sudo ufw allow 8000  # If running directly
sudo ufw allow 80    # If using reverse proxy
sudo ufw allow 443   # For HTTPS
```

2. Set up Nginx as a reverse proxy (recommended):
```nginx
server {
    listen 80;
    server_name your_domain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

3. Set up SSL with Let's Encrypt:
```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your_domain.com
```

### Monitoring and Logs

- View application logs:
```bash
sudo journalctl -u semaphore.service -f
```

- Monitor system resources:
```bash
htop
```

### Backup

Regular database backups are recommended:
```bash
# Create a backup directory
mkdir -p /path/to/backups

# Backup script (create as backup.sh)
#!/bin/bash
timestamp=$(date +%Y%m%d_%H%M%S)
cp test.db "/path/to/backups/test_${timestamp}.db"
```
=======
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
>>>>>>> Stashed changes
