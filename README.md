# Spotify API Service

A FastAPI service for fetching and caching Spotify data, including playlists, artists, and discovered-on information.

## Features

- FastAPI backend with async support
- Redis caching for tokens and query hashes
- SQLite database for data caching
- Playwright for browser automation
- Docker and Docker Compose support
- Rate limiting and proxy support

## Setup

### Prerequisites

- Python 3.11+
- Redis
- Docker (optional)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/spotify-api-python.git
cd spotify-api-python
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Install Playwright browsers:
```bash
playwright install chromium
```

### Running with Docker

```bash
docker-compose up --build
```

### Running Locally

1. Start Redis server
2. Run the FastAPI application:
```bash
uvicorn app.main:app --reload
```

## Production Deployment with Systemd

The application consists of two services:
1. Main API service (`spotify.service`)
2. Token Worker service (`spotify-token-worker.service`) - for handling token refresh

### Setup Instructions

1. Clone the repository to your server (if not already done):
```bash
git clone https://github.com/yourusername/spotify-api-python.git /opt/spotify-api
cd /opt/spotify-api
```

2. Set up Python virtual environment:
```bash
python3 -m venv /opt/spotify-venv
source /opt/spotify-venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

3. Create necessary log directories:
```bash
sudo mkdir -p /var/log/spotify
sudo chmod 755 /var/log/spotify
```

4. Create symbolic links to the systemd service files:
```bash
sudo ln -sf /opt/spotify-api/systemd/spotify.service /etc/systemd/system/spotify.service
sudo ln -sf /opt/spotify-api/systemd/spotify-token-worker.service /etc/systemd/system/spotify-token-worker.service
```

5. Reload systemd and enable services:
```bash
sudo systemctl daemon-reload
sudo systemctl enable spotify.service
sudo systemctl enable spotify-token-worker.service
```

6. Start the services:
```bash
sudo systemctl start spotify-token-worker.service
sudo systemctl start spotify.service
```

7. Check service status:
```bash
sudo systemctl status spotify-token-worker.service
sudo systemctl status spotify.service
```

### Service Management

- Restart services:
```bash
sudo systemctl restart spotify-token-worker.service
sudo systemctl restart spotify.service
```

- View logs:
```bash
sudo tail -f /var/log/spotify/token-worker.log
sudo tail -f /var/log/spotify/spotify.log
```

- Stop services:
```bash
sudo systemctl stop spotify.service
sudo systemctl stop spotify-token-worker.service
```

## API Endpoints

- `GET /api/playlist/{playlist_id}`: Get playlist details
- `POST /api/playlists`: Get multiple playlists
- `GET /api/artist/{artist_id}`: Get artist details
- `POST /api/artists`: Get multiple artists
- `GET /api/discovered-on/{artist_id}`: Get discovered-on data
- `POST /api/discovered-on`: Get discovered-on data for multiple artists
- `GET /api/track/{track_id}`: Get track details
- `POST /api/tracks`: Get multiple tracks

## Configuration

The service can be configured using environment variables:

- `REDIS_HOST`: Redis host (default: "localhost")
- `REDIS_PORT`: Redis port (default: 6379)
- `DATABASE_PATH`: SQLite database path (default: "spotify_cache.db")
