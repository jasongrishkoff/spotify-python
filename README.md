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

## API Endpoints

- `GET /api/playlist/{playlist_id}`: Get playlist details
- `POST /api/playlists`: Get multiple playlists
- `GET /api/artist/{artist_id}`: Get artist details
- `POST /api/artists`: Get multiple artists
- `GET /api/discovered-on/{artist_id}`: Get discovered-on data
- `POST /api/discovered-on`: Get discovered-on data for multiple artists

## Configuration

The service can be configured using environment variables:

- `REDIS_HOST`: Redis host (default: "localhost")
- `REDIS_PORT`: Redis port (default: 6379)
- `DATABASE_PATH`: SQLite database path (default: "spotify_cache.db")
