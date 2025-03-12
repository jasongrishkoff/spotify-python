from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Response, status
import time
from pydantic import BaseModel, Field
import random
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from typing import List, Dict, Optional
from .spotify import SpotifyAPI, ProxyConfig
from .token_management import TokenManager, SpotifyAPIEnhanced, setup_token_management
from .database import AsyncDatabase
from .cache import RedisCache
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class PlaylistRequest(BaseModel):
    ids: List[str]
    with_tracks: Optional[bool] = False

app_ready = False
app = FastAPI()

# Use the enhanced SpotifyAPI class
spotify_api = SpotifyAPIEnhanced()
redis_cache = RedisCache()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def limit_requests_on_token_failure(request: Request, call_next):
    """Prevent request floods during token failures"""
    # Skip health endpoint and warm-up check
    if request.url.path.startswith("/health"):
        return await call_next(request)

    # Check circuit breaker status
    if hasattr(spotify_api, 'circuit_breaker'):
        try:
            if not await spotify_api.circuit_breaker.can_execute():
                return Response(
                    content=json.dumps({
                        "detail": "Service temporarily unavailable due to authentication issues. Please try again later."
                    }),
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    media_type="application/json"
                )
        except Exception as e:
            logger.error(f"Error checking circuit breaker: {e}")

    # Continue with the request
    return await call_next(request)

# We'll set up the token manager during startup
@app.on_event("startup")
async def startup_event():
    global app_ready
    app_ready = False  # Start in warm-up mode
    
    # Initialize the token manager
    await setup_token_management(app, spotify_api, redis_cache)
    logger.info("Token management system initialized")
    
    # Schedule the app to become ready after tokens are refreshed
    asyncio.create_task(mark_app_ready())

async def mark_app_ready():
    """Wait for tokens to be available before marking app ready"""
    global app_ready
    
    # Check if tokens already exist in Redis
    logger.info("Checking if valid tokens already exist")
    
    token_types = ['playlist', 'artist', 'track']
    all_tokens_valid = True
    
    for token_type in token_types:
        if cached := await redis_cache.get_token(token_type):
            proxy_data = cached['proxy']
            if isinstance(proxy_data, str):
                import json
                proxy_data = json.loads(proxy_data)
            
            # If token is less than 10 minutes old, consider it valid
            created_at = proxy_data.get('created_at', 0)
            age = int(time.time()) - created_at
            if age < 600:
                logger.info(f"Found valid {token_type} token (age: {age}s)")
                continue
        
        all_tokens_valid = False
        break
    
    if all_tokens_valid:
        logger.info("All tokens are valid, skipping warm-up")
        app_ready = True
        return
    
    # Otherwise wait for tokens with polling
    max_wait = 120
    check_interval = 5
    start_time = time.time()
    
    logger.info(f"Entering warm-up phase for up to {max_wait} seconds")
    
    while time.time() - start_time < max_wait:
        all_tokens_present = True
        for token_type in token_types:
            if not await redis_cache.get_token(token_type):
                all_tokens_present = False
                break
        
        if all_tokens_present:
            logger.info(f"All tokens available after {int(time.time() - start_time)}s, ending warm-up")
            app_ready = True
            return
        
        await asyncio.sleep(check_interval)
    
    # Fallback to timeout
    app_ready = True
    logger.info("Warm-up complete (timeout reached)")

# Add middleware to check app readiness
@app.middleware("http")
async def check_app_ready(request: Request, call_next):
    if not app_ready and not request.url.path.startswith("/health"):
        return Response(
            content=json.dumps({"detail": "Service is warming up, please try again later"}),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            media_type="application/json"
        )
    return await call_next(request)

# Add a health endpoint that works during warm-up
@app.get("/health")
async def health():
    return {"status": "warming_up" if not app_ready else "ready"}

@app.on_event("startup")
@repeat_every(seconds=604800)  # Run once per week
async def scheduled_db_maintenance():
    """Run database optimization with minimal impact"""
    try:
        # Add a large jitter to spread instances across the week
        jitter = random.uniform(0, 86400)  # Up to 24 hours jitter
        await asyncio.sleep(jitter)
        
        logger.info("Starting database optimization")
        
        async with aiosqlite.connect(AsyncDatabase().path) as db:
            start_time = time.time()
            
            # Run VACUUM to reclaim space
            await db.execute("VACUUM")
            
            # Run OPTIMIZE to defragment the database
            await db.execute("PRAGMA optimize")
            
            # Reset the WAL file
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
            duration = time.time() - start_time
            logger.info(f"Database optimization completed in {duration:.2f}s")
            
    except Exception as e:
        logger.error(f"Error during database optimization: {e}")

# Keep the database cleanup task
@app.on_event("startup")
@repeat_every(seconds=3600)  # Run every hour
async def scheduled_cleanup():
    """Run database cleanup every hour with error handling and logging"""
    try:
        logger.info(f"Starting scheduled cleanup at {datetime.now()}")
        db = AsyncDatabase()

        # Add jitter to prevent all instances cleaning up simultaneously
        jitter = random.uniform(0, 60)  # Random delay between 0-60 seconds
        await asyncio.sleep(jitter)

        start_time = datetime.now()
        await db.cleanup_old_records()
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"Completed cleanup in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in scheduled cleanup: {e}")
        # Don't raise the error - we want the scheduler to continue running

# Keep the browser cleanup task
@app.on_event("startup")
@repeat_every(seconds=300)  # Run every 5 minutes
async def scheduled_browser_cleanup():
    """Periodic task to clean up any orphaned browser processes"""
    from .browser_cleanup import cleanup_browsers
    await cleanup_browsers()

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
    from .browser_cleanup import cleanup_browsers
    await cleanup_browsers()
    await spotify_api.close()

@app.get("/api/playlist/{playlist_id}")
async def get_playlist(
		playlist_id: str,
		with_tracks: bool = False,
		raw_data: bool = False,
		):
	"""Get a single playlist by ID"""
	try:
		# First try to get the playlist
		results = await spotify_api.get_playlists([playlist_id], with_tracks=with_tracks, skip_cache=True, raw_data=raw_data)

		if not results:
			raise HTTPException(status_code=404, detail="Playlist not found")

		# Safely handle dictionary access
		playlist = None
		try:
			playlist = results.get(playlist_id)
		except AttributeError as e:
			raise HTTPException(status_code=500, detail="Invalid response format")

		if not playlist:
			raise HTTPException(status_code=404, detail="Playlist not found")

		return playlist

	except Exception as e:
		logger.error(f"Error fetching playlist {playlist_id}: {e}")
		logger.error(f"Error type: {type(e)}")
		import traceback
		logger.error(f"Traceback: {traceback.format_exc()}")
		raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/playlists")
async def get_playlists(
		request: PlaylistRequest,
		):
	"""Get multiple playlists by their IDs"""
	try:
		if not request.ids:
			raise HTTPException(status_code=400, detail="No playlist IDs provided")

		playlist_ids = request.ids[:200]
		results = await spotify_api.get_playlists(playlist_ids, with_tracks=request.with_tracks)

		# Convert to array and filter out None values
		valid_results = [
				data
				for data in results.values()
				if data is not None
				]

		if not valid_results:
			raise HTTPException(status_code=404, detail="No valid playlists found")

		return valid_results
	except Exception as e:
		logger.error(f"Error fetching playlists: {e}")
		raise HTTPException(status_code=500, detail="Internal server error")

class ArtistRequest(BaseModel):
    ids: List[str]
    detail: Optional[bool] = Field(default=False)
    official: Optional[bool] = Field(default=False)
    with_tracks: Optional[bool] = Field(default=False)  # Add this field

    class Config:
        extra = "forbid"
        validate_assignment = True

@app.get("/api/artist/{artist_id}")
async def get_artist(
		artist_id: str,
		detail: bool = False,
		official: bool = False  # New parameter
		):
	"""
	Get a single artist by ID.
	Use official=true to use Spotify's public API instead of partner API.
	Optionally return detailed data when detail=true
	"""
	try:
		# First try to get the artist
		results = await spotify_api.get_artists([artist_id], skip_cache=True, detail=detail, official=official)

		if not results:
			logger.warning(f"No results returned for artist {artist_id}")
			raise HTTPException(status_code=404, detail="Artist not found")

		# Safely handle dictionary access
		artist = None
		try:
			artist = results.get(artist_id)
		except AttributeError as e:
			logger.error(f"Results is not a dictionary. Type: {type(results)}")
			logger.error(f"Results content: {results}")
			raise HTTPException(status_code=500, detail="Invalid response format")

		if not artist:
			logger.warning(f"Artist {artist_id} not found in results dictionary")
			raise HTTPException(status_code=404, detail="Artist not found")

		return artist

	except Exception as e:
		logger.error(f"Error fetching artist {artist_id}: {e}")
		logger.error(f"Error type: {type(e)}")
		import traceback
		logger.error(f"Full traceback: {traceback.format_exc()}")
		if isinstance(e, HTTPException):
			raise
		raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/artists")
async def get_artists(request: ArtistRequest):
    """Get multiple artists by their IDs.
    Optionally return detailed data or use official Spotify API."""
    try:
        if not request.ids:
            raise HTTPException(status_code=400, detail="No artist IDs provided")

        # Debug the parsed request
        #logger.info("Parsed request model: %s", request.dict())

        artist_ids = request.ids[:200]

        results = await spotify_api.get_artists(
            artist_ids,
            detail=request.detail,
            official=request.official
        )

        # Convert to array and filter out None values
        valid_results = [
            data
            for data in results.values()
            if data is not None
        ]

        if not valid_results:
            raise HTTPException(status_code=404, detail="No valid artists found")

        return valid_results

    except Exception as e:
        logger.error(f"Error fetching artists: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

class DiscoveredRequest(BaseModel):
	ids: List[str]

@app.get("/api/discovered-on/{artist_id}")
async def get_discovered_on(
	artist_id: str,
):
	"""Get discovered-on data for a single artist"""
	try:
		results = await spotify_api.get_discovered_on([artist_id], skip_cache=True)

		if not results:
			raise HTTPException(status_code=404, detail="Artist not found")

		artist_data = results.get(artist_id)
		if not artist_data:
			raise HTTPException(status_code=404, detail="Artist not found")

		return artist_data

	except Exception as e:
		logger.error(f"Error fetching discovered-on {artist_id}: {e}")
		if isinstance(e, HTTPException):
			raise
		raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/discovered-on")
async def get_multiple_discovered_on(
	request: DiscoveredRequest,
):
	"""Get discovered-on data for multiple artists"""
	try:
		if not request.ids:
			raise HTTPException(status_code=400, detail="No artist IDs provided")

		discovered_on_ids = request.ids[:200]
		results = await spotify_api.get_discovered_on(discovered_on_ids)

		if not results:
			raise HTTPException(status_code=404, detail="No valid artists found")

		return list(results.values())

	except Exception as e:
		logger.error(f"Error fetching discovered-on: {e}")
		if isinstance(e, HTTPException):
			raise
		raise HTTPException(status_code=500, detail="Internal server error")

class TrackRequest(BaseModel):
	ids: List[str]

@app.get("/api/track/{track_id}")
async def get_track(
	track_id: str,
):
	"""Get a single track by ID"""
	try:
		results = await spotify_api.get_tracks([track_id], skip_cache=True)

		if not results:
			raise HTTPException(status_code=404, detail="Track not found")

		track = results.get(track_id)
		if not track:
			raise HTTPException(status_code=404, detail="Track not found")

		return track

	except Exception as e:
		logger.error(f"Error fetching track {track_id}: {e}")
		if isinstance(e, HTTPException):
			raise
		raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/track-detail/{track_id}")
async def get_track_detail(
		track_id: str,
		):
	"""Get detailed track information by ID"""
	try:
		results = await spotify_api.get_tracks([track_id], skip_cache=True, detail=True)

		if not results:
			raise HTTPException(status_code=404, detail="Track not found")

		track = results.get(track_id)
		if not track:
			raise HTTPException(status_code=404, detail="Track not found")

		return track

	except Exception as e:
		logger.error(f"Error fetching track detail {track_id}: {e}")
		if isinstance(e, HTTPException):
			raise
		raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/tracks")
async def get_tracks(
		request: TrackRequest,
		):
	"""Get multiple tracks by their IDs"""
	try:
		if not request.ids:
			raise HTTPException(status_code=400, detail="No track IDs provided")

		track_ids = request.ids[:200]
		results = await spotify_api.get_tracks(track_ids)

		# Convert to array and filter out None values
		valid_results = [
				data
				for data in results.values()
				if data is not None
				]

		if not valid_results:
			raise HTTPException(status_code=404, detail="No valid tracks found")

		return valid_results

	except Exception as e:
		logger.error(f"Error fetching tracks: {e}")
		raise HTTPException(status_code=500, detail="Internal server error")
