from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Response, status
import aiohttp
import time
from pydantic import BaseModel, Field
import random
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from typing import List, Dict, Optional
from .spotify import SpotifyAPI, ProxyConfig
from .token_management import TokenManager, SpotifyAPIEnhanced
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
    """Limit requests when tokens are unavailable"""
    # Skip health endpoint
    if request.url.path.startswith("/health"):
        return await call_next(request)

    # Check if tokens are available for the relevant endpoint
    token_type = "playlist"  # Default
    
    # Determine token type based on path
    if "/artist" in request.url.path:
        token_type = "artist"
    elif "/track" in request.url.path:
        token_type = "track"
    
    # Check if token exists in Redis
    if not await redis_cache.get_token(token_type):
        return Response(
            content=json.dumps({
                "detail": "Service temporarily unavailable. Token worker is refreshing credentials."
            }),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            media_type="application/json"
        )

    # Continue with the request
    return await call_next(request)

# We'll set up the token manager during startup
@app.on_event("startup")
async def startup_event():
    global app_ready
    app_ready = False  # Start in warm-up mode
    
    # Don't initialize the token manager anymore
    # Just initialize GraphQL hash manager
    if hasattr(spotify_api, 'hash_manager'):
        await spotify_api.hash_manager.initialize()
    
    # Schedule the app to become ready after tokens are available
    asyncio.create_task(mark_app_ready())

async def mark_app_ready():
    """Check if tokens exist in Redis before marking app ready"""
    global app_ready

    logger.info("Checking if valid tokens exist in Redis")

    token_types = ['playlist', 'artist', 'track']
    all_tokens_valid = True

    for token_type in token_types:
        if cached := await redis_cache.get_token(token_type):
            # Token exists in Redis
            logger.info(f"Found {token_type} token in Redis")
        else:
            logger.warning(f"No {token_type} token available in Redis")
            all_tokens_valid = False
            break

    if all_tokens_valid:
        logger.info("All tokens are available in Redis, ending warm-up")
        app_ready = True
        return

    # Poll for tokens with exponential backoff
    max_wait = 300  # 5 minutes max
    check_interval = 5
    max_interval = 30
    start_time = time.time()

    logger.info(f"Entering warm-up phase for up to {max_wait} seconds")

    while time.time() - start_time < max_wait:
        all_tokens_valid = True
        
        for token_type in token_types:
            if not await redis_cache.get_token(token_type):
                all_tokens_valid = False
                break
                
        if all_tokens_valid:
            logger.info(f"All tokens available after {int(time.time() - start_time)}s, ending warm-up")
            app_ready = True
            return

        # Exponential backoff with cap
        check_interval = min(check_interval * 1.5, max_interval)
        logger.info(f"Not all tokens available yet, waiting {check_interval:.1f}s")
        await asyncio.sleep(check_interval)

    # Fallback after timeout
    logger.warning("Warm-up timeout reached, marking as ready but tokens may be missing")
    app_ready = True

async def _test_token_with_request(token_type):
    """Test a token with an actual API request to verify it works"""
    try:
        if not spotify_api.session:
            spotify_api.session = aiohttp.ClientSession()

        if token_type == 'playlist':
            # Test with a known public playlist
            playlist_id = "37i9dQZEVXcJZyENOWUFo7"  # A Spotify editorial playlist
            url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?limit=1"
        elif token_type == 'artist':
            # Test with a known artist
            artist_id = "4gzpq5DPGxSnKTe4SA8HAU"  # Very popular artist
            url = f"https://api.spotify.com/v1/artists/{artist_id}"
        elif token_type == 'track':
            # Test with a known track
            track_id = "4cOdK2wGLETKBW3PvgPWqT"  # Popular track
            url = f"https://api.spotify.com/v1/tracks/{track_id}"
        else:
            return False

        headers = {'Authorization': f'Bearer {spotify_api.access_token}'}
        if hasattr(spotify_api, 'client_token') and spotify_api.client_token:
            headers['client-token'] = spotify_api.client_token

        async with spotify_api.session.get(
            url,
            headers=headers,
            proxy=spotify_api.proxy.url,
            proxy_auth=spotify_api.proxy.auth,
            timeout=10
        ) as response:
            if response.status == 200:
                return True
            else:
                logger.warning(f"Token test failed with status {response.status}")
                return False
    except Exception as e:
        logger.error(f"Error in token test request: {e}")
        return False

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

@app.get("/health")
async def health():
    """
    Enhanced health check endpoint with detailed token status.
    
    Returns detailed information about all tokens (current and backups),
    including health metrics and validation status.
    """
    # Check if the app is ready
    app_status = "ready" if app_ready else "warming_up"
    
    # Initialize token status
    token_types = ['playlist', 'artist', 'track']
    tokens_status = {}
    
    # Get detailed status for each token type
    for token_type in token_types:
        token_status = await get_token_status(token_type)
        tokens_status[token_type] = token_status
    
    # Check discovered hash
    discovered_hash = await redis_cache.get_discovered_hash()
    discovered_status = {
        "available": discovered_hash is not None,
    }
    
    return {
        "status": app_status,
        "tokens": tokens_status,
        "discovered_hash": discovered_status,
        "timestamp": int(time.time())
    }

async def get_token_status(token_type: str):
    """
    Get detailed status for all tokens of a specific type.
    
    Args:
        token_type (str): Type of token
        
    Returns:
        dict: Token status information
    """
    # Get all tokens for this type
    tokens = await redis_cache.get_all_tokens(token_type)
    
    # If no tokens, return empty status
    if not tokens:
        return {
            "available": False,
            "message": "No tokens available",
            "backups_available": 0
        }
    
    # Check validation status of current token
    current_valid = False
    if tokens and len(tokens) > 0:
        current = tokens[0]
        
        # Get age
        proxy_data = current['proxy']
        if isinstance(proxy_data, str):
            import json
            proxy_data = json.loads(proxy_data)
        
        created_at = proxy_data.get('created_at', 0)
        age = int(time.time()) - created_at
        
        # Get health metrics
        health_metrics = current.get('health_metrics', {})
        
        # Check if current token is valid (assume valid if fresh)
        if age < 300:  # Less than 5 minutes old
            current_valid = True
        else:
            # For older tokens, check health metrics
            consecutive_failures = health_metrics.get('consecutive_failures', 0)
            success_rate = health_metrics.get('success_rate', 1.0)
            
            # Consider valid if not too many failures and decent success rate
            current_valid = consecutive_failures < 3 and success_rate > 0.7
    
    # Count valid backups
    valid_backups = 0
    backup_details = []
    
    for token in tokens[1:]:  # Skip current token
        backup_index = token.get('backup_index', 0)
        
        # Get basic info
        proxy_data = token['proxy']
        if isinstance(proxy_data, str):
            import json
            proxy_data = json.loads(proxy_data)
        
        created_at = proxy_data.get('created_at', 0)
        age = int(time.time()) - created_at
        
        # Get health metrics
        health_metrics = token.get('health_metrics', {})
        consecutive_failures = health_metrics.get('consecutive_failures', 0)
        success_rate = health_metrics.get('success_rate', 1.0)
        
        # Consider valid if not too many failures and decent success rate
        is_valid = consecutive_failures < 3 and success_rate > 0.7
        
        if is_valid:
            valid_backups += 1
        
        # Add backup details
        backup_details.append({
            "index": backup_index,
            "age_seconds": age,
            "health": {
                "success_rate": success_rate,
                "consecutive_failures": consecutive_failures,
                "last_used": health_metrics.get('last_used', 0)
            },
            "valid": is_valid
        })
    
    # Return comprehensive status
    return {
        "available": len(tokens) > 0,
        "current_valid": current_valid,
        "current_age_seconds": age if tokens else None,
        "backups_available": len(tokens) - 1,
        "valid_backups": valid_backups,
        "backups": backup_details if len(backup_details) > 0 else None,
        "health": health_metrics if tokens else None
    }

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

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
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
    top_tracks: Optional[bool] = Field(default=False)  # Add new parameter

    class Config:
        extra = "forbid"
        validate_assignment = True

@app.get("/api/artist/{artist_id}")
async def get_artist(
        artist_id: str,
        detail: bool = False,
        official: bool = False,
        top_tracks: bool = False  # New parameter
        ):
    """
    Get a single artist by ID.
    Use official=true to use Spotify's public API instead of partner API.
    Optionally return detailed data when detail=true.
    Include top tracks when top_tracks=true.
    """
    try:
        # First try to get the artist
        results = await spotify_api.get_artists(
            [artist_id], 
            skip_cache=True, 
            detail=detail, 
            official=official,
            top_tracks=top_tracks  # Pass new parameter
        )

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
    Optionally return detailed data, use official Spotify API, or include top tracks."""
    try:
        if not request.ids:
            raise HTTPException(status_code=400, detail="No artist IDs provided")

        artist_ids = request.ids[:200]

        results = await spotify_api.get_artists(
            artist_ids,
            detail=request.detail,
            official=request.official,
            top_tracks=request.top_tracks  # Pass new parameter
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
    official: Optional[bool] = Field(default=False)

@app.get("/api/track/{track_id}")
async def get_track(
    track_id: str,
    official: bool = False,  # Add official parameter
):
    """
    Get a single track by ID.
    Use official=true to use Spotify's public API instead of partner API.
    """
    try:
        results = await spotify_api.get_tracks([track_id], skip_cache=True, official=official)

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
    """Get multiple tracks by their IDs, optionally using Spotify's official API"""
    try:
        if not request.ids:
            raise HTTPException(status_code=400, detail="No track IDs provided")

        track_ids = request.ids[:200]
        results = await spotify_api.get_tracks(track_ids, official=request.official)  # Add official parameter

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
