from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel, Field
import random
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from pydantic import BaseModel
from typing import List, Dict, Optional
from .spotify import SpotifyAPI
from .database import AsyncDatabase
from .cache import RedisCache
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class PlaylistRequest(BaseModel):
	ids: List[str]
	with_tracks: Optional[bool] = False

app = FastAPI()
spotify_api = SpotifyAPI()
redis_cache = RedisCache()

app.add_middleware(
		CORSMiddleware,
		allow_origins=["*"],
		allow_methods=["*"],
		allow_headers=["*"],
		)

async def refresh_token_task():
	"""Token refresh task with proper error handling for all token types and hashes"""
	try:
		# Refresh playlist token
		if await redis_cache.acquire_lock('playlist'):
			try:
				await spotify_api.refresh_token('playlist')
			finally:
				await redis_cache.release_lock('playlist')

		# Refresh artist token
		if await redis_cache.acquire_lock('artist'):
			try:
				await spotify_api.refresh_token('artist')
			finally:
				await redis_cache.release_lock('artist')

		# Refresh track token
		if await redis_cache.acquire_lock('track'):
			try:
				await spotify_api.refresh_token('track')
			finally:
				await redis_cache.release_lock('track')

		# Refresh discovered-on hash if needed
		if await redis_cache.acquire_lock('discovered_hash'):
			try:
				discovered_hash = await spotify_api._get_discovered_hash()
				if discovered_hash:
					await redis_cache.save_discovered_hash(discovered_hash)
			finally:
				await redis_cache.release_lock('discovered_hash')

	except Exception as e:
		logger.error(f"Token refresh error: {str(e)}")
		raise

@app.on_event("startup")
@repeat_every(seconds=300)  # Run every 5 minutes
async def scheduled_token_refresh():
	# Add random jitter between 0-30 seconds
	jitter = random.uniform(0, 30)
	await asyncio.sleep(jitter)
	await refresh_token_task()

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

@app.on_event("startup")
@repeat_every(seconds=300)  # Run every 5 minutes
async def scheduled_browser_cleanup():
	"""Periodic task to clean up any orphaned browser processes"""
	from .browser_cleanup import cleanup_browsers  # Import the new cleanup function
	await cleanup_browsers()

@app.on_event("shutdown")
async def shutdown_event():
	logger.info("Shutting down application...")
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
        logger.info("Parsed request model: %s", request.dict())

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
