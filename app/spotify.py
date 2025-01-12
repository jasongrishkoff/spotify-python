import asyncio
import urllib.parse
import aiohttp
import json
import time
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from contextlib import asynccontextmanager
from playwright.async_api import async_playwright
from .database import AsyncDatabase
from .cache import RedisCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ProxyConfig:
    address: str
    port: int
    username: str
    password: str
    created_at: int = 0  
    
    @classmethod
    def from_response(cls, response: Dict) -> 'ProxyConfig':
        return cls(
            address=response['proxy_address'],
            port=response['ports']['http'],
            username=response['username'],
            password=response['password']
        )
        
    @property
    def url(self) -> str:
        return f"http://{self.address}:{self.port}"
        
    @property
    def auth(self) -> aiohttp.BasicAuth:
        return aiohttp.BasicAuth(self.username, self.password)
        
    def to_playwright_config(self) -> Dict:
        return {
            'server': self.url,
            'username': self.username,
            'password': self.password
        }
        
    def __str__(self):
        return f"Proxy({self.address}:{self.port})"

@dataclass
class RateLimiter:
    rate: int  # requests per second
    max_concurrent: int
    
    def __init__(self, rate: int = 100, max_concurrent: int = 20):
        self.rate = rate
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._request_times: List[float] = []
        
    async def acquire(self):
        await self._semaphore.acquire()
        now = time.time()
        
        # Remove old timestamps
        self._request_times = [t for t in self._request_times if now - t < 1.0]
        
        if len(self._request_times) >= self.rate:
            delay = 1.0 - (now - self._request_times[0])
            if delay > 0:
                await asyncio.sleep(delay)
                
        self._request_times.append(time.time())
        
    def release(self):
        self._semaphore.release()

class SpotifyAPI:
    MAX_RETRIES = 3

    def __init__(self, db_path: str = 'spotify_cache.db'):
        self.db = AsyncDatabase(db_path)
        self.rate_limiter = RateLimiter()
        self.session: Optional[aiohttp.ClientSession] = None
        self._init_task = None
        self.access_token: Optional[str] = None
        self.proxy: Optional[ProxyConfig] = None
        self.cache = RedisCache()  # Redis cache instance

    async def _ensure_initialized(self):
        if self._init_task is None:
            self._init_task = asyncio.create_task(self._initialize())
        await self._init_task
        
    async def _initialize(self):
        await self.db._init_db()
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def _get_proxy(self) -> Optional[ProxyConfig]:
        if self.proxy:
            return self.proxy

        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get('https://api.submithub.com/api/proxy', timeout=15) as response:
                if response.status == 200:
                    self.proxy = ProxyConfig.from_response(await response.json())
                    logger.info(f"Got proxy: {self.proxy}")
                    return self.proxy
                logger.error(f"Failed to get proxy, status: {response.status}")
        except Exception as e:
            logger.error(f"Proxy error: {e}")
        return None

    @asynccontextmanager
    async def _browser_session(self, retry_count: int = 0):
        """Managed browser session with proper cleanup"""
        browser = None
        context = None
        try:
            logger.info(f"Starting browser (attempt {retry_count + 1}/{self.MAX_RETRIES})")
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    proxy=self.proxy.to_playwright_config() if self.proxy else None,
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--single-process',
                    ]
                )
                # Create the context immediately after launching the browser
                context = await browser.new_context()
                yield browser
        except Exception as e:
            logger.error(f"Browser session error: {str(e)}")
            raise
        finally:
            try:
                if context:
                    # Close context first - this will automatically close all associated pages
                    await context.close()
                if browser:
                    await browser.close()
                logger.info("Browser cleanup completed")
            except Exception as e:
                logger.error(f"Error during browser cleanup: {str(e)}")

    async def _ensure_auth(self, token_type: str = 'playlist') -> bool:
        # Quick check if we already have a valid token
        if cached := await self.cache.get_token(token_type):
            cached_time = cached['proxy'].get('created_at', 0)
            current_time = int(time.time())

            if current_time - cached_time < 480:  # 8 minutes
                self.access_token = cached['access_token']
                self.proxy = ProxyConfig(**cached['proxy'])
                return True

        # Use exponential backoff for waiting
        max_attempts = 5
        for attempt in range(max_attempts):
            if await self.cache.acquire_lock(token_type, timeout=30):
                try:
                    success = await self._get_token(token_type)
                    if success:
                        return True
                    logger.error("Token refresh failed")
                finally:
                    await self.cache.release_lock(token_type)

            # Exponential backoff wait
            wait_time = min(2 ** attempt, 8)  # Max 8 second wait
            await asyncio.sleep(wait_time)

            # Check if another process refreshed the token
            if cached := await self.cache.get_token(token_type):
                self.access_token = cached['access_token']
                self.proxy = ProxyConfig(**cached['proxy'])
                return True

        return False

    async def _get_token(self, token_type: str = 'playlist') -> bool:
        """
        Get a new token using browser automation.
        Returns True if successful, False otherwise.
        """
        for retry in range(self.MAX_RETRIES):
            try:
                if not self.proxy:
                    self.proxy = await self._get_proxy()
                if not self.proxy:
                    logger.error("Failed to get proxy")
                    continue

                async with self._browser_session(retry) as browser:
                    logger.info(f"Browser launched (attempt {retry + 1})")
                    context = await browser.new_context()
                    page = await context.new_page()

                    # Set a strict timeout for the entire operation
                    try:
                        async with asyncio.timeout(45):  # 45 second total timeout
                            await page.goto('https://open.spotify.com', timeout=30000)
                            await page.wait_for_load_state('networkidle', timeout=30000)

                            token_info = await page.evaluate('''() => {
                                const script = document.getElementById('session');
                                return script ? JSON.parse(script.text.trim()) : null;
                            }''')

                            if not token_info or not token_info.get('accessToken'):
                                logger.error("No token found in page")
                                continue

                            self.access_token = token_info['accessToken']

                            # Save to Redis with current timestamp
                            await self.cache.save_token(
                                token=self.access_token,
                                proxy=self.proxy.__dict__,
                                token_type=token_type
                            )
                            return True

                    except asyncio.TimeoutError:
                        logger.error("Timeout during token acquisition")
                        continue

            except Exception as e:
                logger.error(f"Error in token acquisition: {str(e)}")
                self.proxy = None
                await asyncio.sleep(1)

        logger.error("All token refresh attempts failed")
        return False

    async def get_playlists(
            self,
            playlist_ids: List[str],
            with_tracks: bool = False,
            skip_cache: bool = False,
    ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()
        # Get unique playlist IDs while preserving order
        unique_ids = list(dict.fromkeys(playlist_ids))

        if skip_cache:
            # If skipping cache, fetch all playlists directly
            fetch_tasks = [
                self._fetch_playlist(pid, with_tracks)
                for pid in unique_ids
            ]
            fetched_playlists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_playlists = [
                p for p in fetched_playlists
                if isinstance(p, dict) and p.get('id')
            ]
            # Still save to cache for future requests
            if valid_playlists:
                await self.db.save_playlists(valid_playlists)
            return {
                p['id']: self._format_playlist(p, with_tracks)
                for p in valid_playlists
            }

        # Get all cached playlists
        cached_playlists = await self.db.get_playlists(unique_ids)
        # Determine which playlists need to be fetched
        to_fetch = []
        for pid in unique_ids:
            cached = cached_playlists.get(pid)
            needs_fetch = (
                not cached or  # Not cached
                not isinstance(cached, dict) or  # Cached data is invalid
                (with_tracks and not self._has_track_data(cached))  # Cached but missing track data
            )
            if needs_fetch:
                to_fetch.append(pid)
        # Fetch missing or outdated playlists
        if to_fetch:
            fetch_tasks = [
                self._fetch_playlist(pid, with_tracks)
                for pid in to_fetch
            ]
            fetched_playlists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_playlists = [
                p for p in fetched_playlists
                if isinstance(p, dict) and p.get('id')
            ]
            if valid_playlists:
                # Save new playlists to cache
                await self.db.save_playlists(valid_playlists)
                # Update our working set with new data
                cached_playlists.update({p['id']: p for p in valid_playlists})
        # Format and return results, filtering out invalid cached entries
        return {
            pid: self._format_playlist(data, with_tracks) if data and isinstance(data, dict) else None
            for pid, data in cached_playlists.items()
            if not (with_tracks and data and not self._has_track_data(data))
        }


    @staticmethod
    def _has_track_data(playlist: Optional[Dict]) -> bool:
        """
        Check if a playlist has track data cached.

        Args:
            playlist (Optional[Dict]): The playlist data to check

        Returns:
            bool: True if the playlist has track data, False otherwise
        """
        # Handle None case
        if not playlist:
            return False

        # Handle case where playlist might be a string
        if not isinstance(playlist, dict):
            return False

        tracks = playlist.get('tracks', {})
        # Handle case where tracks might be a number (total) instead of dict
        if not isinstance(tracks, dict):
            return False

        return bool(tracks.get('items', []))

    async def _fetch_playlist(self, playlist_id: str, with_tracks: bool = False) -> Optional[Dict]:
        try:
            await self.rate_limiter.acquire()
            
            if not await self._ensure_auth():
                logger.error("Failed to get auth token")
                return None
                
            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.error("No proxy available")
                return None
                
            url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
            if with_tracks:
                url += "?fields=collaborative,id,name,owner,description,followers,images,tracks.total,tracks.items(added_at,track(id,artists))"
                
            async with self.session.get(
                url,
                headers={'Authorization': f'Bearer {self.access_token}'},
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=10
            ) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status in {401, 407}:
                    self.access_token = None
                    self.proxy = None
                    
                logger.warning(f"Failed to fetch playlist {playlist_id}: {response.status}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching playlist {playlist_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_playlist(playlist: Dict, with_tracks: bool = False) -> Dict:
        """
        Format a playlist response from Spotify API into our standard format.
        If the playlist is already formatted, return it as is.
        """
        # If the playlist is already formatted (has our standard fields), return it
        if isinstance(playlist, dict):
            # Check if this is already in our format
            standard_fields = {'id', 'name', 'owner', 'owner_id', 'description', 'followers', 'images'}
            if standard_fields.issubset(playlist.keys()):
                return playlist

        # If not formatted, format it from Spotify API response
        base_data = {
            'id': playlist.get('id'),
            'name': playlist.get('name'),
            'owner': playlist.get('owner', {}).get('display_name'),
            'owner_id': playlist.get('owner', {}).get('id'),
            'description': playlist.get('description'),
            'followers': playlist.get('followers', {}).get('total'),
            'images': playlist.get('images', []),
            'collaborative': playlist.get('collaborative', False)
        }

        if not with_tracks:
            base_data['tracks'] = playlist.get('tracks', {}).get('total')
            return base_data

        # Format track data when requested
        tracks_data = playlist.get('tracks', {})
        formatted_tracks = []

        for item in tracks_data.get('items', []):
            track = item.get('track', {})
            if not track:
                continue

            artists = [
                {
                    'id': artist.get('id'),
                    'name': artist.get('name')
                }
                for artist in track.get('artists', [])
                if artist.get('id') and artist.get('name')
            ]

            formatted_tracks.append({
                'added_at': item.get('added_at'),
                'track': {
                    'id': track.get('id'),
                    'artists': artists
                }
            })

        base_data['tracks'] = {
            'total': tracks_data.get('total'),
            'items': formatted_tracks
        }

        return base_data

    async def refresh_token(self, token_type: str = 'playlist') -> bool:
        """Background task to refresh token if existing token is > 5 minutes old"""
        if cached := await self.cache.get_token(token_type):
            proxy_data = cached['proxy']
            if isinstance(proxy_data, str):
                proxy_data = json.loads(proxy_data)

            # If token is less than 5 minutes old, skip refresh
            if int(time.time()) - proxy_data.get('created_at', 0) < 300:
                return True

        # Use appropriate token getter based on type
        if token_type == 'track':
            return await self._get_track_token()
        else:
            return await self._get_token(token_type)

    async def get_artists(
        self,
        artist_ids: List[str],
        skip_cache: bool = False,
        detail: bool = False
    ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()
        # Get unique artist IDs while preserving order
        unique_ids = list(dict.fromkeys(artist_ids))

        if skip_cache:
            fetch_tasks = [
                self._fetch_artist(aid)
                for aid in unique_ids
            ]
            fetched_artists = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_artists = []
            for i, result in enumerate(fetched_artists):
                if isinstance(result, dict):
                    valid_artists.append(result)

            formatted_results = {}
            for i, artist_data in enumerate(valid_artists):
                formatted = self._format_artist(artist_data, detail)
                if formatted:
                    artist_id = unique_ids[i]
                    formatted_results[artist_id] = formatted

            return formatted_results

        # Get all cached artists
        cached_artists = await self.db.get_artists(unique_ids)

        # Determine which artists need to be fetched
        to_fetch = [
            aid for aid in unique_ids
            if not cached_artists.get(aid) or not isinstance(cached_artists.get(aid), dict)
        ]

        # Fetch missing artists
        if to_fetch:
            fetch_tasks = [
                self._fetch_artist(aid)
                for aid in to_fetch
            ]
            fetched_artists = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            valid_artists = []

            for i, result in enumerate(fetched_artists):
                if isinstance(result, dict):
                    formatted = self._format_artist(result, detail)
                    if formatted:
                        valid_artists.append(formatted)

            # Save to cache and update results
            if valid_artists:
                await self.db.save_artists(valid_artists)
                cached_artists.update({a['id']: a for a in valid_artists})

        # Format and return results
        return {
            aid: cached_artists.get(aid)
            for aid in unique_ids
            if cached_artists.get(aid)
        }

    async def _fetch_artist(self, artist_id: str) -> Optional[Dict]:
        """Fetch artist data from Spotify Partner API"""
        try:
            await self.rate_limiter.acquire()

            if not await self._ensure_auth():
                logger.error("Failed to get auth token")
                return None

            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.error("No proxy available")
                return None

            url = "https://api-partner.spotify.com/pathfinder/v1/query"

            # Updated variables to include required fields
            variables = {
                'uri': f'spotify:artist:{artist_id}',
                'locale': '',
                'includePrerelease': False,
                'enableAssociatedVideos': False,
                'withSaved': True  # Add this field to ensure saved is not null
            }

            params = {
                'operationName': 'queryArtistOverview',
                'variables': json.dumps(variables),
                'extensions': json.dumps({
                    'persistedQuery': {
                        'version': 1,
                        'sha256Hash': '4bc52527bb77a5f8bbb9afe491e9aa725698d29ab73bff58d49169ee29800167'
                    }
                })
            }

            async with self.session.get(
                url,
                params=params,
                headers={
                    'Authorization': f'Bearer {self.access_token}',
                    'content-type': 'application/json',
                    'accept': 'application/json'
                },
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=10
            ) as response:
                if response.status == 200:
                    data = await response.json()

                    if 'errors' in data:
                        error_msg = data.get('errors', [{}])[0].get('message', '')
                        if 'NullValueInNonNullableField' in error_msg:
                            # Handle the specific error by providing a default value
                            if 'data' in data and 'artistUnion' in data['data']:
                                data['data']['artistUnion']['saved'] = False
                        else:
                            #logger.error(f"GraphQL errors in response: {data['errors']}")
                            return None

                    return data

                elif response.status in {401, 407}:
                    logger.error(f"Authorization error {response.status}")
                    self.access_token = None
                    self.proxy = None
                    return None

                logger.warning(f"Failed to fetch artist {artist_id}: {response.status}")
                return None

        except Exception as e:
            logger.error(f"Error fetching artist {artist_id}: {e}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_artist(artist_data: Dict, detail: bool = False) -> Optional[Dict]:
        """Format an artist response from Spotify Partner API"""
        # If detail is True, return the full response
        if detail:
            return artist_data.get('data', {}).get('artistUnion', {})

        # Extract artist data
        artist = artist_data.get('data', {}).get('artistUnion', {})
        if not artist:
            return None

        # Construct base profile
        formatted_data = {
            'id': artist.get('id'),
            'profile': {
                'name': artist.get('profile', {}).get('name'),
            }
        }

        # Add stats if available
        if 'stats' in artist:
            formatted_data['stats'] = {
                'followers': artist.get('stats', {}).get('followers'),
                'monthlyListeners': artist.get('stats', {}).get('monthlyListeners'),
                'topCities': artist.get('stats', {}).get('topCities', [])
            }

        # Add visuals if available
        if 'visuals' in artist:
            avatar_image = artist.get('visuals', {}).get('avatarImage', {})
            if avatar_image and avatar_image.get('sources'):
                formatted_data['image'] = avatar_image.get('sources')[0].get('url')

        # Add genres if available
        if artist.get('profile', {}).get('genres', {}).get('items'):
            formatted_data['genres'] = [
                genre.get('name')
                for genre in artist['profile']['genres']['items']
                if genre.get('name')
            ]

        # Make sure we have at least an ID and name
        if not formatted_data.get('id') or not formatted_data.get('profile', {}).get('name'):
            return None

        return formatted_data

    async def _fetch_discovered_on(self, artist_id: str) -> Optional[Dict]:
        try:
            await self.rate_limiter.acquire()

            if not await self._ensure_auth('artist'):  # Use artist token type
                logger.error("Failed to get auth token")
                return None
                
            # Use the helper method to get the hash
            discovered_hash = await self._ensure_discovered_hash()
            if not discovered_hash:
                logger.error("Could not get discovered-on hash")
                return None

            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.error("No proxy available")
                return None

            url = "https://api-partner.spotify.com/pathfinder/v1/query"

            variables = {
                'uri': f'spotify:artist:{artist_id}',
                'locale': ''
            }

            params = {
                'operationName': 'queryArtistDiscoveredOn',
                'variables': json.dumps(variables),
                'extensions': json.dumps({
                    'persistedQuery': {
                        'version': 1,
                        'sha256Hash': discovered_hash
                    }
                })
            }

            async with self.session.get(
                url,
                params=params,
                headers={
                    'Authorization': f'Bearer {self.access_token}',
                    'content-type': 'application/json',
                    'accept': 'application/json'
                },
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=10
            ) as response:

                if response.status == 200:
                    data = await response.json()

                    if 'errors' in data:
                        logger.error(f"GraphQL errors in response: {data['errors']}")
                        logger.error(f"Full error response: {data}")
                        # If we get PersistedQueryNotFound, clear the cached hash
                        if any(error.get('message') == 'PersistedQueryNotFound' for error in data.get('errors', [])):
                            await self.cache.redis.delete('spotify_discovered_hash')
                        return None

                    return data

                elif response.status in {401, 407}:
                    logger.error(f"Authorization error {response.status}")
                    error_body = await response.text()
                    logger.error(f"Auth error response body: {error_body}")
                    self.access_token = None
                    self.proxy = None
                    return None

                logger.warning(f"Failed to fetch discovered-on {artist_id}: {response.status}")
                error_body = await response.text()
                logger.error(f"Error response body: {error_body}")
                return None

        except Exception as e:
            logger.error(f"Error fetching discovered-on {artist_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        finally:
            self.rate_limiter.release()

    async def _get_discovered_hash(self) -> Optional[str]:
        """Get the discovered-on hash by monitoring actual Spotify web requests"""
        logger.info("Getting discovered-on hash from live request")

        try:
            if not self.proxy:
                self.proxy = await self._get_proxy()
            if not self.proxy:
                logger.error("Failed to get proxy")
                return None

            async with self._browser_session() as browser:
                context = await browser.new_context()
                page = await context.new_page()

                discovered_hash = None
                hash_event = asyncio.Event()

                async def handle_request(route):
                    nonlocal discovered_hash
                    request = route.request
                    if 'queryArtistDiscoveredOn' in request.url:
                        logger.info(f"Found queryArtistDiscoveredOn request: {request.url}")
                        try:
                            parts = request.url.split('=')
                            for part in parts:
                                try:
                                    data = json.loads(urllib.parse.unquote(part))
                                    if data.get('persistedQuery', {}).get('sha256Hash'):
                                        discovered_hash = data['persistedQuery']['sha256Hash']
                                        logger.info(f"Found hash: {discovered_hash}")
                                        hash_event.set()
                                except json.JSONDecodeError:
                                    continue
                        except Exception as e:
                            logger.error(f"Error parsing URL: {e}")
                    await route.continue_()

                # Listen to network requests
                await page.route("**/*", handle_request)

                # Visit a known artist page
                artist_id = "3GBPw9NK25X1Wt2OUvOwY3"
                await page.goto(f'https://open.spotify.com/artist/{artist_id}/discovered-on', timeout=60000)

                # Wait for hash with timeout
                try:
                    await asyncio.wait_for(hash_event.wait(), timeout=30)
                except asyncio.TimeoutError:
                    logger.error("Timeout waiting for discovered hash")
                    return None

                result = None
                if discovered_hash:
                    # Save the hash in Redis for future use
                    await self.cache.save_discovered_hash(discovered_hash)
                    result = discovered_hash
                else:
                    logger.error("Failed to capture discovered-on hash")

                # Cleanup routes before closing
                try:
                    await page.unroute_all(behavior='ignoreErrors')
                    await page.close()
                except Exception as e:
                    logger.error(f"Error during page cleanup: {e}")

                return result

        except Exception as e:
            logger.error(f"Error getting discovered-on hash: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None

    async def _ensure_discovered_hash(self) -> Optional[str]:
        """Ensures we have a valid discovered-on hash, waiting for background refresh if needed"""
        for retry in range(3):
            if discovered_hash := await self.cache.get_discovered_hash():
                return discovered_hash
            await asyncio.sleep(2)
        return None

    @staticmethod
    def _format_discovered_on(data: Dict) -> Optional[Dict]:
        """Format discovered-on response from Spotify Partner API"""
        # Extract data
        artist_data = data.get('data', {}).get('artistUnion', {})
        if not artist_data:
            return None

        # Get the discovered on items
        discovered_items = artist_data.get('relatedContent', {}).get('discoveredOnV2', {}).get('items', [])
        if not discovered_items:
            return None

        # Format playlists
        clean_items = []
        for item in discovered_items:
            playlist_data = item.get('data', {})
            if playlist_data.get('__typename') != 'Playlist':
                continue

            image_url = None
            if playlist_data.get('images', {}).get('items'):
                image_sources = playlist_data['images']['items'][0].get('sources', [])
                if image_sources:
                    image_url = image_sources[0].get('url')

            clean_items.append({
                'id': playlist_data['uri'].split(':')[-1],
                'name': playlist_data.get('name'),
                'owner': playlist_data.get('ownerV2', {}).get('data', {}).get('name'),
                'position': len(clean_items),  # Maintain order
                'image': image_url
            })

        return {
            'data': {
                'artist': {
                    'id': artist_data.get('id'),
                    'relatedContent': {
                        'discoveredOn': {
                            'items': clean_items
                        }
                    }
                }
            }
        }

    async def get_discovered_on(
            self,
            artist_ids: List[str],
            skip_cache: bool = False,
    ) -> Dict[str, Optional[Dict]]:
        await self._ensure_initialized()

        # Get unique artist IDs while preserving order
        unique_ids = list(dict.fromkeys(artist_ids))

        if skip_cache:
            fetch_tasks = [
                self._fetch_discovered_on(aid)
                for aid in unique_ids
            ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            results = {}
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_discovered_on(data)
                    if formatted:
                        results[unique_ids[i]] = formatted

            return results

        # Get all cached data
        cached_data = await self.db.get_discovered_on(unique_ids)

        # Determine which need to be fetched
        to_fetch = [
            aid for aid in unique_ids
            if not cached_data.get(aid) or not isinstance(cached_data.get(aid), dict)
        ]

        # Fetch missing data
        if to_fetch:
            fetch_tasks = [
                self._fetch_discovered_on(aid)
                for aid in to_fetch
            ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_data = []
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_discovered_on(data)
                    if formatted:
                        formatted['id'] = to_fetch[i]  # Add ID for caching
                        valid_data.append(formatted)

            # Save to cache and update results
            if valid_data:
                await self.db.save_discovered_on(valid_data)
                cached_data.update({d['id']: d for d in valid_data})

        return {
            aid: cached_data.get(aid)
            for aid in unique_ids
            if cached_data.get(aid)
        }


    async def _get_track_token(self) -> bool:
        """Get a new track token using browser automation."""
        for retry in range(self.MAX_RETRIES):
            try:
                logger.info(f"Starting track token generation (attempt {retry + 1})")

                if not self.proxy:
                    self.proxy = await self._get_proxy()
                if not self.proxy:
                    logger.error("Failed to get proxy")
                    continue

                logger.info(f"Using proxy: {self.proxy}")

                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        proxy=self.proxy.to_playwright_config() if self.proxy else None,
                        headless=True,
                        args=[
                            '--no-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-gpu',
                            '--single-process',
                        ]
                    )

                    try:
                        context = await browser.new_context()
                        page = await context.new_page()

                        # Track the hash we find
                        hash_value = None

                        async def handle_request(route):
                            nonlocal hash_value
                            url = route.request.url
                            logger.debug(f"Intercepted request: {url}")
                            if 'getTrack&' in url:
                                logger.info(f"Found getTrack request: {url}")
                                try:
                                    # Parse URL to get the query string
                                    parsed = urllib.parse.urlparse(url)
                                    # Get the extensions parameter
                                    params = urllib.parse.parse_qs(parsed.query)
                                    if 'extensions' in params:
                                        ext_json = urllib.parse.unquote(params['extensions'][0])
                                        data = json.loads(ext_json)
                                        if data.get('persistedQuery', {}).get('sha256Hash'):
                                            hash_value = data['persistedQuery']['sha256Hash']
                                            logger.info(f"Found track hash: {hash_value}")
                                except Exception as e:
                                    logger.error(f"Error parsing hash from URL: {e}")
                            await route.continue_()

                        await page.route("**/*", handle_request)

                        # Navigate to a track URL
                        url = 'https://open.spotify.com/track/6K4t31amVTZDgR3sKmwUJJ'
                        logger.info(f"Navigating to {url}")
                        await page.goto(url, timeout=60000)
                        await page.wait_for_load_state('networkidle')
                        await page.wait_for_timeout(5000)

                        # Get page content for debugging if needed
                        content = await page.content()
                        logger.debug(f"Page content: {content[:500]}...")

                        # Get token from page
                        token_info = await page.evaluate('''() => {
                            const script = document.getElementById('session');
                            if (!script) {
                                console.log('No session script found');
                                return null;
                            }
                            try {
                                return JSON.parse(script.text.trim());
                            } catch (e) {
                                console.log('Error parsing session script:', e);
                                return null;
                            }
                        }''')

                        logger.info(f"Token info found: {bool(token_info)}")
                        logger.info(f"Hash found: {bool(hash_value)}")

                        if not token_info or not token_info.get('accessToken') or not hash_value:
                            logger.error(f"Missing track token or hash. Token: {bool(token_info)}, Hash: {bool(hash_value)}")
                            continue

                        # Store track token and hash
                        self.access_token = token_info['accessToken']
                        self.track_hash = hash_value

                        logger.info(f"Successfully got track token. Hash: {hash_value}")

                        # Save to Redis
                        await self.cache.save_token(
                            token=token_info['accessToken'],
                            proxy=self.proxy.__dict__,
                            token_type='track',
                            hash_value=hash_value
                        )

                        return True

                    except Exception as e:
                        logger.error(f"Error during page operations: {e}")
                        import traceback
                        logger.error(f"Full traceback: {traceback.format_exc()}")
                        continue
                    finally:
                        if browser:
                            await browser.close()

            except Exception as e:
                logger.error(f"Error in track token acquisition: {str(e)}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                self.proxy = None
                await asyncio.sleep(1)

        logger.error("All track token refresh attempts failed")
        return False

    async def get_tracks(
            self,
            track_ids: List[str],
            skip_cache: bool = False,
            detail: bool = False
    ) -> Dict[str, Optional[Dict]]:
        """Get track data for multiple track IDs"""
        await self._ensure_initialized()
        unique_ids = list(dict.fromkeys(track_ids))

        if skip_cache:
            fetch_tasks = [
                self._fetch_track(tid, detail)
                for tid in unique_ids
            ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            results = {}
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_track(data, detail)
                    if formatted:
                        results[unique_ids[i]] = formatted

            return results

        # Get all cached data
        cached_data = await self.db.get_tracks(unique_ids)

        # Determine which need to be fetched
        to_fetch = [
            tid for tid in unique_ids
            if not cached_data.get(tid) or not isinstance(cached_data.get(tid), dict)
        ]

        # Fetch missing data
        if to_fetch:
            fetch_tasks = [
                self._fetch_track(tid, detail)
                for tid in to_fetch
            ]
            fetched_data = await asyncio.gather(*fetch_tasks, return_exceptions=True)

            valid_data = []
            for i, data in enumerate(fetched_data):
                if isinstance(data, dict):
                    formatted = self._format_track(data, detail)
                    if formatted:
                        formatted['id'] = to_fetch[i]  # Add ID for caching
                        valid_data.append(formatted)

            # Save to cache and update results
            if valid_data:
                await self.db.save_tracks(valid_data)
                cached_data.update({d['id']: d for d in valid_data})

        return {
            tid: cached_data.get(tid)
            for tid in unique_ids
            if cached_data.get(tid)
        }

    async def _fetch_track(self, track_id: str, detail: bool = False) -> Optional[Dict]:
        """Fetch track data from Spotify Partner API"""
        try:
            await self.rate_limiter.acquire()

            #logger.info(f"Fetching track {track_id} (detail={detail})")

            if not await self._ensure_auth('track'):
                logger.error("Failed to get track auth token")
                return None

            #logger.info(f"Using track token: {self.access_token[:20]}...")
            #logger.info(f"Using track hash: {getattr(self, 'track_hash', 'None')}")

            if not self.session:
                self.session = aiohttp.ClientSession()

            if not self.proxy:
                logger.error("No proxy available")
                return None

            url = "https://api-partner.spotify.com/pathfinder/v1/query"

            variables = {
                'uri': f'spotify:track:{track_id}'
            }

            extensions = {
                'persistedQuery': {
                    'version': 1,
                    'sha256Hash': getattr(self, 'track_hash', '7fb74da4937948e158f48105eb4c39fc89e6a8d49fbde2e859f11844354e3908')
                }
            }

            params = {
                'operationName': 'getTrack',
                'variables': json.dumps(variables),
                'extensions': json.dumps(extensions)
            }

            #logger.info(f"Track request URL: {url}")
            #logger.info(f"Track request params: {params}")

            async with self.session.get(
                url,
                params=params,
                headers={
                    'Authorization': f'Bearer {self.access_token}',
                    'content-type': 'application/json',
                    'accept': 'application/json'
                },
                proxy=self.proxy.url,
                proxy_auth=self.proxy.auth,
                timeout=10
            ) as response:
                #logger.info(f"Track response status: {response.status}")

                response_text = await response.text()
                #logger.info(f"Track response text: {response_text[:200]}...")

                if response.status == 200:
                    data = json.loads(response_text)

                    if 'errors' in data:
                        #logger.error(f"GraphQL errors in track response: {data['errors']}")
                        return None

                    if data.get('data', {}).get('trackUnion') is None:
                        logger.error(f"No track data in response: {data}")
                        return None

                    return data

                elif response.status in {401, 407}:
                    logger.error(f"Authorization error {response.status}")
                    error_body = await response.text()
                    logger.error(f"Auth error response body: {error_body}")
                    self.access_token = None
                    self.proxy = None
                    return None

                logger.warning(f"Failed to fetch track {track_id}: {response.status}")
                error_body = await response.text()
                logger.error(f"Error response body: {error_body}")
                return None

        except Exception as e:
            logger.error(f"Error fetching track {track_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return None
        finally:
            self.rate_limiter.release()

    @staticmethod
    def _format_track(track_data: Dict, detail: bool = False) -> Optional[Dict]:
        """Format a track response from Spotify Partner API"""
        #logger.info(f"Formatting track data structure: {list(track_data.keys()) if isinstance(track_data, dict) else 'Not a dict'}")

        # For detail view, return the trackUnion data directly
        if detail:
            return track_data.get('data', {}).get('trackUnion', {})

        # Extract track data
        track = track_data.get('data', {}).get('trackUnion', {})
        if not track:
            logger.error("No trackUnion in response")
            return None

        # Get artist IDs from otherArtists and firstArtist
        artist_ids = []

        # Get artists from firstArtist
        if track.get('firstArtist', {}).get('items'):
            for artist in track['firstArtist']['items']:
                if artist.get('id'):
                    artist_ids.append(artist['id'])
                    #logger.info(f"Found artist ID from firstArtist: {artist['id']}")

        # Get artists from otherArtists
        if track.get('otherArtists', {}).get('items'):
            for artist in track['otherArtists']['items']:
                if artist.get('id'):
                    artist_ids.append(artist['id'])
                    #logger.info(f"Found artist ID from otherArtists: {artist['id']}")

        # Basic track data
        formatted_data = {
            'id': track.get('id'),
            'playcount': track.get('playcount'),
            'artistIds': artist_ids,
            'name': track.get('name'),
            'duration': track.get('duration', {}).get('totalMilliseconds'),
        }

        #logger.info(f"Formatted track data: {formatted_data}")

        if not formatted_data.get('id'):
            logger.error("No track ID in formatted data")
            return None

        return formatted_data
