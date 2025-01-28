import aiosqlite
import time
import json
from typing import List, Dict, Optional, Tuple
from contextlib import asynccontextmanager
import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AsyncDatabase:
    def __init__(self, path: str = 'spotify_cache.db'):
        self.path = path
        self.pool_size = 10
        self._init_done = False  # Flag to track initialization
        self._cleanup_lock = asyncio.Lock()
        
    async def _init_db(self):
        """Initialize database tables if they don't exist"""
        if self._init_done:  # Skip if already initialized
            return
            
        async with aiosqlite.connect(self.path) as db:
            await db.executescript('''
                CREATE TABLE IF NOT EXISTS auth (
                    id INTEGER PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    proxy TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS playlists (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS artists (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS discovered_on (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tracks (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );

                
                CREATE INDEX IF NOT EXISTS idx_auth_created_at ON auth(created_at);
                CREATE INDEX IF NOT EXISTS idx_playlists_created_at ON playlists(created_at);
                CREATE INDEX IF NOT EXISTS idx_artists_created_at ON artists(created_at);
                CREATE INDEX IF NOT EXISTS idx_discovered_created_at ON discovered_on(created_at);
                CREATE INDEX IF NOT EXISTS idx_tracks_created_at ON tracks(created_at);
            ''')
            await db.commit()
            self._init_done = True

    @asynccontextmanager
    async def connection(self):
        """Get a database connection with optimized settings"""
        await self._init_db()  # Ensure tables exist
        async with aiosqlite.connect(self.path, timeout=60.0) as db:
            db.row_factory = aiosqlite.Row
            await db.execute('PRAGMA journal_mode = WAL')
            await db.execute('PRAGMA busy_timeout = 60000')
            yield db

    async def get_auth(self) -> Optional[Dict]:
        async with self.connection() as db:
            async with db.execute(
                'SELECT access_token, proxy FROM auth WHERE created_at > ? ORDER BY created_at DESC LIMIT 1',
                (int(time.time()) - 600,)
            ) as cursor:
                row = await cursor.fetchone()
                return {'access_token': row['access_token'], 'proxy': json.loads(row['proxy'])} if row else None

    async def save_auth(self, access_token: str, proxy: 'ProxyConfig'):
        async with self.connection() as db:
            await db.execute(
                'INSERT INTO auth (access_token, proxy, created_at) VALUES (?, ?, ?)',
                (access_token, json.dumps(proxy.__dict__), int(time.time()))
            )
            await db.commit()

    async def get_playlists(self, playlist_ids: List[str]) -> Dict[str, Dict]:
        async with self.connection() as db:
            placeholders = ','.join('?' * len(playlist_ids))
            current_time = int(time.time()) - 3600  # 1 hour cache
            
            query = f'''
                SELECT id, data 
                FROM playlists 
                WHERE id IN ({placeholders}) 
                AND created_at > ?
            '''
            
            async with db.execute(query, (*playlist_ids, current_time)) as cursor:
                results = await cursor.fetchall()
                return {
                    row['id']: json.loads(row['data']) 
                    for row in results
                }

    async def save_playlists(self, playlists: List[Dict]):
        async with self.connection() as db:
            current_time = int(time.time())
            await db.executemany(
                'INSERT OR REPLACE INTO playlists (id, data, created_at) VALUES (?, ?, ?)',
                [(p['id'], json.dumps(p), current_time) for p in playlists]
            )
            await db.commit()

    async def get_artists(self, artist_ids: List[str]) -> Dict[str, Dict]:
        async with self.connection() as db:
            placeholders = ','.join('?' * len(artist_ids))
            current_time = int(time.time()) - 3600  # 1 hour cache
            
            query = f'''
                SELECT id, data 
                FROM artists 
                WHERE id IN ({placeholders}) 
                AND created_at > ?
            '''
            
            async with db.execute(query, (*artist_ids, current_time)) as cursor:
                results = await cursor.fetchall()
                return {
                    row['id']: json.loads(row['data']) 
                    for row in results
                }

    async def save_artists(self, artists: List[Tuple[str, Dict]]):
        """
        Save artists to cache. Each artist is a tuple of (cache_key, data)
        """
        async with self.connection() as db:
            current_time = int(time.time())
            await db.executemany(
                'INSERT OR REPLACE INTO artists (id, data, created_at) VALUES (?, ?, ?)',
                [(key, json.dumps(data), current_time) for key, data in artists]
            )
            await db.commit()

    async def get_discovered_on(self, artist_ids: List[str]) -> Dict[str, Dict]:
        async with self.connection() as db:
            placeholders = ','.join('?' * len(artist_ids))
            current_time = int(time.time()) - 43200  # 12 hour cache
            
            query = f'''
                SELECT id, data 
                FROM discovered_on 
                WHERE id IN ({placeholders}) 
                AND created_at > ?
            '''
            
            async with db.execute(query, (*artist_ids, current_time)) as cursor:
                results = await cursor.fetchall()
                return {
                    row['id']: json.loads(row['data']) 
                    for row in results
                }

    async def save_discovered_on(self, discovered_data: List[Dict]):
        async with self.connection() as db:
            current_time = int(time.time())
            await db.executemany(
                'INSERT OR REPLACE INTO discovered_on (id, data, created_at) VALUES (?, ?, ?)',
                [(d['id'], json.dumps(d), current_time) for d in discovered_data]
            )
            await db.commit()

    async def cleanup_old_records(self):
        """
        Efficiently clean up old records using optimized queries and batched deletions.
        Uses a lock to prevent concurrent cleanup operations.
        """
        # Prevent multiple cleanup operations from running simultaneously
        if not await self._cleanup_lock.acquire():
            logger.warning("Cleanup already in progress, skipping...")
            return
            
        try:
            async with aiosqlite.connect(self.path, timeout=60.0) as db:  # Increased timeout
                await db.execute('PRAGMA journal_mode = WAL')  # Use Write-Ahead Logging
                await db.execute('PRAGMA busy_timeout = 60000')  # 60 second timeout
                current_time = int(time.time())

                try:
                    # Define cleanup thresholds
                    thresholds = {
                        'auth': current_time - 3600,        # 1 hour for auth tokens
                        'playlists': current_time - 3600,   # 1 hour for playlists
                        'artists': current_time - 3600,     # 1 hour for artists
                        'discovered_on': current_time - 3600, # 1 hour for discovered_on
                        'tracks': current_time - 3600       # 1 hour for tracks
                    }

                    # Get counts before cleanup
                    counts_before = {}
                    for table in thresholds.keys():
                        async with db.execute(f'SELECT COUNT(*) FROM {table}') as cursor:
                            counts_before[table] = (await cursor.fetchone())[0]

                    # Process each table
                    for table, threshold in thresholds.items():
                        try:
                            # Use a single transaction per table
                            await db.execute('BEGIN TRANSACTION')
                            
                            # Delete in smaller batches (500 instead of 1000)
                            while True:
                                async with db.execute(
                                    f'DELETE FROM {table} WHERE created_at < ? LIMIT 500',
                                    (threshold,)
                                ) as cursor:
                                    if cursor.rowcount == 0:
                                        break
                                    logger.info(f'Deleted {cursor.rowcount} records from {table}')
                                await db.commit()  # Commit each batch
                                
                                # Small delay between batches to reduce contention
                                await asyncio.sleep(0.1)
                                
                                # Start new transaction for next batch
                                await db.execute('BEGIN TRANSACTION')
                                
                            await db.commit()
                            
                        except Exception as e:
                            logger.error(f'Error cleaning up {table}: {e}')
                            await db.execute('ROLLBACK')
                            continue

                    # Get final counts
                    counts_after = {}
                    for table in thresholds.keys():
                        try:
                            async with db.execute(f'SELECT COUNT(*) FROM {table}') as cursor:
                                counts_after[table] = (await cursor.fetchone())[0]
                        except Exception as e:
                            logger.error(f'Error getting final count for {table}: {e}')
                            continue

                    # Log results
                    for table in thresholds.keys():
                        if table in counts_before and table in counts_after:
                            removed = counts_before[table] - counts_after[table]
                            if removed > 0:
                                logger.info(f'Cleaned up {removed} records from {table}. '
                                          f'Remaining: {counts_after[table]}')

                    # Optimize database after cleanup
                    try:
                        await db.execute('PRAGMA optimize')
                        await db.execute('PRAGMA wal_checkpoint(FULL)')
                    except Exception as e:
                        logger.error(f'Error during optimization: {e}')

                except Exception as e:
                    logger.error(f'Error during cleanup: {e}')
                    raise

        except Exception as e:
            logger.error(f'Database cleanup failed: {e}')
            import traceback
            logger.error(f'Cleanup traceback: {traceback.format_exc()}')
        finally:
            self._cleanup_lock.release()  # Always release the lock

    async def get_tracks(self, track_ids: List[str]) -> Dict[str, Dict]:
        async with self.connection() as db:
            placeholders = ','.join('?' * len(track_ids))
            current_time = int(time.time()) - 3600  # 1 hour cache

            query = f'''
                SELECT id, data
                FROM tracks
                WHERE id IN ({placeholders})
                AND created_at > ?
            '''

            async with db.execute(query, (*track_ids, current_time)) as cursor:
                results = await cursor.fetchall()
                return {
                    row['id']: json.loads(row['data'])
                    for row in results
                }

    async def save_tracks(self, tracks: List[Dict]):
        async with self.connection() as db:
            current_time = int(time.time())
            await db.executemany(
                'INSERT OR REPLACE INTO tracks (id, data, created_at) VALUES (?, ?, ?)',
                [(t['id'], json.dumps(t), current_time) for t in tracks]
            )
            await db.commit()

