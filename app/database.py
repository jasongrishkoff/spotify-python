import aiosqlite
import time
import json
from typing import List, Dict, Optional
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
        """Get a database connection, ensuring tables exist"""
        await self._init_db()  # Ensure tables exist
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
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

    async def save_artists(self, artists: List[Dict]):
        async with self.connection() as db:
            current_time = int(time.time())
            await db.executemany(
                'INSERT OR REPLACE INTO artists (id, data, created_at) VALUES (?, ?, ?)',
                [(a['id'], json.dumps(a), current_time) for a in artists]
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
        Runs every hour instead of daily.
        """
        async with self.connection() as db:
            current_time = int(time.time())

            try:
                # Start a transaction for all cleanup operations
                await db.execute('BEGIN TRANSACTION')

                # Define cleanup thresholds
                thresholds = {
                    'auth': current_time - 3600,        # 1 hour for auth tokens
                    'playlists': current_time - 3600,   # 1 hour for playlists
                    'artists': current_time - 3600,    # 1 hour for artists
                    'discovered_on': current_time - 3600  # 1 hour for discovered_on
                }

                # Get counts before cleanup for logging
                counts_before = {}
                for table in thresholds.keys():
                    async with db.execute(f'SELECT COUNT(*) FROM {table}') as cursor:
                        counts_before[table] = (await cursor.fetchone())[0]

                # Perform deletions with batching
                for table, threshold in thresholds.items():
                    # Delete in batches of 1000 to avoid locking the database for too long
                    while True:
                        async with db.execute(
                            f'DELETE FROM {table} WHERE created_at < ? LIMIT 1000',
                            (threshold,)
                        ) as cursor:
                            if cursor.rowcount == 0:
                                break
                            logger.info(f'Deleted {cursor.rowcount} records from {table}')
                            await db.execute('COMMIT')  # Commit each batch
                            await db.execute('BEGIN TRANSACTION')  # Start new transaction

                # Get counts after cleanup
                counts_after = {}
                for table in thresholds.keys():
                    async with db.execute(f'SELECT COUNT(*) FROM {table}') as cursor:
                        counts_after[table] = (await cursor.fetchone())[0]

                # Log cleanup results
                for table in thresholds.keys():
                    removed = counts_before[table] - counts_after[table]
                    if removed > 0:
                        logger.info(f'Cleaned up {removed} records from {table}. '
                                  f'Remaining: {counts_after[table]}')

                # Optimize database after cleanup
                await db.execute('PRAGMA optimize')

                # Final commit
                await db.execute('COMMIT')

            except Exception as e:
                logger.error(f'Error during cleanup: {e}')
                await db.execute('ROLLBACK')
                raise

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

