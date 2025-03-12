# test_token_worker.py
import asyncio
import logging
import sys
import os

# Add app directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_worker")

async def test_token_workflow():
    from token_worker import TokenWorker
    
    worker = TokenWorker()
    
    print("\n=== Testing Complete Token Workflow ===")
    
    # 1. Test proxy acquisition 
    print("\nStep 1: Acquiring proxy...")
    proxy = await worker._get_proxy()
    if proxy:
        print(f"✅ Proxy acquired: {proxy}")
    else:
        print("❌ Proxy acquisition failed")
        return
        
    # 2. Test token capture
    print("\nStep 2: Attempting token capture...")
    access_token, client_token, operation_hashes = await worker._capture_tokens(proxy)
    
    if access_token:
        print(f"✅ Access token captured: {access_token[:15]}...")
        if client_token:
            print(f"✅ Client token captured: {client_token[:15]}...")
        else:
            print("⚠️ No client token captured")
            
        if operation_hashes:
            print(f"✅ Captured {len(operation_hashes)} operation hashes")
            for op, hash_val in operation_hashes.items():
                print(f"  - {op}: {hash_val[:10]}...")
        else:
            print("⚠️ No operation hashes captured")
            
        # 3. Test token saving
        print("\nStep 3: Saving token to Redis...")
        saved = await worker.redis_cache.save_token(
            access_token,
            proxy.__dict__,
            token_type="playlist",
            client_token=client_token
        )
        
        if saved:
            print("✅ Token saved successfully")
        else:
            print("❌ Failed to save token")
    else:
        print("❌ Token capture failed")
    
    # Clean up
    if hasattr(worker, 'session') and worker.session:
        await worker.session.close()

if __name__ == "__main__":
    asyncio.run(test_token_workflow())
