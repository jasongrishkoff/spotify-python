import asyncio
import psutil
import logging
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

async def cleanup_browsers():
    """Clean up any orphaned browser processes"""
    try:
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox']
                )
                if browser:
                    # Get all contexts
                    contexts = browser.contexts
                    for context in contexts:
                        # Get pages for the context
                        pages = await context.pages
                        if pages:
                            # Close each page
                            await asyncio.gather(*(page.close() for page in pages))
                        # Close the context
                        await context.close()
                    # Finally close the browser
                    await browser.close()
                    logger.info("Browser cleanup completed successfully")
            except Exception as e:
                logger.error(f"Error in graceful browser cleanup: {e}")

        # Force cleanup of remaining processes
        chrome_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if any(browser in proc.info['name'].lower() 
                      for browser in ['chrome', 'chromium']):
                    if any(arg for arg in (proc.info['cmdline'] or [])
                          if '--headless' in str(arg)):
                        chrome_processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        for proc in chrome_processes:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except psutil.TimeoutExpired:
                    proc.kill()
            except psutil.NoSuchProcess:
                continue
            except Exception as e:
                logger.error(f"Error killing process {proc.pid}: {e}")

        if chrome_processes:
            logger.info(f"Cleaned up {len(chrome_processes)} chrome processes")

    except Exception as e:
        logger.error(f"Error in browser cleanup: {e}")
        import traceback
        logger.error(f"Cleanup traceback: {traceback.format_exc()}")
