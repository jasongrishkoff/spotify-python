import asyncio
import psutil
import logging
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

async def cleanup_browsers():
    """Clean up any orphaned browser processes"""
    try:
        # More defensive approach to browser cleanup
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox']
                )
                if browser:
                    try:
                        # Get all contexts
                        contexts = browser.contexts
                        for context in contexts:
                            try:
                                # Get pages for the context
                                pages = await context.pages()
                                if pages:
                                    # Close each page individually with error handling
                                    for page in pages:
                                        try:
                                            await page.close(timeout=5000)
                                        except Exception as page_e:
                                            logger.warning(f"Error closing page: {page_e}")
                                # Close the context with timeout
                                await context.close(timeout=5000)
                            except Exception as ctx_e:
                                logger.warning(f"Error handling context: {ctx_e}")
                        # Finally close the browser
                        await browser.close()
                        logger.info("Browser cleanup completed successfully")
                    except Exception as browser_e:
                        logger.error(f"Error in browser handling: {browser_e}")
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
