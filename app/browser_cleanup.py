import asyncio
import psutil
import logging
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

async def cleanup_browsers():
    """
    Clean up any orphaned browser processes with proper error handling.
    This version handles both Playwright browser instances and orphaned processes.
    """
    try:
        # First try to close any active Playwright browsers gracefully
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp('http://localhost:9222')
                if browser:
                    for context in browser.contexts:
                        for page in context.pages:
                            await page.close()
                        await context.close()
                    await browser.close()
        except Exception as browser_e:
            logger.debug(f"No active browser sessions to close: {browser_e}")

        # Then clean up any remaining browser processes
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                proc_info = proc.info
                if any(browser_process in str(proc_info.get('name', '')).lower() 
                       for browser_process in ['chrome', 'chromium', 'playwright']):
                    logger.info(f"Killing browser process: {proc.pid}")
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                logger.debug(f"Could not access process {proc}: {e}")
            except Exception as e:
                logger.error(f"Error handling process {proc}: {e}")

    except Exception as e:
        logger.error(f"Error during browser cleanup: {e}")
