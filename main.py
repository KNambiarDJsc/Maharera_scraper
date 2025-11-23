import asyncio
import logging
import os
import csv
import pandas as pd
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
from typing import Set, Optional

# --- Configuration ---
# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MahaReraScraper")

# --- File Configuration ---
OUTPUT_FILENAME = "maharera_complete_data.csv"
FAILED_PROJECTS_FILENAME = "maharera_failed_projects.csv"

# --- Scraping Configuration ---
START_ID = 401
END_ID = 800
BASE_URL = "https://maharerait.maharashtra.gov.in/public/project/view/"

# --- Worker counts (12 total = 8 normal + 4 retry) ---
TOTAL_WORKERS = 16
RETRY_WORKERS = 5
NORMAL_WORKERS = TOTAL_WORKERS - RETRY_WORKERS