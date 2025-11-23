import asyncio
import logging
import os
import csv
import pandas as pd
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from modules.captcha_solver import CaptchaSolver
from modules.data_extracter import DataExtracter
from typing import Set, Optional