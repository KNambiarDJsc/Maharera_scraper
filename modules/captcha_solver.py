import os
import time
from PIL import Image
import io
import pytesseract
import numpy as np
import cv2
import logging

logger = logging.getLogger(__name__)

class CaptchaSolver:
    def __init__(self, captcha_dir="./captchas"):
        self.captcha_dir = captcha_dir
        os.makedirs(self.captcha_dir, exist_ok=True)