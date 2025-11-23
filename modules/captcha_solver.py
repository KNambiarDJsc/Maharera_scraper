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

    async def preprocess_image(self, image_bytes):
        """Convert captcha image to binary thresholded form for OCR."""
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img_np = np.array(img)
        gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return Image.fromarray(thresh)