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
    

    async def extract_text(self, image_bytes):
        """Run OCR on captcha image with multiple configs."""
        processed_img = await self.preprocess_image(image_bytes)
        
        configs = [
            '--psm 8 --oem 3 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
            '--psm 7 --oem 3 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        ]

        results = []
        for img in [Image.open(io.BytesIO(image_bytes)), processed_img]:
            for config in configs:
                text = pytesseract.image_to_string(img, config=config).strip()
                if text and len(text) == 6 and text.isalnum():
                    results.append(text.upper())
        if results:
            return max(set(results), key=results.count)
        return None
    
