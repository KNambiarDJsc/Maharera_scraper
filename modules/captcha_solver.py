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
    
    async def solve_and_fill(
        self,
        page,
        captcha_selector,
        input_selector,
        submit_selector,
        reg_no
    ):
        """Solve captcha with only ONE attempt. Return success or failure."""
        logger.info(f"Attempting to solve captcha for {reg_no} (1 attempt only).")
        try:
            # Wait for captcha element and take screenshot
            captcha_el = await page.wait_for_selector(captcha_selector, timeout=10000)
            captcha_bytes = await captcha_el.screenshot(type="png", scale="device")

            # Extract OCR text
            captcha_text = await self.extract_text(captcha_bytes)
            logger.info(f"[DEBUG] OCR extracted: {captcha_text}")

            if captcha_text:
                # Fill input and submit
                await page.fill(input_selector, captcha_text)
                await page.click(submit_selector)

                # Success check: Wait for captcha to disappear
                try:
                    await page.wait_for_selector(captcha_selector, state="detached", timeout=5000)
                    logger.info(f"✅ Captcha solved successfully for {reg_no}")
                    return True # Success
                except Exception:
                    logger.warning(f"Captcha incorrect for {reg_no}. Marking as failed.")
                    return False # Failure (incorrect captcha)
            else:
                logger.warning(f"OCR failed to read text for {reg_no}. Marking as failed.")
                return False # Failure (OCR couldn't read)

        except Exception as e:
            logger.error(f"Error during captcha solve attempt for {reg_no}: {e}")
            return False # Failure (any other error)
        
    
