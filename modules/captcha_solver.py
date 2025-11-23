import os
import time
from PIL import Image
import io
import pytesseract
import numpy as np
import cv2
import logging

logger = logging.getLogger(__name__)