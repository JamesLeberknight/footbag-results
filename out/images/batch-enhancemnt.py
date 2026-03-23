import cv2
import numpy as np
import os
from PIL import Image, ImageEnhance

# Configuration
INPUT_DIR = '.' # Current directory
OUTPUT_DIR = './enhanced_results'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def enhance_batch():
    extensions = ('.jpg', '.jpeg', '.png')
    files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(extensions)]
    
    print(f"Found {len(files)} images to process...")

    for filename in files:
        path = os.path.join(INPUT_DIR, filename)
        
        # 1. READABILITY ENHANCEMENT (For human eyes)
        # ------------------------------------------
        img_pil = Image.open(path).convert('L') # Convert to grayscale
        
        # Boost Contrast
        enhancer = ImageEnhance.Contrast(img_pil)
        img_pil = enhancer.enhance(1.8)
        
        # Boost Sharpness
        enhancer = ImageEnhance.Sharpness(img_pil)
        img_pil = enhancer.enhance(2.5)
        
        readability_name = f"READABLE_{filename}"
        img_pil.save(os.path.join(OUTPUT_DIR, readability_name))

        # 2. CLEAN BINARIZATION (For OCR / Data Extraction)
        # ------------------------------------------
        img_cv = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
        if img_cv is not None:
            # Adaptive threshold handles uneven lighting/shadows in scans
            clean = cv2.adaptiveThreshold(
                img_cv, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                cv2.THRESH_BINARY, 15, 8
            )
            clean_name = f"CLEAN_{filename}"
            cv2.imwrite(os.path.join(OUTPUT_DIR, clean_name), clean)
            
        print(f" - Processed: {filename}")

if __name__ == "__main__":
    enhance_batch()
    print("\nEnhancement complete. Check the 'enhanced_results' folder.")
