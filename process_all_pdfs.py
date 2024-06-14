import pdfplumber
import pytesseract
from PIL import Image
import os

# Set the path to Tesseract-OCR executable
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def extract_text_from_pdf_with_ocr(file_path):
    text_output = ''
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_output += text
            else:
                im = page.to_image().original
                text_output += pytesseract.image_to_string(im)
    return text_output

def process_all_pdfs(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.pdf'):
            file_path = os.path.join(directory, filename)
            print(f"Processing {filename}...")
            extracted_text = extract_text_from_pdf_with_ocr(file_path)
            text_file_path = os.path.join(directory, f"{os.path.splitext(filename)[0]}.txt")
            with open(text_file_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            print(f"Finished processing {filename}. Extracted text saved to {text_file_path}")

# Specify the directory containing your PDFs
directory = r'C:\Users\Nik Muryn\Desktop\CANNN Documents'
process_all_pdfs(directory)
