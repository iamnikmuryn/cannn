import os
from google.cloud import storage, vision
import io
from elasticsearch import Elasticsearch
import PyPDF2

def extract_text_from_page(bucket_name, source_blob_name, page_number):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    pdf_bytes = blob.download_as_bytes()

    # Load the PDF from bytes
    pdf_file = io.BytesIO(pdf_bytes)
    reader = PyPDF2.PdfReader(pdf_file)

    # Extract the specific page
    if page_number < len(reader.pages):
        writer = PyPDF2.PdfWriter()
        writer.add_page(reader.pages[page_number])

        # Save the selected page to a new PDF in memory
        output_pdf = io.BytesIO()
        writer.write(output_pdf)
        output_pdf.seek(0)

        # Continue processing as usual
        client = vision.ImageAnnotatorClient()
        image = vision.Image(content=output_pdf.read())
        response = client.document_text_detection(image=image)
        texts = response.text_annotations
        return texts[0].description if texts else ""
    return ""  # Return empty string if page number is out of range

def extract_and_index_documents(bucket_name, document_list, es_client):
    print("Starting document extraction and indexing...")
    storage_client = storage.Client()  # Ensure the storage client is created here
    for filename in document_list:
        blob = storage_client.bucket(bucket_name).blob(filename)
        reader = PyPDF2.PdfReader(io.BytesIO(blob.download_as_bytes()))
        num_pages = len(reader.pages)
        for page_number in range(num_pages):
            print(f"Processing {filename}, Page {page_number + 1}")
            text = extract_text_from_page(bucket_name, filename, page_number)
            if text:  # Only index if text was extracted
                es_client.index(index='documents', id=f"{filename}_page_{page_number + 1}", body={'text': text})
    print("All documents processed.")

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Nik Muryn\Desktop\cannn-chat-bot-0500ac6a1483.json"
    bucket_name = 'cannn_documents_bucket'
    document_list = [
        'CANNN Bylaw 1.pdf',
        'CANNN Bylaw 5.pdf',
        'CANNN Code of Ethics.pdf',
        'CANNN NWT Nursing Act.pdf',
        'CANNN Nunavut Nursing Act.pdf',
        'CANNN bylaw 13.pdf',
        'CANNN bylaw 18.pdf',
        'CANNN bylaw 2.pdf',
        'CANNN bylaw 3.pdf',
        'CANNN bylaw 4.pdf',
        'CANNN standards of practice.pdf',
        'CAnnn bylaw 19.pdf',
        'Cannn Policy n2.pdf',
        'Cannn bylaw 17.pdf',
        'cannn bylaw 10.pdf',
        'cannn bylaw 11.pdf',
        'cannn bylaw 12.pdf',
        'cannn bylaw 14.pdf',
        'cannn bylaw 15.pdf',
        'cannn bylaw 16.pdf',
        'cannn bylaw 20.pdf',
        'cannn bylaw 21.pdf',
        'cannn bylaw 22.pdf',
        'cannn bylaw 23.pdf',
        'cannn bylaw 24.pdf',
        'cannn bylaw 25.pdf',
        'cannn bylaw 6.pdf',
        'cannn bylaw 7.pdf',
        'cannn bylaw 8.pdf',
        'cannn bylaw 9.pdf',
        'cannn policy af1.pdf',
        'cannn policy af2.pdf',
        'cannn policy af3.pdf',
        'cannn policy af4.pdf',
        'cannn policy af5.pdf',
        'cannn policy af6.pdf',
        'cannn policy af7.pdf',
        'cannn policy ag1.pdf',
        'cannn policy ag3.pdf',
        'cannn policy ag4.pdf',
        'cannn policy ag6.pdf',
        'cannn policy ag7.pdf',
        'cannn policy ag8.pdf',
        'cannn policy ag9.pdf',
        'cannn policy b1.pdf',
        'cannn policy b10.pdf',
        'cannn policy b12.pdf',
        'cannn policy b13.pdf',
        'cannn policy b14.pdf',
        'cannn policy b2.pdf',
        'cannn policy b3.pdf',
        'cannn policy b4.pdf',
        'cannn policy b5.pdf',
        'cannn policy b6.pdf',
        'cannn policy b8.pdf',
        'cannn policy n1.pdf',
        'cannn policy pc1.pdf',
        'cannn policy pc2.pdf',
        'cannn policy pc3.pdf',
        'cannn policy pc4.pdf',
        'cannn policy pc5.pdf',
        'cannn policy r1.1.pdf',
        'cannn policy r1.2.pdf',
        'cannn policy r1.pdf',
        'cannn policy r10.pdf',
        'cannn policy r11.pdf',
        'cannn policy r12.pdf',
        'cannn policy r13.pdf',
        'cannn policy r14.pdf',
        'cannn policy r17.pdf',
        'cannn policy r18.pdf',
        'cannn policy r19.pdf',
        'cannn policy r2.pdf',
        'cannn policy r3.pdf',
        'cannn policy r4.pdf',
        'cannn policy r5.pdf',
        'cannn policy r6.pdf',
        'cannn policy r7.pdf',
        'cannn policy r9.pdf'
    ]

    es = Elasticsearch(
        cloud_id='My_deployment:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjkxNDkzZmFjYWI0NWQ3OTcyMDkyYmEyN2VlMmFhNiQ5ZjFlZTE1MDYxMTk0Y2I3OGNlODBiYmE1ODE4N2EyYQ==',
        api_key='SnU0ZUZaQUJ1WXNjb0QyUkRJVWc6M2E0SVlLaW5UbmVnczF3ajZsU2NZQQ=='
    )
    extract_and_index_documents(bucket_name, document_list, es)
