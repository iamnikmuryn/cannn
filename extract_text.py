import os
from google.cloud import storage, vision
import io
from elasticsearch import Elasticsearch

def extract_text_from_pdf(bucket_name, source_blob_name):
    print("Starting text extraction from PDF...")
    client = vision.ImageAnnotatorClient()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    print(f"Downloading PDF content from {source_blob_name}...")
    pdf_content = blob.download_as_bytes()
    image = vision.Image(content=pdf_content)

    print("Calling Vision API for text detection...")
    response = client.document_text_detection(image=image)
    texts = response.text_annotations
    print("Text detection complete.")
    return texts[0].description if texts else ""

def extract_text_from_pages(bucket_name, source_blob_name, start_page, end_page):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    pdf_bytes = blob.download_as_bytes()

    # Load the PDF from bytes
    pdf_file = io.BytesIO(pdf_qtyes)
    reader = PyPDF2.PdfReader(pdf_file)

    # Extract specified pages
    writer = PyPDF2.PdfWriter()
    for i in range(start_page, min(end_page + 1, len(reader.pages))):
        writer.add_page(reader.pages[i])

    # Save the extracted pages to a new PDF in memory
    output_pdf = io.BytesIO()
    writer.write(output_pdf)
    output_pdf.seek(0)

    # Use Vision API to process the extracted pages
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=output_pdf.read())
    response = client.document_text_detection(image=image)
    texts = response.text_annotations
    return texts[0].description if texts else ""


if __name__ == "__main__":
    try:
        print("Starting the script...")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Nik Muryn\Desktop\cannn-chat-bot-0500ac6a1487.json"
        print(f"Google application credentials set: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")

        bucket_name = 'cannn_documents_bucket'  # Correct bucket name
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

        print("Connecting to Elasticsearch...")
        es = Elasticsearch(
            cloud_id='My_deployment:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjkxNDkzZmFjYWI0NWQ3OTcyMDkyYmEyN2VlMmFhNiQ5ZjFlZTE1MDYxMTk0Y2I3OGNlODBiYmE1ODE4N2EyYQ==',  # Replace with your actual Cloud ID
            basic_auth=('elastic', 'jblfmfZpwzgIMyzhCdSnmjBb')  # Replace with your actual credentials
        )
        print("Connected to Elasticsearch.")

        extract_and_index_documents(bucket_name, document_list, es)
        print("Documents indexed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")