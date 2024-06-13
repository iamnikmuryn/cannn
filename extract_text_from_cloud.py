import os
import io
from google.cloud import storage, vision
import PyPDF2
from elasticsearch import Elasticsearch

def list_all_files_in_bucket(bucket_name, subfolder_name):
    """List all files in the specified Google Cloud Storage bucket within a specified subfolder."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    prefix = subfolder_name + '/'  # Ensure the subfolder name ends with '/'
    blobs = bucket.list_blobs(prefix=prefix)  # Only list blobs that start with the folder name
    return [blob.name for blob in blobs if blob.name.endswith('.pdf')]  # Include only PDF files

def extract_text_from_pdf(bucket_name, source_blob_name):
    """Extract text from a specified PDF document using Google Vision API."""
    print(f"Starting text extraction from PDF: {source_blob_name}...")
    client = vision.ImageAnnotatorClient()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    try:
        pdf_content = blob.download_as_bytes()
        pdf_stream = io.BytesIO(pdf_content)
        reader = PyPDF2.PdfReader(pdf_stream)
        extracted_text = ""
        
        for i in range(len(reader.pages)):
            writer = PyPDF2.PdfWriter()
            writer.add_page(reader.pages[i])
            output_pdf_stream = io.BytesIO()
            writer.write(output_pdf_stream)
            output_pdf_stream.seek(0)
            
            image = vision.Image(content=output_pdf_stream.read())
            response = client.document_text_detection(image=image)
            texts = response.text_annotations
            if texts:
                extracted_text += texts[0].description + " "
    except Exception as e:
        print(f"Failed to process PDF: {source_blob_name}, Error: {e}")
        return None  # Return None if there's an error
    
    return extracted_text

def extract_and_index_documents(bucket_name, document_list, es_client):
    """Extract and index documents from a list."""
    print("Starting document extraction and indexing...")
    for document in document_list:
        text = extract_text_from_pdf(bucket_name, document)
        if text:
            print(f"Indexing document: {document}")
            es_client.index(index='documents', id=document, body={'text': text})
    print("All documents processed.")

if __name__ == "__main__":
    try:
        print("Starting the script...")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Nik Muryn\Desktop\cannn-chat-bot-0500ac6a1487.json"
        print(f"Google application credentials set: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")

        bucket_name = 'cannn_documents_bucket'
        subfolder_name = 'CANNN Documents'
        print("Listing documents in bucket subfolder...")
        document_list = list_all_files_in_bucket(bucket_name, subfolder_name)

        print("Connecting to Elasticsearch...")
        es = Elasticsearch(
            cloud_id='My_deployment:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjkxNDkzZmFjYWI0NWQ3OTcyMDkyYmEyN2VlMmFhNiQ5ZjFlZTE1MDYxMTk0Y2I3OGNlODBiYmE1ODE4N2EyYQ==',
            basic_auth=('elastic', 'jblfmfZpwzgIMyzhCdSnmjBb')
        )
        print("Connected to Elasticsearch.")

        extract_and_index_documents(bucket_name, document_list, es)
        print("Documents indexed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
