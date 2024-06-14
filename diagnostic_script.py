import os

print("Script started")

# Check Google Cloud credentials
try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Nik Muryn\Desktop\cannn-chat-bot-0500ac6a1487.json"
    print(f"Google application credentials set: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")
except Exception as e:
    print(f"Error setting Google application credentials: {e}")

# Test Google Cloud Storage and Vision API
try:
    from google.cloud import storage, vision

    print("Testing Google Cloud Storage and Vision API...")
    client = vision.ImageAnnotatorClient()
    storage_client = storage.Client()
    bucket_name = 'CANNN chat bot'
    blob_name = 'CANNN Bylaw 1.pdf'  # Replace with an actual file in your bucket

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    pdf_content = blob.download_as_bytes()
    image = vision.Image(content=pdf_content)
    response = client.document_text_detection(image=image)
    texts = response.text_annotations
    print("Text detection complete.")
    print(texts[0].description if texts else "No text detected.")
except Exception as e:
    print(f"Error with Google Cloud: {e}")

# Test Elasticsearch connection
try:
    from elasticsearch import Elasticsearch

    print("Testing Elasticsearch connection...")
    es = Elasticsearch(
        cloud_id='My_deployment:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjkxNDkzZmFjYWI0NWQ3OTcyMDkyYmEyN2VlMmFhNiQ5ZjFlZTE1MDYxMTk0Y2I3OGNlODBiYmE1ODE4N2EyYQ==',  # Replace with your actual cloud ID
        basic_auth=('elastic', '5FrVgYcA0Ut5leOpZQ6dv1WC')  # Replace with your actual credentials
    )
    if es.ping():
        print("Connected to Elasticsearch.")
    else:
        print("Could not connect to Elasticsearch.")
except Exception as e:
    print(f"Error with Elasticsearch: {e}")

print("Script finished")
