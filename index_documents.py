import urllib3
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError, ApiError

# Suppress SSL warnings (optional, for development purposes)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize the Elasticsearch client with authentication
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=("elastic", "RoyoqeNK-lIH4FaozhGD"),
    verify_certs=False
)

def index_documents(texts):
    for doc_id, text in texts.items():
        try:
            response = es.index(index='documents', id=doc_id, document={'text': text})
            print(f"Document {doc_id} indexed successfully: {response}")
        except ConnectionError as e:
            print(f"Connection error indexing document {doc_id}: {e}")
        except ApiError as e:
            print(f"API error indexing document {doc_id}: {e}")
        except Exception as e:
            print(f"Error indexing document {doc_id}: {e}")

# Example text from documents
texts = {
    'doc_1': 'Extracted text from document 1',
    'doc_2': 'Extracted text from document 2',
    # Add more documents here
}
index_documents(texts)
