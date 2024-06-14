import urllib3
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError, ApiError

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize the Elasticsearch client with authentication
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=("elastic", "RoyoqeNK-lIH4FaozhGD"),
    verify_certs=False
)

def test_connection():
    try:
        response = es.info()
        print("Elasticsearch info:", response)
    except ConnectionError as e:
        print("Connection error:", e)
    except ApiError as e:
        print("API error:", e)
    except Exception as e:
        print("Error connecting to Elasticsearch:", e)

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

def search_documents(query):
    try:
        response = es.search(index="documents", query={"match": {"text": query}})
        print(f"Search results for '{query}': {response['hits']['hits']}")
    except ConnectionError as e:
        print(f"Connection error searching for '{query}': {e}")
    except ApiError as e:
        print(f"API error searching for '{query}': {e}")
    except Exception as e:
        print(f"Error searching for '{query}': {e}")

# Test the connection
test_connection()

# Example text from documents
texts = {
    'doc_1': 'Extracted text from document 1',
    'doc_2': 'Extracted text from document 2',
    # Add more documents here
}

# Index the documents
index_documents(texts)

# Search for a term
search_documents("document 1")
