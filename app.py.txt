from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch(
      cloud_id='My_deployment:bm9ydGhhbWVyaWNhLW5vcnRoZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyRjMjkxNDkzZmFjYWI0NWQ3OTcyMDkyYmEyN2VlMmFhNiQ5ZjFlZTE1MDYxMTk0Y2I3OGNlODBiYmE1ODE4N2EyYQ==',  # Replace with your actual Cloud ID
            basic_auth=('elastic', 'jblfmfZpwzgIMyzhCdSnmjBb')  # Replace with your actual credentials
        )

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    query = req['queryResult']['queryText']
    res = es.search(index='documents', body={'query': {'match': {'text': query}}})
    hits = res['hits']['hits']
    response_text = hits[0]['_source']['text'] if hits else "No relevant documents found."
    return jsonify({'fulfillmentText': response_text})

if __name__ == '__main__':
    app.run(debug=True)
