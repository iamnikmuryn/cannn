from elasticsearch import Elasticsearch

# Replace 'your_password_here' with the actual password
es = Elasticsearch(
    ['http://localhost:9200'],
    basic_auth=('elastic', 's_1pP*f*V-73vkG920Vy')
)

# Test the connection
try:
    # Get cluster health
    health = es.cluster.health()
    print("Elasticsearch Cluster Health:", health)
except Exception as e:
    print("Failed to connect to Elasticsearch:", e)
