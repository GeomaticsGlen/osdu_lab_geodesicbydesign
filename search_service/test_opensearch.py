from opensearchpy import OpenSearch, ConnectionError

try:
    client = OpenSearch([{"host": "localhost", "port": 9200}])
    info = client.info()
    print("✅ Connected to OpenSearch")
    print(info)
except ConnectionError as e:
    print("❌ Failed to connect to OpenSearch:", e)
