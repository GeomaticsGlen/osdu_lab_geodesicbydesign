from opensearchpy import OpenSearch

# Connect to your local OpenSearch
client = OpenSearch([{"host": "localhost", "port": 9200}])

# Fetch cluster info
info = client.info()
print("✅ Connected to OpenSearch")
print(info)
