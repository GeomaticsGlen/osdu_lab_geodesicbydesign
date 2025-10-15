# test_index.py
from opensearchpy import OpenSearch

client = OpenSearch([{"host": "localhost", "port": 9200}])

# Create an index with a simple mapping
index_name = "osdu-records"
if not client.indices.exists(index=index_name):
    client.indices.create(
        index=index_name,
        body={
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "kind": {"type": "keyword"},
                    "data": {"type": "object", "enabled": True}
                }
            }
        }
    )
    print(f"✅ Created index: {index_name}")
else:
    print(f"ℹ️ Index {index_name} already exists")

# Index a sample record
doc = {
    "id": "rec-1",
    "kind": "osdu:wks:master-data--Well:1.4.0",
    "data": {"name": "Test Well"}
}
client.index(index=index_name, id=doc["id"], body=doc, refresh=True)
print("✅ Indexed test record")

# Search for it
resp = client.search(
    index=index_name,
    body={"query": {"match": {"data.name": "Test"}}}
)
print("🔎 Search results:")
print(resp["hits"]["hits"])
