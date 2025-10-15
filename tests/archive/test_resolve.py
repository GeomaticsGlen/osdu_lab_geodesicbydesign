import requests, json

url = "http://127.0.0.1:5001/api/schema-service/v1/schemas/osdu:wks:master-data--Wellbore:1.1.0"
resp = requests.get(url, params={"resolve": "true"})

print("Status:", resp.status_code)

data = resp.json()
pretty = json.dumps(data, indent=2)

# Print to console
print(pretty)

# Write to file
with open("resolved_output.json", "w", encoding="utf-8") as f:
    f.write(pretty)

print("\n[✔] Output also written to resolved_output.json")
