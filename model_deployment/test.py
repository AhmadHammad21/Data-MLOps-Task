import requests

product_payload = {
    "product_id": 99999,
}

url = "http://localhost:5000/recommend"
response = requests.post(url, json=product_payload)
print(response)
print(response.json())
