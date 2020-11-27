import requests

url = "http://localhost:9047/api/v3/catalog/dremio%3A%2Ftest%2FTelco%2FTelco_1.csv.bz2"

payload = '''{
    "entityType": "dataset",
    "id": "dremio%3A%2Ftest%2FTelco%2FTelco_1.csv.bz2",
    "path": [
        "test",
        "Telco",
        "Telco_1.csv.bz2"
        ],

    "type": "PHYSICAL_DATASET",
    "format": {
        "type": "Text",
        "fieldDelimiter": "|",
        "lineDelimiter": "\\n",
        "quote": "\\f",
        "escape": "\\"",
        "skipFirstLine": false,
        "extractHeader": false,
        "trimHeader": false,
        "autoGenerateColumnNames": true
    }
}'''
headers = {
    'Authorization': "_dremioc9cgjphqhtj726doagrmtueuf6",
    'Content-Type': "application/json",
    'cache-control': "no-cache",
    'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)