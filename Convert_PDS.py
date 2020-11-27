import os
import requests
import getToken
import json

#tablePath = "/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark/Arade/tables/Arade_1.table.sql"
benchmarkPath = "/media/jonas/TOSHIBA EXT/Jonas/public_bi_benchmark-master/benchmark"

# mit allen Daten
# datenPath = "/media/jonas/TOSHIBA EXT/Jonas/daten/PublicBIbenchmark"

#testData
datenPath = "/media/jonas/TOSHIBA EXT/Jonas/testDaten"
nas = "allData" #NAS Name in Dremio

gotToken = json.loads(getToken.getToken())
token = str(gotToken.get('token'))
#token = "5kudnld31e18pbhtongp6dpkff"


# convertiert alle zu PDS
def convertpds(i2, x2, nas, token):
    # url = "http://localhost:9047/api/v3/catalog/dremio%3A%2Ftest%2FTelco%2FTelco_1.csv.bz2"
    url = "http://localhost:9047/api/v3/catalog/"
    id = "dremio%3A%2F"
    id += nas + "%2F"  # Name-NAS folder
    id += i2 + "%2F"
    id += x2
    url += id
    # print(id)
    # print(url)

    payload = '''{
    "entityType": "dataset",
    "id": '''
    payload += "\"" + id + "\","
    payload += "\n    " + "\"path\": ["
    payload += "\n        \"" + nas + "\","
    payload += "\n        \"" + i2 + "\","
    payload += "\n        \"" + x2 + "\""
    payload += "\n        " + '''],
    
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
        'Authorization': "_dremio"+token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response.text)
    # print(payload)


# convertPDS("Telco", "Telco_1.csv.bz2", "test")

arr = os.listdir(datenPath)
for i in arr:
    tablePath2 = datenPath + "/" + i
    arr2 = os.listdir(
        tablePath2)  # Array von allen table files pro ordner['CityMaxCapita_1.table.sql']['CMSprovider_1.table.sql', 'CMSprovider_2.table.sql']
    for x in arr2:
        convertpds(i, x, nas, token)
