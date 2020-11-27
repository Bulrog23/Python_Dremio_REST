import requests

# import json
# id_job = "204dbb37-3c84-bc0f-d747-fb03b4e7b200"
token = "5g6cnsmflbl34vhjs9apqquae8"


# schmeißt error
def get_allreflections(token):
    url = "http://localhost:9047/api/v3/reflection"
    headers = {
        'Authorization': "_dremio" + token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("GET", url, headers=headers)
    print(response.text)
    return response.text


get_allreflections(token)
