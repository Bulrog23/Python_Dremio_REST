import requests

# import json
# id_job = "204dbb37-3c84-bc0f-d747-fb03b4e7b200"

def get_reflection(id_jo, token):
    url = "http://localhost:9047/api/v3/reflection/"
    url += id_jo
    headers = {
        'Authorization': "_dremio"+token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("GET", url, headers=headers)
    # print(response.text)
    return response.text