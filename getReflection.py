import requests
# import json

# id_job = "204dbb37-3c84-bc0f-d747-fb03b4e7b200"
#hostAdresse = "http://141.76.47.15:9047" #Dirk
#hostAdresse = "http://localhost:9047" #mein Adresse local

#gotToken = json.loads(getToken.getToken())
#token = str(gotToken.get('token'))

def get_reflection(id_jo, token, hostAdresse):
    url = hostAdresse+"/api/v3/reflection/"
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