import json
import requests
import getToken

hostAdresse = "http://141.76.47.15:9047" #Dirk
#hostAdresse = "http://localhost:9047" #mein Adresse local

gotToken = json.loads(getToken.getToken(hostAdresse))
token = str(gotToken.get('token'))

# schmeißt error
def get_allreflections(token, hostadresse):
    url = hostadresse+"/api/v3/reflection"
    headers = {
        'Authorization': "_dremio" + token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("GET", url, headers=headers)
    print(response.text)
    return response.text

get_allreflections(token, hostAdresse)
