import requests
#import json
#import getToken

#hostAdresse = "http://localhost:9047" #mein Adresse local

#id_job = "203c7bbe-49a0-aca4-5e19-3e955448fa00"
#gotToken = json.loads(getToken.getToken())
#token = str(gotToken.get('token'))

def get_job(id_jo, token, hostAdresse):
    url = hostAdresse+"/api/v3/job/"
    url += id_jo
    headers = {
        'Authorization': "_dremio"+token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("GET", url, headers=headers)
    return response.text

#print(get_job(id_job, token, hostAdresse))

