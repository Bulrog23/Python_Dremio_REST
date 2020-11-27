import requests
import json
import getToken
from datetime import datetime

id_job = "20458e4e-862f-7a05-ca55-4b1b3fba3e00"
gotToken = json.loads(getToken.getToken())
token = str(gotToken.get('token'))

def get_job(id_jo, token):
    url = "http://localhost:9047/api/v3/job/"
    url += id_jo
    headers = {
        'Authorization': "_dremio"+token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }
    response = requests.request("GET", url, headers=headers)
    return response.text


###reflection build Zeiten auslesen

#print(get_job(id_job, token))
#job_profil_json = json.loads(get_job(id_job, token))
#timeJobStart = job_profil_json.get('startedAt').replace("T", " ").replace("Z", "")
#timeJobEnd = job_profil_json.get('endedAt').replace("T", " ").replace("Z", "")
#startDate = datetime.strptime(timeJobStart, "%Y-%m-%d %H:%M:%S.%f")
#endDate = datetime.strptime(timeJobEnd, "%Y-%m-%d %H:%M:%S.%f")
#jobTimeResult = endDate-startDate
#print(str(jobTimeResult))'''
