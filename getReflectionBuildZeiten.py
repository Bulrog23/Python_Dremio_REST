import requests
import json
import getToken
from datetime import datetime

#Manuell eingeben
#wird nur im log/console ausgegeben (nicht in UI)
#log nicht gefunden -> Manuell abfangen/eingeben
reflectionBuild_id_job = "203c7bbe-49a0-aca4-5e19-3e955448fa00"

hostAdresse = "http://141.76.47.15:9047" #Dirk
#hostAdresse = "http://localhost:9047" #mein Adresse local

gotToken = json.loads(getToken.getToken(hostAdresse))
token = str(gotToken.get('token'))

def get_reflectionJob(id_jo, token, hostAdresse):
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

###reflection build Zeiten auslesen

print(get_reflectionJob(reflectionBuild_id_job, token, hostAdresse))
job_profil_json = json.loads(get_reflectionJob(reflectionBuild_id_job, token, hostAdresse))
timeJobStart = job_profil_json.get('startedAt').replace("T", " ").replace("Z", "")
timeJobEnd = job_profil_json.get('endedAt').replace("T", " ").replace("Z", "")
startDate = datetime.strptime(timeJobStart, "%Y-%m-%d %H:%M:%S.%f")
endDate = datetime.strptime(timeJobEnd, "%Y-%m-%d %H:%M:%S.%f")
jobTimeResult = endDate-startDate
print(str(jobTimeResult))