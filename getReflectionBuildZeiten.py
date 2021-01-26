import requests
import json
import getToken
from datetime import datetime

##Hier werden die Reflection-Build-Zeiten abgefangen
#Die Job-ID des Reflection-Builds wird nicht in der UI oder in der REST API ausgegeben
#Die Job ID muss im log/console manuell abgefangen werden (dort wird die ID ausgegeben)
#ID manuell eingeben unter "reflectionBuild_id_job"

#GET /job
#https://docs.dremio.com/rest-api/jobs/get-job.html

reflectionBuild_id_job = "203c7bbe-49a0-aca4-5e19-3e955448fa00" #hier Job-Id des Reflection-Build eingeben

hostAdresse = "http://localhost:9047" #hier Hostadresse angeben (bei einem Single-Node-Cluster ist dies die Default-Hostadresse)

#hier wird ein Token für die Anfrage erstellt, dass sie über die REST API geschickt werden kann
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
#hier wird die JSON-Datei des Reflection-Build-Jobs heruntergeladen
#Reflection-Zeiten werden ausgelesen -> in Sekunden ausgegeben

print(get_reflectionJob(reflectionBuild_id_job, token, hostAdresse))
job_profil_json = json.loads(get_reflectionJob(reflectionBuild_id_job, token, hostAdresse))
timeJobStart = job_profil_json.get('startedAt').replace("T", " ").replace("Z", "")
timeJobEnd = job_profil_json.get('endedAt').replace("T", " ").replace("Z", "")
startDate = datetime.strptime(timeJobStart, "%Y-%m-%d %H:%M:%S.%f")
endDate = datetime.strptime(timeJobEnd, "%Y-%m-%d %H:%M:%S.%f")
jobTimeResult = endDate-startDate
print(str(jobTimeResult))