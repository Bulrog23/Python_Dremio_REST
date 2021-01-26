import os
import requests
import json
import getJob
import time
import jobAuslesen2
import getToken

###Hier werden SQL-Anfragen hintereinander einzeln zu Dremio gesendet und dann die Ausführungszeiten/daten ausgewertet
#alle SQL-Anfragen die ausgeführt werden sollen -> müssen sich in einem Ordner befinden
#Es werden alle Anfragen ausgeführt -> nach jeder abgeschlossenen SQL-Anfrage: wird die Job-ID für die spätere Auswertung gespeichert (in dict)
#Obwohl Anfragenausführung nicht abgeschlossen -> kann man ein vorläufiges Job-Datei abfrage -> durch diese wird zeitlich regelmäßig gecheckt ob Anfrage abgeschlossen wurde

#GET /job
#https://docs.dremio.com/rest-api/jobs/get-job.html

#Gedanke: mit den Tool DBeaver kann man auch viele Anfragen an Dremio hintereinander senden
#Aber für die Auswertungen benötigt man die Job-Ids -> erhält man nur, wenn man Anfragen über REST ausführt
#(kann die IDs auch über das log/console abfangen (log nicht gefunden + schwierig Code aus laufender Console abzufangen)

mappingIDqueries = dict() #Job-Ids werden in mapping gespeichert
datenPath = "/media/jonas/TOSHIBA EXT/Jonas/testQuery" #Ordner der alle SQL-Anfragen enthält die durchgeführt werden sollen
path = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/" #ZwischenSpeicher für MappingQueryId.json (JOb-JSON)
speicherPath = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/" #Speicherort der Ergebnisdateien
abfrageZeitServer = 30 #Sekunden Wartezeit zwischen den regelmäßigen Job-Status-Anfragen

hostAdresse = "http://localhost:9047" #Single-Node-Adresse

gotToken = json.loads(getToken.getToken(hostAdresse))
token = gotToken.get('token')

def sqlquerie(payload, tableNameNumber, token, abfrageZeit, hostAdresse):
    url = hostAdresse+"/api/v3/sql"
    headers = {
        'Authorization': "_dremio" + token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e568431f-3935-46bd-b7e6-9e8adf44e606"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response.text)

    response_native = json.loads(response.text)
    #ID Beispiel: 204fa4f5-23d8-1995-fc1d-3b3499e57500
    mappingIDqueries[tableNameNumber] = response_native.get(
        'id')  # added Eintrag zu dic wenn tableNameNr(key) noch nicht vorhanden
    print(mappingIDqueries)
    response_job = json.loads(getJob.get_job(response_native.get('id'), token, hostAdresse))

    status_job = response_job.get('jobState')

    #Warteschleife -> solange Anfrage nicht beendet
    while status_job != "COMPLETED" and status_job != "CANCELED" and status_job != "FAILED":
        time.sleep(abfrageZeit)
        response_job = json.loads(getJob.get_job(response_native.get('id'), token, hostAdresse))
        status_job = response_job.get('jobState')
        print(status_job)
    print("Queue completed")
    #Beispiel Job-JSON Anfragenausführung nicht beendet:
    # {"jobState":"METADATA_RETRIEVAL","errorMessage":"","startedAt":"2020-11-16T09:22:16.894Z","queryType":"REST","cancellationReason":""}
    # Beispiel Job-JSON Anfragenausführung beendet:
    # {"jobState":"COMPLETED","rowCount":30,"errorMessage":"","startedAt":"2020-11-16T09:22:16.928Z","endedAt":"2020-11-16T09:22:17.997Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-16T09:22:17.474Z","resourceSchedulingEndedAt":"2020-11-16T09:22:17.505Z","cancellationReason":""}

arr = os.listdir(datenPath)

for i in arr:
    filePath = datenPath + "/" + i
    sql_file = open(filePath, "r")
    sql_txt = sql_file.read()
    #fügt die SQL anfrage in die Request ein -> New Lines und gewisse Zeichen müssen gesetzt werden
    sql_text = "{" + sql_txt + "}"  # \\\ nicht möglich nur in {} in python möglich
    sql_txt = sql_txt.replace("\"",
                              "\\\"")  # json muss das Zeichen " mit einer Ausnahme bekommen und das Zecichen \ wird von python string aufgebraucht deswegen = \\\
    sqla = "{\n\t\"sql\": \"" + sql_txt.replace("\n",
                                                "") + "\"\n}"  # new line die durch die {} add entsteht wird entfernt
    # print(sqla)
    sqlquerie(sqla, i, token, abfrageZeitServer, hostAdresse)  # i=fileName=TableNameNR

#speichert mappingErgebnis(tablenAMeQueryNR, job_id) als json -> benötigt die Auswertung (jobAuslesen2.py)
path += "queueJobIDTable.json"
text_file = open(path, "w")
result = json.dumps(mappingIDqueries)
n = text_file.write(result)
text_file.close()

#führt automatisch Auswertung der Anfragen aus
jobAuslesen2.jobAuslesen2(path, token, speicherPath, hostAdresse)