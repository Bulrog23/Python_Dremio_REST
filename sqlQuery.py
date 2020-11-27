import os
import requests
import json
import getJob
import time
import jobAuslesen2
import getToken

mappingIDqueries = dict()
datenPath = "/media/jonas/TOSHIBA EXT/Jonas/testQuery"
path = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/" #ZwischenSpeicher für MappingQueryId.json
speicherPath = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/" #Speicherort der auswertungsFiles
abfrageZeitServer = 30 #30 sec abfragenOb query completed

gotToken = json.loads(getToken.getToken())
token = gotToken.get('token')

def sqlquerie(payload, tableNameNumber, token, abfrageZeit):
    url = "http://localhost:9047/api/v3/sql"
    headers = {
        'Authorization': "_dremio" + token,
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e568431f-3935-46bd-b7e6-9e8adf44e606"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    print(response.text)
    response_native = json.loads(response.text)
    # print(response_native.get('id'))  # return id bsp: 204fa4f5-23d8-1995-fc1d-3b3499e57500
    mappingIDqueries[tableNameNumber] = response_native.get(
        'id')  # added Eintrag zu dic wenn tableNameNr(key) noch nicht vorhanden
    print(mappingIDqueries)
    # getJob.get_job(response_native.get('id'), token)
    response_job = json.loads(getJob.get_job(response_native.get('id'), token))
    # print(response_job.get('jobState'))
    status_job = response_job.get('jobState')
    while status_job != "COMPLETED" and status_job != "CANCELED" and status_job != "FAILED":
        time.sleep(abfrageZeit)
        response_job = json.loads(getJob.get_job(response_native.get('id'), token))
        status_job = response_job.get('jobState')
        print(status_job)
    print("Queue completed")
    # {"jobState":"METADATA_RETRIEVAL","errorMessage":"","startedAt":"2020-11-16T09:22:16.894Z","queryType":"REST","cancellationReason":""}
    # {"jobState":"COMPLETED","rowCount":30,"errorMessage":"","startedAt":"2020-11-16T09:22:16.928Z","endedAt":"2020-11-16T09:22:17.997Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-16T09:22:17.474Z","resourceSchedulingEndedAt":"2020-11-16T09:22:17.505Z","cancellationReason":""}


# datenPath = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/sqlQueries_bearbeitet_einzeln"

# loopt durch alle txt files in geg Ordner
arr = os.listdir(datenPath)
for i in arr:
    filePath = datenPath + "/" + i
    sql_file = open(filePath, "r")
    sql_txt = sql_file.read()
    sql_text = "{" + sql_txt + "}"  # \\\ nur in {} in python möglich
    sql_txt = sql_txt.replace("\"",
                              "\\\"")  # json muss die " mit ausnahme bekommen und ein \ wird von python string gebraucht = \\\
    sqla = "{\n\t\"sql\": \"" + sql_txt.replace("\n",
                                                "") + "\"\n}"  # muss new line die durch die {} add entsteht entfernen
    # print(sqla)
    sqlquerie(sqla, i, token, abfrageZeitServer)  # i=fileName=tableName

#speichert mappingErgebnis(tablenAMeQueryNR, job_id) als json
path += "queueJobIDTable.json"
text_file = open(path, "w")
result = json.dumps(mappingIDqueries)
n = text_file.write(result)
text_file.close()

jobAuslesen2.jobAuslesen2(path, token, speicherPath)