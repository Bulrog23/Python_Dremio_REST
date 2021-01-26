import json
from datetime import datetime

import getJob
import getReflection

##Hier werden die Zeiten/Informationen/Daten einer SQL-Anfrage ausgelesen und in eine Ergebnisdatei geschrieben
#Die Zwischenergebnisse(gesuchte Daten), die aus der JSON extrahiert werden, werden nach nach einer Txt Datei eingehangen
#wurde mit SQL-Query verbunden

#GET /reflection/{id}
#https://docs.dremio.com/rest-api/reflections/get-reflection-id.html
#GET /job
#https://docs.dremio.com/rest-api/jobs/get-job.html

##falls Methode ohne SQL Query durchgeführt wird -> muss ein JSON-Datei-Speicherort angegeben werden
# path = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/queueJobIDTable.json"
#hostAdresse = "http://localhost:9047" #Single-Node-Adresse
# gotToken = json.loads(getToken.getToken(hostAdresse))
# token = str(gotToken.get('token'))
def jobAuslesen2(pathJobIdTable, token, speicherPath, hostAdresse):
    queueJobIDTable = open(pathJobIdTable)
    data_json = json.load(queueJobIDTable)
    # print(data_json)
    queueJobIDTable.close() #Nachdem Daten eingelesen
    ausgebenDaten = ""
    ausgebenColumnName ="queryName,queryStatus,queryTime,resourceSchedulingTime" #für die CSV Ergebnis Datei die Spaltennamen(=Erste Zeile)
    RAW = False #wird für Prüfung benötigt, ob eine RAW-Reflection für den Dataset erstellt wurde
    AGG = False #wird für Prüfung benötigt, ob eine Aggregation-Reflection für den Dataset erstellt wurde
    queryTypReflection = "withNoReflections"
    x = 0 #Anzahl wie oft ReflectionColumnNames an die columnNames rangehangen wurde
    #da mehr Reflections für einen Dataset kreiert werden können (->Länge der Ergebnis-Zeile unsicher)

    for (k, v) in data_json.items(): #Es wird durch die JSON iteriert
        # Key(Dataset)-Beispiel: Arade_1_NR2.sql
        # Value(Job-id)-Beispiel: 204da5df-f450-d332-1865-b829a8fd8400
        ausgebenDaten+=k
        job_profil = getJob.get_job(v, token, hostAdresse)
        # Beispiel JSON-Job-Daten ohne Reflections #{"jobState":"COMPLETED","rowCount":100,"errorMessage":"","startedAt":"2020-11-16T10:53:22.896Z","endedAt":"2020-11-16T10:53:23.423Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-16T10:53:23.079Z","resourceSchedulingEndedAt":"2020-11-16T10:53:23.095Z","cancellationReason":""}
        #Beispiel JSON-Job-Daten mit Reflections #{"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-17T10:29:20.367Z","endedAt":"2020-11-17T10:29:24.830Z","acceleration":{"reflectionRelationships":[{"datasetId":"dc88e753-ebd7-4e0d-ad76-d0ca9e7c6323","reflectionId":"2329c428-8c4a-4f2c-8454-cc145bba9a6a","relationship":"CONSIDERED"},{"datasetId":"dc88e753-ebd7-4e0d-ad76-d0ca9e7c6323","reflectionId":"008e40a7-9bf1-4ffb-a285-5207f4b28607","relationship":"CHOSEN"}]},"queryType":"REST","queueName":"SMALL","queueId":"SMALL","resourceSchedulingStartedAt":"2020-11-17T10:29:20.831Z","resourceSchedulingEndedAt":"2020-11-17T10:29:20.839Z","cancellationReason":""}

        job_profil_json = json.loads(job_profil)
        ausgebenDaten+=","+job_profil_json.get('jobState')

        if job_profil_json.get('jobState') == "COMPLETED": #Wenn SQL-Anfrage erfolgreich berechnet wurde -> Auswertung
            timeJobStart = job_profil_json.get('startedAt').replace("T", " ").replace("Z", "") # Zeit-Schema in JSON: yyyy-mm-ddThh:min:sec.mmmZ
            timeJobEnd = job_profil_json.get('endedAt').replace("T", " ").replace("Z", "") #es wird das T und Z entfernt
            startDate = datetime.strptime(timeJobStart, "%Y-%m-%d %H:%M:%S.%f") #Zeiten in Python konformes Zeit-Format
            endDate = datetime.strptime(timeJobEnd, "%Y-%m-%d %H:%M:%S.%f")
            jobTimeResult = endDate-startDate
            ausgebenDaten += "," + str(jobTimeResult)
            # ohne reflection
            # {"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-17T08:22:23.067Z","endedAt":"2020-11-17T08:25:07.223Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-17T08:22:23.776Z","resourceSchedulingEndedAt":"2020-11-17T08:22:23.798Z","cancellationReason":""}
            i = 0  # schleifen Abruf für reflection

            #Hier werden die Reflections-Ausgewertet
            #Es wird aus der JSON-Job Datei, die Reflection-Id ausgelesen -> die wieder abgefragt und ausgelesen wird
            while True:
                try: # returns none wenn keine Reflection vorhanden sonst iteriert es durch alle Reflections

                    reflectionID1 = job_profil_json['acceleration']['reflectionRelationships'][i]['reflectionId']
                    reflectionChoosen1 = job_profil_json['acceleration']['reflectionRelationships'][i]['relationship']
                    # print("erste Reflection: "+getReflection.get_reflection(reflectionID1, token))

                    acc1Job = json.loads(getReflection.get_reflection(reflectionID1, token, hostAdresse)) #JSON-Datei der Reflections wird geladen
                    #Beispiel-Reflection-JSON: {"id":"0fc64806-0a09-4cf4-83b2-808f472bbdfa","type":"AGGREGATION","name":"Aggregation Reflection","tag":"nV/W2xgzRcY=","createdAt":"2020-11-16T16:36:03.008Z","updatedAt":"2020-11-16T16:36:03.008Z","datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","currentSizeBytes":0,"totalSizeBytes":0,"enabled":false,"arrowCachingEnabled":false,"status":{"config":"OK","refresh":"SCHEDULED","availability":"NONE","combinedStatus":"DISABLED","failureCount":0,"lastDataFetch":"1970-01-01T00:00:00.000Z","expiresAt":"1970-01-01T00:00:00.000Z"},"dimensionFields":[{"name":"F6","granularity":"DATE"},{"name":"F7","granularity":"DATE"},{"name":"Number of Records","granularity":"DATE"},{"name":"F3","granularity":"DATE"},{"name":"F1","granularity":"DATE"},{"name":"WNET (bin)","granularity":"DATE"},{"name":"F2","granularity":"DATE"}],"measureFields":[{"name":"F9","measureTypeList":["COUNT","SUM"]},{"name":"F4","measureTypeList":["COUNT","SUM"]},{"name":"F8","measureTypeList":["COUNT","SUM"]},{"name":"F5","measureTypeList":["COUNT","SUM"]}],"partitionDistributionStrategy":"CONSOLIDATED","entityType":"reflection"}

                    reflectionName = acc1Job['name']
                    acc1Size2 = acc1Job['currentSizeBytes'] #Reflection-Größe in MebiBytes
                    typeReflection = acc1Job['type'] #Ob RAW oder Aggregation

                    if typeReflection=="RAW": RAW = True #RAW-Reflection vorhanden
                    elif typeReflection=="AGGREGATION": AGG = True #Aggregation-Reflection vorhanden

                    sizeReflection1 = ((acc1Size2 / 1024) / 1024)  # Umrechnung in MB

                    ausgebenDaten+=","+typeReflection+","+reflectionName+","+str(sizeReflection1)+","+reflectionChoosen1
                    i=i+1 #erhöht Reflektion-Anzahl pro SQL-Anfrage
                except (KeyError, IndexError):
                    break
                    pass
            if (i > x): x = i #checkt die max Reflection-Anzahl pro Anfrage für ColumnName
        ausgebenDaten += "," + v #Job-Id
        ausgebenDaten += "\n"
    for z in range(x):
        ausgebenColumnName += ",ReflectionTyp" + str(z) + ",ReflectionName" + str(z) + ",ReflectionSize" + str(z)

        ##erstellt eine andere Ergebnisdatei (->Ergebnisdateien: keine, RAW, Aggregation, Beide)
        #standardmäßig mit withNoReflection sonst
    if (RAW==True and AGG==False): queryTypReflection = "withRAWReflections"
    elif (RAW == False and AGG==True): queryTypReflection = "withAGGReflections"
    elif (RAW == True and AGG==True): queryTypReflection = "withBothReflections"
    ausgebenColumnName+=",QueryID"

    ausgebenDaten=ausgebenColumnName+"\n"+ausgebenDaten

    print(ausgebenDaten)
    #Beispiel Ergebniszeile: Bimbo_1_NR4.sql,COMPLETED,5078,0:00:07.059000,AGGREGATION,0.08037948608398438,CONSIDERED,RAW,903.67444896698,CHOSEN

    #Schreibt das Endergebnis in eine Datei
    fileNamePath = speicherPath+queryTypReflection+"_unzipped"
    text_file = open(fileNamePath, "w")
    text_file.write(ausgebenDaten)
    text_file.close()

    ## falls JSON gespeichert werden soll (JobIdTable.json)
    # {"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-16T15:23:22.407Z","endedAt":"2020-11-16T15:23:31.335Z","acceleration":{"reflectionRelationships":[{"datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","reflectionId":"162e9073-86f7-40bc-a214-3f533e9cec4a","relationship":"CHOSEN"},{"datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","reflectionId":"0fc64806-0a09-4cf4-83b2-808f472bbdfa","relationship":"CONSIDERED"}]},"queryType":"REST","queueName":"SMALL","queueId":"SMALL","resourceSchedulingStartedAt":"2020-11-16T15:23:23.789Z","resourceSchedulingEndedAt":"2020-11-16T15:23:23.827Z","cancellationReason":""}
    # fileNamePathJson = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/" + queryTypReflection+".json"
    # json_file = open(fileNamePathJson, "w")
    # json_file.write(data_json)
    # text_file.close()