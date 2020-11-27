import json
from datetime import datetime

import getJob
import getReflection
# json.dumps() json->string
# json.loads() string->json
# token = "10p2mmuo4ssl5qm9ehqafmbr67"
# path = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/queueJobIDTable.json"
# path = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/RAW.json"
def jobAuslesen2(pathJobIdTable, token, speicherPath):
    queueJobIDTable = open(pathJobIdTable)
    # als json
    data_json = json.load(queueJobIDTable)# json.load() für keine strings
    # print(data_json)
    queueJobIDTable.close() #closen nachdem data eingelesen
    ausgebenDaten = ""
    ausgebenColumnName ="queryName,queryStatus,queryTime,resourceSchedulingTime"
    RAW = False
    AGG = False
    queryTypReflection = "withNoReflections"
    x = 0 #Anzahl wie oft ReflectionColumnNames an die columnNames rangehangen
    for (k, v) in data_json.items():
        ausgebenDaten+=k
        # print("Key: " + k) # Key: Arade_1_NR2.sql
        # print("Value: " + str(v)) # Value(id): 204da5df-f450-d332-1865-b829a8fd8400
        job_profil = getJob.get_job(v, token)
        # print("HERE us: "+job_profil) #{"jobState":"COMPLETED","rowCount":100,"errorMessage":"","startedAt":"2020-11-16T10:53:22.896Z","endedAt":"2020-11-16T10:53:23.423Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-16T10:53:23.079Z","resourceSchedulingEndedAt":"2020-11-16T10:53:23.095Z","cancellationReason":""}
        #AGG&RAW
        #{"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-17T10:29:20.367Z","endedAt":"2020-11-17T10:29:24.830Z","acceleration":{"reflectionRelationships":[{"datasetId":"dc88e753-ebd7-4e0d-ad76-d0ca9e7c6323","reflectionId":"2329c428-8c4a-4f2c-8454-cc145bba9a6a","relationship":"CONSIDERED"},{"datasetId":"dc88e753-ebd7-4e0d-ad76-d0ca9e7c6323","reflectionId":"008e40a7-9bf1-4ffb-a285-5207f4b28607","relationship":"CHOSEN"}]},"queryType":"REST","queueName":"SMALL","queueId":"SMALL","resourceSchedulingStartedAt":"2020-11-17T10:29:20.831Z","resourceSchedulingEndedAt":"2020-11-17T10:29:20.839Z","cancellationReason":""}
        job_profil_json = json.loads(job_profil) # here time schema yyyy-mm-ddThh:min:sec.mmmZ
        ausgebenDaten+=","+job_profil_json.get('jobState')
        if job_profil_json.get('jobState') == "COMPLETED":
            timeJobStart = job_profil_json.get('startedAt').replace("T", " ").replace("Z", "")
            timeJobEnd = job_profil_json.get('endedAt').replace("T", " ").replace("Z", "")
            startDate = datetime.strptime(timeJobStart, "%Y-%m-%d %H:%M:%S.%f")
            endDate = datetime.strptime(timeJobEnd, "%Y-%m-%d %H:%M:%S.%f")
            jobTimeResult = endDate-startDate
            ausgebenDaten += "," + str(jobTimeResult)
            scheduleTimeBeginning = job_profil_json.get('resourceSchedulingStartedAt').replace("T", " ").replace("Z", "")
            scheduleTimeEnding = job_profil_json.get('resourceSchedulingEndedAt').replace("T", " ").replace("Z", "")
            scheduleTimeBeginning2 = datetime.strptime(scheduleTimeBeginning, "%Y-%m-%d %H:%M:%S.%f")
            scheduleTimeEnding2 = datetime.strptime(scheduleTimeEnding, "%Y-%m-%d %H:%M:%S.%f")
            schedulingTime = (scheduleTimeEnding2-scheduleTimeBeginning2)
            ausgebenDaten += "," + str(schedulingTime)
            # print(jobTimeResult)
            # ohne reflection
            # {"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-17T08:22:23.067Z","endedAt":"2020-11-17T08:25:07.223Z","queryType":"REST","queueName":"LARGE","queueId":"LARGE","resourceSchedulingStartedAt":"2020-11-17T08:22:23.776Z","resourceSchedulingEndedAt":"2020-11-17T08:22:23.798Z","cancellationReason":""}
            i = 0  # schleifen Abruf für reflection
            while True:
                try: # returns none wenn nicht accelerated
                    reflectionID1 = job_profil_json['acceleration']['reflectionRelationships'][i]['reflectionId']
                    reflectionChoosen1 = job_profil_json['acceleration']['reflectionRelationships'][i]['relationship']
                    # print("erste Reflection: "+getReflection.get_reflection(reflectionID1, token))
                    acc1Job = json.loads(getReflection.get_reflection(reflectionID1, token))
                    #BSP-refl: {"id":"0fc64806-0a09-4cf4-83b2-808f472bbdfa","type":"AGGREGATION","name":"Aggregation Reflection","tag":"nV/W2xgzRcY=","createdAt":"2020-11-16T16:36:03.008Z","updatedAt":"2020-11-16T16:36:03.008Z","datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","currentSizeBytes":0,"totalSizeBytes":0,"enabled":false,"arrowCachingEnabled":false,"status":{"config":"OK","refresh":"SCHEDULED","availability":"NONE","combinedStatus":"DISABLED","failureCount":0,"lastDataFetch":"1970-01-01T00:00:00.000Z","expiresAt":"1970-01-01T00:00:00.000Z"},"dimensionFields":[{"name":"F6","granularity":"DATE"},{"name":"F7","granularity":"DATE"},{"name":"Number of Records","granularity":"DATE"},{"name":"F3","granularity":"DATE"},{"name":"F1","granularity":"DATE"},{"name":"WNET (bin)","granularity":"DATE"},{"name":"F2","granularity":"DATE"}],"measureFields":[{"name":"F9","measureTypeList":["COUNT","SUM"]},{"name":"F4","measureTypeList":["COUNT","SUM"]},{"name":"F8","measureTypeList":["COUNT","SUM"]},{"name":"F5","measureTypeList":["COUNT","SUM"]}],"partitionDistributionStrategy":"CONSOLIDATED","entityType":"reflection"}
                    #acc1Size = acc1Job['totalSizeBytes']
                    reflectionName = acc1Job['name']

                    acc1Size2 = acc1Job['currentSizeBytes']
                    typeReflection = acc1Job['type']
                    #if typeReflection=="RAW": queryTypReflection = "withRawReflections"
                    # else: queryTypReflection = "withAggregationReflections"
                    if typeReflection=="RAW": RAW = True
                    elif typeReflection=="AGGREGATION": AGG = True
                    sizeReflection1 = ((acc1Size2 / 1024) / 1024)  # in MebiBytes
                    # print(k+" ,type: "+typeReflection+", size in MB: "+ str(sizeReflection1)+", status: "+reflectionChoosen1+", zeit: "+str(jobTimeResult))
                    ausgebenDaten+=","+typeReflection+","+reflectionName+","+str(sizeReflection1)+","+reflectionChoosen1
                    i=i+1 #schleife Reflection abruf erhöht #nach abbruch while ReflektionAnzahl pro Query
                except (KeyError, IndexError):
                    break
                    pass
            if (i > x): x = i #checkt die max ReflectionAnzahl pro Query für ColumnName
        ausgebenDaten += "," + v
        ausgebenDaten += "\n"
    for z in range(x):
        ausgebenColumnName += ",ReflectionTyp" + str(z) + ",ReflectionName" + str(z) + ",ReflectionSize" + str(z)
        #std mäßig withNoReflection. Bei andere Reflection genutzt setzt
    if (RAW==True and AGG==False): queryTypReflection = "withRAWReflections"
    elif (RAW == False and AGG==True): queryTypReflection = "withAGGReflections"
    elif (RAW == True and AGG==True): queryTypReflection = "withBothReflections"
    ausgebenColumnName+=",QueryID"

    ausgebenDaten=ausgebenColumnName+"\n"+ausgebenDaten

    print(ausgebenDaten)
    #bsp: Bimbo_1_NR4.sql,COMPLETED,5078,0:00:07.059000,AGGREGATION,0.08037948608398438,CONSIDERED,RAW,903.67444896698,CHOSEN
    fileNamePath = speicherPath+queryTypReflection+"_unzipped"
    #fileNamePath = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/"+queryTypReflection+"_Join"
    text_file = open(fileNamePath, "w")
    text_file.write(ausgebenDaten)
    text_file.close()

    # JobIdTable.json speichern
    # {"jobState":"COMPLETED","rowCount":130,"errorMessage":"","startedAt":"2020-11-16T15:23:22.407Z","endedAt":"2020-11-16T15:23:31.335Z","acceleration":{"reflectionRelationships":[{"datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","reflectionId":"162e9073-86f7-40bc-a214-3f533e9cec4a","relationship":"CHOSEN"},{"datasetId":"f22d3ac8-2159-485c-8df7-036d62259fba","reflectionId":"0fc64806-0a09-4cf4-83b2-808f472bbdfa","relationship":"CONSIDERED"}]},"queryType":"REST","queueName":"SMALL","queueId":"SMALL","resourceSchedulingStartedAt":"2020-11-16T15:23:23.789Z","resourceSchedulingEndedAt":"2020-11-16T15:23:23.827Z","cancellationReason":""}
    # fileNamePathJson = "/media/jonas/TOSHIBA EXT/Jonas/Skripte/testDurchlaufJobs/" + queryTypReflection+".json"
    # json_file = open(fileNamePathJson, "w")
    # json_file.write(data_json)
    # text_file.close()