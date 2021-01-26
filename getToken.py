import requests

##Hier werden Tokens für die Anfragen erstellt
#Tokens = wie ein Login für die REST API

#Authentication
#https://docs.dremio.com/rest-api/overview.html


def getToken(hostAdresse):
    url = hostAdresse + "/apiv2/login"

    ##Login-Daten
    #Bei einem Single-Node-Cluster sind dies die Default-Daten
    if(hostAdresse=="http://localhost:9047"):#local Server
        payload = '''{
              "userName": "dremio",
              "password": "dremio123"
        }'''

    ###hier falls kein Single-Node-Cluster benutzt wird oder Accounts erstellt werden:
    ##if(hostAdresse=="http://000.00.00.00:9047"): #0er ersetzten durch Adresse
    ##falls andere Adresse genutzt wird --> auch in Methoden ändern/setzten
    #    payload = '''{
    #      "userName": "Hallo",
    #      "password": "Hallo123"
    #    }'''

    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    #print(response.text)
    return response.text
