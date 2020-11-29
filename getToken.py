import requests

def getToken(hostAdresse):
    # mein Server
    if(hostAdresse=="http://localhost:9047"):
        url = "http://localhost:9047/apiv2/login"
        payload = '''{
              "userName": "dremio",
              "password": "dremio123"
        }'''
    else:#(hostAdresse=="http://141.76.47.15:9047"):
        # Dirk Server
        url = hostAdresse+"/apiv2/login"
        payload = '''{
          "userName": "greim",
          "password": "greim45$"
        }'''
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    #print(response.text)
    return response.text

