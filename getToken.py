import requests

def getToken():
    url = "http://localhost:9047/apiv2/login"
    payload = '''{
      "userName": "dremio",
      "password": "dremio123"
    }'''
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache",
        'Postman-Token': "e3627456-a277-457f-8734-1bd78658e6df"
    }

    response = requests.request("POST", url, data=payload, headers=headers)
    #print(response.text)
    return response.text

