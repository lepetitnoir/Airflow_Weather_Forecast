import requests
import json
import datetime as dt

def get_city():
    address = "api.openweathermap.org/data/2.5/weather?q="
    key = "&appid=1e3e215327b663856b574dc1fc11fb70"
    weather = []
    r1 = requests.get(
        url="https://{address}london{key}".format(address=address, key=key)
        )
    weather.append(r1.json())
    r2 = requests.get(
        url="https://{address}paris{key}".format(address=address, key=key)
        )
    weather.append(r2.json())
    r3 = requests.get(
        url="https://{address}washington{key}".format(address=address, key=key)
        )
    weather.append(r3.json()) 
    now = dt.datetime.now().isoformat(timespec='minutes').replace("T","_")
    with open("/app/raw_files/{now}.json".format(now=now), "w") as outfile:
        json.dump(weather, outfile)
    return
