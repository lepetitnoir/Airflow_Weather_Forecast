def get_city() -> None:
    import datetime as dt
    from dotenv import load_dotenv, find_dotenv
    import json
    import os
    import requests

    address = "https://api.openweathermap.org/data/2.5/weather"
    load_dotenv(find_dotenv)
    key = os.environ.get("API_KEY")
    weather = []
    for city in ["london", "paris", "washington"]:
        response = requests.get(url=address, params={"q": city, "appid": key})
        response.raise_for_status()
        weather.append(response.json())

    now = dt.datetime.now().isoformat(timespec="minutes").replace("T", "_")
    with open("/app/raw_files/{now}.json".format(now=now), "w") as outfile:
        json.dump(weather, outfile)

