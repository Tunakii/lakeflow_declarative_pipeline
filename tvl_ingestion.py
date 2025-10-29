import requests
import json
from datetime import datetime

tiers = ["ingestion", "databronze", "datasilver", "datagold"]
adls_path = {tier: f"abfss://{tier}@devdataxxxx.dfs.core.windows.net" for tier in tiers}

#Access paths
ingestion_adls = adls_path["ingestion"]
bronze_adls = adls_path["databronze"]
silver_adls = adls_path["datasilver"]
gold_adls = adls_path["datagold"]

dbutils.fs.ls(ingestion_adls)
dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

#Call restURL
restURL = "https://restcountries.com/v3.1/all?fields=name,capital,flag,region,currencies,population"
#Add Date
dbutils.widgets.text(
    "run_date",
    datetime.today().strftime("%Y-%m-%d"),
    "run date"
)
run_date = dbutils.widgets.get("run_date")
#GET data from rest call
try:
    # Make the GET request to fetch data
    response = requests.get(restURL)
    # Raise HTTPError if the request was unsuccessful
    response.raise_for_status()

    data = response.json()
    if not data:
        print("There was no data found.")
    else:
        file_path = f"{ingestion_adls}/countries/{run_date}_restCountries.json"
        # Save the JSON data to the ADLS path 
        json_str = json.dumps(data)
        dbutils.fs.put(file_path, json_str, overwrite=True)
        print(f"Data saved to {file_path}")
except requests.exceptions.RequestException as exception:
    print(f"Error: {exception}")
