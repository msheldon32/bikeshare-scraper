# scraper

import threading
import csv
import os
import json
import requests
import time
import datetime

#parameters
frequency = 15

def pull_gbfs(name, url):
    print(f"name: {name}, url: {url}")
    print("pwd")
    
    schema = requests.get(url).json()["data"]
    schema = list(schema.values())[0]["feeds"]
    for page in schema:
        with open("{}/{}".format(name,page["name"]), "a") as file:
            data = requests.get(page["url"]).json()
            data["date"] = str(datetime.datetime.now())
            file.write(json.dumps(data))
            file.write("\n")

def pull_data():
    all_folders = os.listdir()
    with open("api_sources") as file:
        api_reader = csv.reader(file)
        for row in api_reader:
            if row[0] not in all_folders:
                os.mkdir(row[0])
            row_thread = threading.Thread(target=pull_gbfs, args=row[:2], daemon=True)
            row_thread.start()

if __name__=="__main__":
    while True:
        pull_data()
        time.sleep(frequency*60)
