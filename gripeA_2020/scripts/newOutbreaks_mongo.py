import requests
from requests_html import HTMLSession
from pymongo import MongoClient
import sys
import pandas as pd
import json
from datetime import datetime, timedelta, date
import pygeohash as geohash
import numpy as np
from bs4 import BeautifulSoup
import time

# GLOBALS
client = MongoClient('mongodb://localhost:27017/')
db = client.lv
outbreaks = db.outbreaks

#Load csv outbreaks (2017-2021)
def loadOutbreaks():
    file = 'data/AvianInfluenza.csv'
    df = pd.read_csv(file, sep=",")
    #Renombramos
    df.rename(columns={'Event ID': 'oieid', 'Disease': 'disease', 'Serotype': 'serotype', 'Locality': 'city', 
        'lon': 'long', 'Country': 'country', 'Region': 'region','Location': 'location', 'Species': 'species', 'Date': 'date'}, inplace=True)
    
    #Solo nos quedamos con brotes de Europa
    indexNames = df[ df['region'] != 'Europe' ].index
    df.drop(indexNames , inplace=True)
    
    df = df.fillna(value="No Data")

    #Convertimos string a datetime columna Report Date
    df['report_date'] = pd.to_datetime(df['report_date'], format='%Y-%m-%d')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    df = webScraping(df)

    records = df.to_dict(orient='records')
    outbreaks.delete_many({})
    
    outbreaks.insert_many(records)

#WebScrapping to get Cases and Deaths
#Parameters -> list(idOutbreak)

def webScraping(df):
    cases = []
    deaths = []
    animalType = []
    geohashA = []
    fullReport=[]
    observationDate = []
    country = []
    payload = json.dumps({})
    error = False
    for i in df.index:
        #url = "http://empres-i.fao.org/empres-i/obdj?id={}&lang=EN".format(df['oieid'][i])
        url = "http://empres-i.fao.org/eipws3g/2/obd?idOutbreak={}&lang=EN".format(df['oieid'][i])
        #print(url)
        session = HTMLSession()
        r = session.get(url)
        #r = session.get(url,
        #    data = payload,  
        #    headers={
        #        'Host': 'empres-i.fao.org',
                # 'Connection': 'keep-alive',
        #        'Pragma': 'no-cache',
        #        'Cache-Control': 'no-cache',
        #        'Content-Type': 'application/json',
        #        'Accept': 'application/json',
                #'DNT': '1',
        #        'X-Requested-With': 'XMLHttpRequest',
                # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36 Edg/89.0.774.57',
                #'Referer': 'http://empres-i.fao.org/empres-i/2/obd?idOutbreak=287665',
                #'Accept-Encoding': 'gzip',
                #'Accept-Language': 'en'
         #   })
        try:
             r.html.render(wait=2,sleep=2)
             gt = r.html.find('table', containing='SPECIES AFFECTED',first=True)
        except:
            print("HTML render error. Skip entry",file = sys.stderr)
            error = True
            #continue
        try:
            soup = BeautifulSoup(gt.html,'lxml')
            table = soup.find_all('table')[0]
            rows=table.find_all('tr')
        except:
            print("Error when parsing URL. Not table found gt = ",file = sys.stderr)
            if gt is not None:
                print(gt,file = sys.stderr)
            #continue 
            error = True
        #s = json.loads(r.html.text)
        
        #Carga de valores obtenidos por requests en variables
        try:
            #casos = s['outbreak']['speciesAffectedList'][0]['cases']
            casos = rows[2].find_all('td')[4].get_text()
        except:
            casos = ""
            error = True
        try:
            #muertes = s['outbreak']['speciesAffectedList'][0]['deaths']
            muertes = rows[2].find_all('td')[5].get_text()
        except:
            muertes = ""
            error = True
        try:
            #animal = s['outbreak']['speciesAffectedList'][0]['animalType']
            animal = rows[2].find_all('td')[0].get_text()
        except:
            animal = ""
            error = True

        #Geohash
        valueGeohash = geohash.encode(float(df['lat'][i]), float(df['long'][i]))

        #Guardado en listas
        cases.append(casos)
        deaths.append(muertes)
        animalType.append(animal)
        geohashA.append(valueGeohash)
        #fullReport.append("http://empres-i.fao.org/empres-i/2/obd?idOutbreak={}".format(df['oieid'][i]))
        fullReport.append("http://empres-i.fao.org/eipws3g/2/obd?idOutbreak={}".format(df['oieid'][i]))


        #Si el valor de ObservationDate es NaN, ponemos el valor del reporte
        valOb = df['report_date'][i] if (df['observation_date'][i] == "No Data") else datetime.strptime( df['observation_date'][i], '%Y-%m-%d')
        observationDate.append(valOb)

        try:
            locT = r.html.find('table', containing='LOCATION',first=True)
            soup = BeautifulSoup(locT.html,'lxml')
            table = soup.find_all('table')[0]
            rows=table.find_all('tr')
            cntry=rows[2].find_all('td')[1].get_text()
        except:
            cntry=""
            print("Error when parsing URL. Not LOCATION found ",file = sys.stderr)
            error = True
            #continue
        #Cambiar nombres de reino unido
        if "U.K. of Great Britain and Northern Ireland" in cntry:
            country.append(cntry.split(" ")[0])
        else: 
            country.append(df['country'][i])
        if error:
            print(url, file = sys.stderr)
        error = False
	# Try to wait before next connection
        time.sleep(1)

    df['cases'] = cases
    df['deaths'] = deaths
    df['epiunit'] = animalType 
    df['geohash'] = geohashA
    df['urlFR'] = fullReport
    df['observation_date'] = observationDate
    df['country'] = country


    return df


#Download outbreaks last week
def downloadOutbreaks():
    #Descargamos el archivo 
    #url = "https://us-central1-fao-empres-re.cloudfunctions.net/getEventsInfluenzaAvian"
    #url = "https://europe-west1-fao-empres-re.cloudfunctions.net/getEventsInfluenzaAvian"
    # OJO: esta URL solo vale para 2022 !!!!!!
    url = "https://europe-west1-fao-empres-re.cloudfunctions.net/getEventsInfluenzaAvian?start_date=2022-01-01&end_date=2022-12-31&serotype=all&diagnosis_status=confirmed&animal_type=all"
    #url = "https://empresi-shiny-app-dot-fao-empres-re.ew.r.appspot.com/session/b580a60d31e3a4ade4f0d3d5da8fbed4/download/overview-rawDataDownload?w="
    myFile = requests.get(url)
    #Guardamos
    open("data/outbreaksWeeks.csv", 'wb').write(myFile.content)
    #Abrimos csv para quedarnos con brotes nuevos de la ultima semana
    df = pd.read_csv('data/outbreaksWeeks.csv')
    df.rename(columns={'event_id': 'oieid', 'Disease': 'disease', 'Serotype': 'serotype', 'locality': 'city', 
        'lon': 'long', 'Country': 'country', 'Location': 'location', 'Species': 'species', 'display_date': 'date'}, inplace=True)
    
    #Solo con brotes de Europa
    # TODO esto deberia ser un parametro de la herramienta
    indexNames = df[ df['region'] != 'Europe' ].index
    df.drop(indexNames , inplace=True)

    df = df.fillna(value="No Data")

    #Buscar los de la ultima semana
    #fecha de hoy
    today = datetime.today()
    #Lunes de esta semana
    monday = today + timedelta(days = -today.weekday())
    #Semana anterior 
    lastWeek = monday - timedelta(weeks = 1)


    # NACHO. Cambio para incluir fechas arbitrarias en BD
    #firstWeek = date(2022,3,7)
    #lastWeek = date(2022,4,25) 
    ####  Fin CAMBIO

    #Indices para borrar el resto de filas
    dfAux = []
    #Convertimos string a datetime columna Report Date
    df['report_date'] = pd.to_datetime(df['report_date'], format='%Y-%m-%d')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    for i in df.index:
        
        # if df['observation_date'][i] == "No Data":
        #     dateOutbreak = df['report_date'][i]
        # else:
        #     dateOutbreak = datetime.strptime(df['observation_date'][i], '%Y-%m-%d') 

        dateOutbreak = df['report_date'][i]

        #NACHO: cambio limites temporales si se usan fechas arbitrarias
        # OJO: vamos al continue si el brote NOS intenresa (porque luego se hace drop)
        #if dateOutbreak <= lastWeek or dateOutbreak >= firstWeek:
        if dateOutbreak >= lastWeek and dateOutbreak <= monday:
            #print('Consultando brote con id ' + str(df['oieid'][i])) 
            # Check if it was already in db (if not, do not append it)
            cnt = outbreaks.find({'oieid': df['oieid'][i].item()}).count()
            if (cnt == 0):
                continue

        dfAux.append(i)

 
    df = df.drop(dfAux,axis=0)
    df = webScraping(df)


    records = df.to_dict(orient='records')

    #Si el brote ya existe remplazamos la nueva infomación y si no, lo añade
    print('Updating MongoDb with ' + str(len(records)) + 'new elements') 
    for i in records:
        insert = outbreaks.replace_one({'oieid': i['oieid']}, i, upsert = True)
    

#Main

def main(argv):

    #loadOutbreaks()
    downloadOutbreaks()
    #print("Outbreaks updated")
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])
