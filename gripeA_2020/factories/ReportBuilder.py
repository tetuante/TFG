from .Builder import Builder
from pymongo import MongoClient
import os
from model.gdriveUploader import gDriveUploader
import unicodecsv as csv
import codecs
import os
from os import remove
import zipfile
from datetime import datetime, timedelta, date
import operator
class ReportBuilder(Builder):
    uploader = gDriveUploader()

    def __init__(self):
        super().__init__("report")

    def reportPDF(self, filepath, pdfpath=None):
        if pdfpath == None:
            pdfpath, old_ext = os.path.splitext(filepath)
            pdfpath += ".pdf"

        if os.path.exists(filepath):
            converion = f"pandoc -H markdown/header.sty --latex-engine=xelatex +RTS -K512M -RTS -o {pdfpath} {filepath}"
            os.system(converion)
        else:
            print(f"El fichero markdown {filepath} no existe")
        # output = pypandoc.convert_file(filepath, 'pdf', outputfile=pdfpath, extra_args=['-H', 'markdown/header.sty', '--latex-engine', 'xelatex', '+RTS', '-K512M', '-RTS'])
        # pandoc -H markdown/header.sty +RTS -K512M -RTS -o markdown/InformeSemanal_28-12-2020.pdf markdown/InformeSemanal_28-12-2020.md
        return pdfpath

    def file_to_drive(self, filepath, title=None, folder=None):
        self.uploader.upload_file(filepath, title, folder)

    def update_drive(self, start):
        #Todos los archivos del drive
        files = self.uploader.get_file_from(foldername="alertas")
        # Desde "weeks" semanas atras hasta esta semana
        this_many_weeks = 52
        startV = start - timedelta(weeks=this_many_weeks)
        #Convert to datetime
        startV = datetime.combine(startV, datetime.min.time())

        for file in files:
            file_name = str(file["title"])
            if file_name.startswith("InformeSemanal") and file_name.endswith(".pdf"):
                file_name = file_name.split(".")[0]
                file_reportDate = datetime.strptime(file_name.split("_")[1], "%d-%m-%Y")
                if startV > file_reportDate:
                    self.uploader.trash_file(file["title"], "alertas")

    def load_csv(self, cabeceraAlertas, cabeceraBrotes, nuevasAlertas, nuevosBrotes, year, month):
        #CSV generales
        alertasDrive = None
        brotesDrive = None
        #Para crear fichero 2020-2021
        if month >= 7: year += 1
        #CSV Alertas
        alertasPath = "markdown/alertasJulio{}Julio{}.csv".format(year-1,year)
        if not os.path.isfile(alertasPath): 
            alertasDrive = codecs.open(alertasPath, "wb+")  
            writer = csv.DictWriter(alertasDrive, fieldnames=cabeceraAlertas)         
            writer.writeheader()
        else: 
            alertasDrive = codecs.open(alertasPath, "ab+") 
            writer = csv.DictWriter(alertasDrive, fieldnames=cabeceraAlertas)

        writer.writerows(nuevasAlertas)
        alertasDrive.close()
        self.file_to_drive(alertasPath, "alertasJulio{}Julio{}.csv".format(year-1,year), "alertas")

        #CSV Brotes
        brotesPath = "markdown/brotesJulio{}Julio{}.csv".format(year-1,year)
        if not os.path.isfile(brotesPath):
            brotesDrive = codecs.open(brotesPath, "wb+")
            writer = csv.DictWriter(brotesDrive, fieldnames=cabeceraBrotes)
            writer.writeheader()
        else:
            brotesDrive = codecs.open(brotesPath, "ab+")
            writer = csv.DictWriter(brotesDrive, fieldnames=cabeceraBrotes)

        writer.writerows(nuevosBrotes)
        brotesDrive.close()
        self.file_to_drive(brotesPath, "brotesJulio{}Julio{}.csv".format(year-1,year), "alertas")
        
    def compress(self, year):
        #Se comprime y se guarda en /zips

        #Creamos el fichero zip
        fileZip = zipfile.ZipFile("markdown/zips/julio{}_julio{}.zip".format(year-1,year), "w")

        #Recorremos la carpeta markdown y buscando los archivos de ese año y los guardamos en el zip 
        for folder, subfolders, files in os.walk("markdown"):
            for file in files:
                comprime = False
                if file.endswith(".pdf"):
                    div = file.split(sep="_")
                    div = div[1].split(sep=".")
                    #Pasamos a datetime la fecha de file
                    div = datetime.strptime(div[0], "%d-%m-%Y")
                    #Limites para escoger ficheros de julio a julio
                    start = datetime(year-1, 7, 1)
                    end = datetime(year, 7, 1)
                    if div >= start and div < end and file.endswith(".pdf"):
                        comprime= True

        
                if comprime or file == "alertasJulio{}Julio{}.csv".format(year-1,year) or file == "brotesJulio{}Julio{}.csv".format(year-1,year):
                    #Guardamos el fichero pdf en el zip
                    fileZip.write(os.path.join(folder, file), file)
                    #Lo borramos de la carpeta del servidork
                    os.remove(os.path.join(folder, file))
                    
                if file.endswith("{}.md".format(year)):
                    try:
                        os.remove(os.path.join(folder, file))
                    except:
                        continue

        fileZip.close()
        #Subimos comprimido a drive
        self.file_to_drive("markdown/zips/julio{}_julio{}.zip".format(year-1,year), "julio{}_julio{}.zip".format(year-1,year), "alertas")    


    def create(self, start, end, parameters):
        
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client.lv
        brotes_db = db.outbreaks
        comarca_db = db.comarcas

        cabecera = ("# DiFLUsion: Informe de Alerta Semanal \n\n - *Fecha*: " +  start.strftime('%d-%m-%Y') 
        + "\n - *Periodo de*: " +   start.strftime('%d-%m-%Y') + " a " + end.strftime('%d-%m-%Y') + "\n")

        cabeceraTablaAlertas = ("\n\n## Tabla de alertas \n" 
        + "| Nº | Fecha  | N alerta | Comarca  | ID CG | Nº brotes | Nº mov. Riesgo | Grado alerta | Temperatura estimada  | Supervivencia del virus (días) |\n"
        + "|:-:|:-------:|:----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-------:|\n")

        cabeceraGenericaTablaBrotesAlertas = "\n\n## Tablas de brotes de IAAP en Europa y su conexión a España a través de  movimientos de aves silvestres"
        
        tablaBrotesAlertas = ("\n| ID | Event ID | Reporting date |Observational date |Country |Location | Latitud | Longitud | An. Type | Species | Cases | Deaths | Especie movimiento |Cód.  Especie | Prob mov semanal |\n" 
        +"|:---:|:---------:|:-------------------:|:----------------:|:---------------------:|:-------------------------:|:------------:|:-----------:|:-------------:|:------------------------:|:--------:|:--------:|:----------------:|:--------------:|:------------------:|\n" )

        #CSV
        csvCabeceraAlertas = ["Nº","Fecha","Nivel de alerta","Comarca","ID CG","Nº brotes","Nº mov. Riesgo","Grado alerta","Temperatura estimada","Supervivencia del virus en días"]
        csvCabeceraBrotes = ["ID","Nº Alerta","Comarca","ID CG", "Grado alerta", "Fecha alerta", "Event ID", "Temperatura estimada", "Supervivencia del virus en días",
        "Reporting date","Observational date", "Country", "Location", "Latitud", "Longitud", "Ponderacion brote", "Riesgo brote", "An. Type","Species", "Cases", "Deaths","Especie movimiento", "Cód.  Especie", "Prob mov semanal"]
        
        
        todosBrotes = ""
        nAlerta = 1
        filasAlertas = ""
        filasBrotes = ""
        nBrote = 0
        allNBrotes = 0

        brotes_set = set()

        #csv
        filasAlertasCsv = []
        filasBrotesCsv = []
        
        #Ordenar de mayor a menor las alertas según el nivel de alerta ya cuantificado
        listAlerts = sorted(parameters['alertas'], key=lambda k : k['alertLevel'], reverse=True) 

        for alerta in listAlerts:
            #Sacar informacion de la comarca
            
            cursor = list(comarca_db.find({'comarca_sg': alerta['comarca_sg']}))
            comarca = cursor[0]

            alerta["temperatura"] = "No data" if alerta['temperatura'] == "No data" else round(alerta["temperatura"],2)
            filasAlertas += ("|" +  str(nAlerta) + "|" + start.strftime('%d-%m-%Y') + "|"+ str(alerta["alertLevel"])+ "|" + comarca['com_sgsa_n'] + "|"+ comarca['com_sgsa_n'] + "|" + alerta['comarca_sg'] 
            + "|" + str(len(alerta['brotes'])) + "|" + str(alerta['movRiesgo']) + "|" + str(round(alerta["valorRiesgo"], 4))+ "|" + str(alerta["temperatura"]) + "|" 
            + str(round(alerta['super'],4)) + "|\n" )

            encabezadoTablasBrotesAlertas = ("\n\n### Alerta {} \n".format(nAlerta)
            + "- *Id comarca*: "+ alerta['comarca_sg'] + "\n"
            + "- *Localización comarca*: " +  comarca['com_sgsa_n'] + "\n")

            #Csv
            filasAlertasCsv.append({"Nº": nAlerta ,"Fecha": start.strftime('%d-%m-%Y'), "Nivel de alerta": alerta["alertLevel"],"Comarca": comarca['com_sgsa_n'],"ID CG": alerta['comarca_sg'] ,"Nº brotes": len(alerta['brotes']),
            "Nº mov. Riesgo": alerta['movRiesgo'] ,"Grado alerta": round(alerta["valorRiesgo"], 4),"Temperatura estimada": alerta["temperatura"] ,"Supervivencia del virus en días": round(alerta['super'],4)})
            
            #Sacar informacion de brotes
            for brote in alerta['brotes']:
                rutas = alerta['brotes'][brote]
                for especie in rutas:
                    cursor = list(brotes_db.find({'oieid': brote}))
                    broteMongo = cursor[0]

                    brotes_set.add(brote)

                    if 'city' not in broteMongo:
                        broteMongo['city'] = "Not especified"

                    if nBrote % 100 == 0 and nBrote != 0:
                        filasBrotes += tablaBrotesAlertas

                    filasBrotes += ("| "  + str(nBrote)  + "| " + str(brote)
                    + "|" + broteMongo['report_date'].strftime('%d-%m-%Y')  + "|" + broteMongo['observation_date'].strftime('%d-%m-%Y') + "|" + broteMongo['country']  + "|" + broteMongo['city'] 
                    + "|" + str(broteMongo['lat']) + "|" + str(broteMongo['long']) + "|" +broteMongo['epiunit']  + "|" + str(broteMongo["species"]).replace(",", " ")  + "|" + str(broteMongo['cases'])
                    + "|" + str(broteMongo['deaths'])  + "|" +especie['cientifico']  + "|" + str(especie["codigoE"]) + "|" + str(round(especie["probEspecie"],4)) + "|\n" )
                    
                    filasBrotesCsv.append({
                        "ID": nBrote,"Nº Alerta": nAlerta,"Comarca": comarca['com_sgsa_n'],"ID CG": alerta['comarca_sg'], 
                        "Grado alerta": round(alerta["valorRiesgo"], 4), "Fecha alerta": start.strftime('%d-%m-%Y'),
                        "Event ID": brote, "Temperatura estimada": alerta["temperatura"],
                        "Supervivencia del virus en días": round(alerta['super'],4),
                        "Reporting date": broteMongo['observation_date'].strftime('%d-%m-%Y'),
                        "Observational date": broteMongo['observation_date'].strftime('%d-%m-%Y'), 
                        "Country": broteMongo['country'], "Location": broteMongo['city'], 
                        "Latitud": broteMongo['lat'], "Longitud": broteMongo['long'],
                        "Ponderacion brote": especie['probType'], "Riesgo brote":  round(especie['riesgoBrote'] * alerta['super'], 4),
                        "An. Type": broteMongo['epiunit'],"Species": str(broteMongo["species"]).replace(",", " "), "Cases": broteMongo['cases'], 
                        "Deaths": broteMongo['deaths'],"Especie movimiento": especie['cientifico'], 
                        "Cód.  Especie": especie["codigoE"], "Prob mov semanal":round(especie["probEspecie"],4)
                    })
                    nBrote+=1
            

            #Creamos tabla de brotes de la alerta i
            todosBrotes += encabezadoTablasBrotesAlertas + tablaBrotesAlertas + filasBrotes
            nAlerta += 1
            allNBrotes += nBrote
            filasBrotes = ""
            nBrote=0

        sumario = ("\n## Sumario del informe \n" +  " - *Número de comarcas ganaderas en alerta*: " + str(len(parameters['alertas']))
        + "\n - *Número de brotes en Europa asociados con movimientos de riesgo a España*: {}".format(len(brotes_set)))
        
        #Volcar fichero
        if len(parameters['alertas']) > 0:
            textoFinal = cabecera + sumario + cabeceraTablaAlertas + filasAlertas + cabeceraGenericaTablaBrotesAlertas + todosBrotes
        else:
            textoFinal = cabecera + sumario
        #Almacenar en un zip todos los ficheros de un año
        #Se guardaran en una carpeta
        

        #Comprobamos que el zip no esté guardado y que sea Julio para poder comprimir
        if not os.path.isfile("markdown/zips/julio{}_julio{}.zip".format(start.year-1,start.year)) and start.month == 7:
            self.compress(start.year)

        #Eliminamos la primera semana de todo el recorrido
        self.update_drive(start)
        #Creamos csv brotes y subimos al drive
        self.load_csv(csvCabeceraAlertas, csvCabeceraBrotes, filasAlertasCsv, filasBrotesCsv, start.year, start.month)

        # #Actualizacion
        informePath = "markdown/InformeSemanal_" + start.strftime("%d-%m-%Y") + ".md"
        f = open (informePath,'w+', encoding="utf-8")
        f.write(textoFinal)
        f.close()

        informePdfPath = self.reportPDF(informePath)
        informePdfName = informePdfPath.split("/")[-1]
        self.file_to_drive(informePdfPath, informePdfName, "alertas")

        return textoFinal

