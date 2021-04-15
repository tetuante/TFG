from datetime import datetime, timedelta, date
import pandas as pd
import os
import json
import copy

class Controller:
    def __init__(self, model,dataFactory, geojsonGen):
        self.model = model
        self.dataFactory = dataFactory
        self.geojsonGen = geojsonGen

    def changeProb(self, prob):
        self.model.changeProb(prob)

    #DateM -> fecha referente
    #weeks -> semanas para generar alertas
    #temporaryWindow -> ventana temporal para busqueda de brotes por defecto 3 Meses/12 semanas/ 90 dias
    def runOfflineTool(self, dateM, weeks, temporaryWindow):

        online= True 
        if dateM != None:#Fecha Herramienta offline
            start = dateM + timedelta(days = -dateM.weekday())
            end = start + timedelta(weeks = 1)
            online = False
        else: #Fecha actual
            today = date.today()
            start = today + timedelta(days = -today.weekday())
            end = start + timedelta(weeks = 1)

        start = datetime.combine(start, datetime.min.time())
        end = datetime.combine(end, datetime.min.time())

        #Parameters
        outbreakStart = start - timedelta(weeks = temporaryWindow)
        comarca_brotes, lista_brotes = self.dataFactory.createData("outbreak", outbreakStart, start,None)

        tMin = self.dataFactory.createData("temp",start, end, online)
        lista_comarcas = self.dataFactory.createData("comarcas", None, None, None)

        probEspecies = self.dataFactory.createData("migration_prob",start, end, None)
        # file = "data/Datos especies1.xlsx"
        # matrizEspecies = pd.read_excel(file, 'Prob_migracion', skiprows=3, usecols='A:AY', header=0, index_col=2)

        data= dict()
        data['comarca_brotes']= comarca_brotes
        data['tMin'] = tMin
        data['probEspecies'] = probEspecies
        data['online'] = online


        brotes_por_semana = dict()
        brotes_por_semana = lista_brotes.copy()
        migrations_por_semana = dict()
        migrations_por_semana[start] = comarca_brotes

        alertas_list = list()

        self.model.setData(data)
        
        if dateM == None:
            alertas = self.model.run(start,end)
            alertas_list.append(alertas)
        else:
            i = 0
            while (i < weeks):
                alertas = self.model.run(start,end)
                alertas_list.append(alertas)

                #Actualizar start / end
                start = end
                end = start + timedelta(weeks = 1)

                outbreakStart = start - timedelta(weeks = temporaryWindow)
                comarca_brotes, lista_brotes = self.dataFactory.createData("outbreak", outbreakStart, start,None)
                
                #Actualizamos solo los brotes
                data['comarca_brotes']= comarca_brotes

                migrations_por_semana[start] = comarca_brotes
                brotes_por_semana[start - timedelta(weeks=1)] = lista_brotes[start - timedelta(weeks=1)].copy()

                i += 1

        reportPDF = self.dataFactory.createData("report",start, end, alertas_list)
        geojson_alerta = self.geojsonGen.generate_comarca(alertas_list, lista_comarcas)
        geojson_outbreak = self.geojsonGen.generate_outbreak(brotes_por_semana)
        geojson_migration = self.geojsonGen.generate_migration(migrations_por_semana, lista_comarcas, brotes_por_semana)

        return geojson_alerta

    def runOnlineTool(self, user_parameters):

        #PARAMETERS
        parameters = dict()
        parameters = self.model.getParameters()

        if user_parameters != None:
            parameters = user_parameters
        else:
            parameters = self.model.getParameters()

        #PARAMETERS

        #Fecha actual
        start_date = date.today()
        if parameters["start_date"] != None:
            day = parameters["start_date"]["day"]
            month = parameters["start_date"]["month"]
            year = parameters["start_date"]["year"]
            start_date = date(year, month, day)

        start = start_date + timedelta(days = -start_date.weekday())
        end = start + timedelta(weeks = 1)
        #Convert to datetime
        start = datetime.combine(start, datetime.min.time())
        end = datetime.combine(end, datetime.min.time())


        #DATA SENT TO MODEL

        #Outbreaks
        # brotes_por_semana
        # 2020/01/01 => list(brotes en esta semana)
        # 2020/01/08 => list(brotes en esta semana)
        # ...
        # ..
        # .
        # comarca_brotes
        # *Codigo de una comarca* => list(info de brotes que llegan a esta comarca)
        # ...
        # ..
        # .
        outbreak_start = start - timedelta(weeks=parameters["temporaryWindow"])
        comarca_brotes, brotes_por_semana = self.dataFactory.createData("outbreak", outbreak_start, end , parameters)

        #Temperature
        tMin = self.dataFactory.createData("temp",start, end, True)

        #Comarca
        lista_comarcas = self.dataFactory.createData("comarcas", None, None, None)

        #Probabilidad Migracion
        file = "data/Datos especies1.xlsx"
        matrizEspecies = pd.read_excel(file, 'Prob_migracion', skiprows=3, usecols='A:AY', header=0, index_col=2)


        data= dict()
        data['comarca_brotes']= comarca_brotes
        data['tMin'] = tMin
        data['matrizEspecies'] = matrizEspecies

        #DATA SENT TO MODEL

        #Geojson generator
        migrations_por_semana = dict()
        migrations_por_semana[start] = comarca_brotes

        self.model.setData(data)
        self.model.setParameters(parameters)

        alertas = self.model.run(start,end)


        reportPDF = self.dataFactory.createData("report",start, end, alertas)
        geojson_alerta = self.geojsonGen.generate_comarca(alertas_list, lista_comarcas)
        geojson_outbreak = self.geojsonGen.generate_outbreak(brotes_por_semana)
        geojson_migration = self.geojsonGen.generate_migration(migrations_por_semana, lista_comarcas, brotes_por_semana)

        return 0


