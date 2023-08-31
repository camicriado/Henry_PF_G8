from datetime import datetime
from google.cloud.storage import Blob
from google.cloud import storage
import numpy as np
import string as string
import pandas as pd
import os
import gcsfs
from io import BytesIO
from deep_translator import GoogleTranslator
from googletrans import Translator
import re
from google.cloud import bigquery
import db_dtypes
from textblob import TextBlob

project_name = 'proyectogrupal-393822'
datawarehouse = 'datawarehouse'

def etl(event, context):
    
    file = event
    source_file_name=file['name']   #nombre del archivo nuevo en el bucket
    
    print(event)
    source_bucket_name = file['bucket'] #nombre del bucket donde esta el archivo
    print(f"Se detectó que se subió el archivo {source_file_name} al bucket {source_bucket_name}.")
    client = storage.Client(project="ProyectoGrupal")   #declara proyecto

    destination_bucket_name = 'pg_processed_data'  # bucket de destino
    destination_file_name=file['name']  # nombre del archivo destino
   
    #Para el etl

    # Obtiene los buckets
    source_bucket = client.get_bucket(source_bucket_name)
    destination_bucket = client.get_bucket(destination_bucket_name)

    # Obtiene el objeto del archivo a extraer
    source_blob = source_bucket.get_blob(source_file_name)

    # Crea un objeto blob para el archivo de destino
    destination_blob = destination_bucket.blob(destination_file_name)
    blop=source_bucket.blob(source_file_name)
    data=blop.download_as_string()
    # df=pd.read_csv(BytesIO(data), encoding="utf-8", delimiter=';')

    # Filtrar las filas donde la columna "categories" contiene las palabras "hotel", "nightlife" o "bars"
    def categorizar (item):
        item = item.lower()
        if 'hotel' in item:
            return 'Hotel'
        elif 'nightlife' in item:
            return 'Local Nocturno'
        elif ('bars' in item or 'bar' in item) and 'barb' not in item:
            return 'Bar'
        else:
            return 'Sin Dato'

    # Funcion que determina el score por analisis de sentimiento
    def analisis_sentimiento(texto):
        analisis = TextBlob(texto)
        return analisis.sentiment.polarity

    # se evalua el nombre del archivo
    if "google/metadata_sitios" in source_file_name:
        #cargamos dataset
        sitios= pd.read_json(BytesIO(data), lines = True)

        # Limpiar los valores faltantes en la columna "categories"
        sitios.rename(columns = {'category':'categories'}, inplace = True)
        sitios['categories'] = sitios['categories'].astype(str)
        sitios['categories'] = sitios['categories'].fillna('')

        #Borramos la columnas que no usamos
        sitios.drop(columns=['description','price','hours','MISC','state','relative_results', 'url'],inplace=True)

        # Categorizando
        sitios['categoria'] = sitios['categories'].apply(categorizar)

        # Elimando categoria "Sin Dato"
        sitios.drop(sitios[(sitios['categoria'] == 'Sin Dato')].index, inplace=True)

        # Elimando duplicados
        sitios.drop_duplicates(inplace=True)

        #Guardemos el dataset filtrado
        source_file_name = source_file_name.replace('.json', '.parquet')
        sitios.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)
        
    elif "google/reviews_estados" in source_file_name:
        #cargamos dataset
        review = pd.read_json(BytesIO(data), lines = True)

        # Damos formato a la columna 'time'
        review['date']=pd.to_datetime(review['time'], unit='ms')

        # filtrando año mayor que 1990
        review = review[review['date'].dt.year > 1990]
        review['date']=review['date'].dt.date        

        # borramos columnas que no usaremos
        review.drop(columns=['pics', 'resp', 'time'],inplace=True)

        # adicionamos una columna con el nombre del estado
        if 'California' in source_file_name:
            estado = 'CA'
        elif 'Nueva_York' in source_file_name:
            estado = 'NY'
        elif 'Florida' in source_file_name:
            estado = 'FL'
        elif 'Nevada' in source_file_name:
            estado = 'NV'
        elif 'Illinois' in source_file_name:
            estado = 'IL'

        review['estado'] = estado

        #cargamos dataset de review bigquery
        client = bigquery.Client(project=project_name)
        sql_query = ('''SELECT DISTINCT gmap_id
                FROM proyectogrupal-393822.datawarehouse.dim_sitios_google
                ''')
        sitios = client.query(sql_query).to_dataframe()
        sitios_id = sitios['gmap_id']

        # Filtrar el DataFrame review para que solo contenga los sitios_id filtrados
        review_filtrado = review[review['gmap_id'].isin(sitios_id)]

        print('review >>>', review.shape)
        print('review_filtrado >>>', review_filtrado.shape)

        # Limpiar los valores faltantes en la columna "text"
        review_filtrado['text'] = review_filtrado['text'].astype(str)
        review_filtrado['text'] = review_filtrado['text'].fillna('')

        #Se aplica la funcion al texto de las reseñas, con un valor de -1 a 1
            # 1 = sentimiento positivo
            # 0 = sentimiento neutro
            # -1 = Sentimiento negativo
        review_filtrado['sentimiento_score'] = review_filtrado['text'].apply(analisis_sentimiento)

        #Guardemos el dataset filtrado
        source_file_name = source_file_name.replace('.json', '.parquet')
        review_filtrado.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

    elif "yelp_academic_dataset_business" in source_file_name:
        #cargamos dataset de businees
        business= pd.read_json(BytesIO(data), lines = True)
        
        # Limpiar los valores faltantes en la columna "categories"
        business['categories'] = business['categories'].astype(str)
        business['categories'] = business['categories'].fillna('')

        # Eliminando columnas
        columnas_a_eliminar = ['is_open', 'hours', 'attributes']
        business.drop(columns=columnas_a_eliminar, inplace=True)

        # Categorizando
        business['categoria'] = business['categories'].apply(categorizar)

        #filtraremos el DF, primero cetiamos los estados a utilziar 
        estados_validos = ['CA', 'NY', 'FL', 'NV', 'IL']

        # Filtrar los registros con las claves de estados válidos
        business_filtrado = business[business['state'].isin(estados_validos)]

        # Elimando categoria "Sin Dato"
        business_filtrado.drop(business_filtrado[(business_filtrado['categoria'] == 'Sin Dato')].index, inplace=True)

        #Guardemos el dataset de business filtrado
        business_filtrado.to_parquet(r'gs://' +destination_bucket_name + '/yelp/business.parquet')

    elif "yelp/review" in source_file_name:
        #cargamos dataset
        review = pd.read_parquet(BytesIO(data))
        print('Review >>>')
        print(review.head(1))

        #cargamos dataset de business
        business= pd.read_parquet('gs://pg_processed_data/yelp/business.parquet')
        print('Business >>>')
        print(business.head(1))
        
        # Filtrar los valores únicos de business_id del DataFrame business_filtrado_completo
        business_ids_filtrados = business['business_id'].unique()

        # Filtrar el DataFrame review0 para que solo contenga los business_id filtrados
        review_filtrado = review[review['business_id'].isin(business_ids_filtrados)]

        # Eliminado columna no utilizada
        columnas = ['review_id']
        review_filtrado.drop(columns=columnas, inplace=True)

        # Cambiar tipo de dato de date
        review_filtrado['date'] = pd.to_datetime(review_filtrado['date']).dt.date

        # Limpiar los valores faltantes en la columna "text"
        review_filtrado['text'] = review_filtrado['text'].astype(str)
        review_filtrado['text'] = review_filtrado['text'].fillna('')

        #Se aplica la funcion al texto de las reseñas, con un valor de -1 a 1
            # 1 = sentimiento positivo
            # 0 = sentimiento neutro
            # -1 = Sentimiento negativo
        review_filtrado['sentimiento_score'] = review_filtrado['text'].apply(analisis_sentimiento)

        #Guardemos el dataset de filtrado
        review_filtrado.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

    elif "yelp/user" in source_file_name:
        #cargamos dataset
        user = pd.read_parquet(BytesIO(data))
        print('user >>>')
        print(user.shape)

        #cargamos dataset de review bigquery
        client = bigquery.Client(project=project_name)
        sql_query = ('''SELECT DISTINCT user_id
                FROM proyectogrupal-393822.datawarehouse.review
                ''')
        review_user_id = client.query(sql_query).to_dataframe()
        review_user_id = review_user_id['user_id']
        
        print('review_user_id >>>')
        print(review_user_id.shape)
        
        # Filtrar el DataFrame user para que solo contenga los user_id filtrados
        user_filtrado = user[user['user_id'].isin(review_user_id)]
        print('user_filtrado >>>')
        print(user_filtrado.shape)

        #establecemos una variable con las columnas a borrar
        columnas_drop= ['yelping_since','elite', 'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list', 'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny', 'compliment_writer','compliment_photos', 'friends', 'fans']
        #borramos columnas.
        user_filtrado.drop(columns=columnas_drop, inplace = True)
                
        #Guardemos el dataset defiltrado
        user_filtrado.to_parquet(r'gs://' + destination_bucket_name + '/' + source_file_name)

    elif "yelp_academic_dataset_tip" in source_file_name:
        #cargamos dataset
        tips= pd.read_json(BytesIO(data), lines = True)
        print('tips >>>')
        print(tips.head(3))

        #cargamos dataset de business
        business= pd.read_parquet('gs://pg_processed_data/yelp/business.parquet')
        print('Business >>>')
        print(business.head(3))

        # Filtrar los valores únicos de user_id del DataFrame
        business_ids_filtrados = business['business_id'].unique()

        # Filtrar el DataFrame tips para que solo contenga los business_id filtrados
        tips_filtrado1 = tips[tips['business_id'].isin(business_ids_filtrados)]

        # Filtrar los valores únicos de user_id del BigQuery user
        client = bigquery.Client(project=project_name)
        sql_query = ('''SELECT DISTINCT user_id
                FROM proyectogrupal-393822.datawarehouse.dim_user
                ''')
        user_ids_filtrados = client.query(sql_query).to_dataframe()
        user_ids_filtrados = user_ids_filtrados['user_id']
       
        print('user_ids_filtrados >>>')
        print(user_ids_filtrados.head(3))

        # Filtrar el DataFrame tips para que solo contenga los user_id filtrados
        tips_filtrado = tips_filtrado1[tips_filtrado1['user_id'].isin(user_ids_filtrados)]

        #eliinamos la columna 'compliment_count'
        tips_filtrado.drop(columns= 'compliment_count', inplace = True)

        # Cambiar tipo de dato de date
        tips_filtrado['date'] = pd.to_datetime(tips_filtrado['date']).dt.date

        # Limpiar los valores faltantes en la columna "text"
        tips_filtrado['text'] = tips_filtrado['text'].astype(str)
        tips_filtrado['text'] = tips_filtrado['text'].fillna('')

        #Se aplica la funcion al texto de las reseñas, con un valor de -1 a 1
            # 1 = sentimiento positivo
            # 0 = sentimiento neutro
            # -1 = Sentimiento negativo
        tips_filtrado['sentimiento_score'] = tips_filtrado['text'].apply(analisis_sentimiento)

        #Guardemos el dataset defiltrado
        tips_filtrado.to_parquet(r'gs://' +destination_bucket_name + '/yelp/tip.parquet')

    elif "yelp_academic_dataset_checkin" in source_file_name:
        #cargamos dataset
        checkin= pd.read_json(BytesIO(data), lines = True)
        print('checkin >>>')
        print(checkin.head(3))

        #cargamos dataset de business
        business= pd.read_parquet('gs://pg_processed_data/yelp/business.parquet')
        print('Business >>>')
        print(business.head(3))

        # Filtrar los valores únicos de business_id del DataFrame business_filtrado_completo
        business_ids_filtrados = business['business_id'].unique()

        # Filtrar el DataFrame para que solo contenga los business_id filtrados
        checkin_filtrado = checkin[checkin['business_id'].isin(business_ids_filtrados)]

        # A partir de la columna date, que como se menciono anteriormente contiene fechas de las visitas en la pagina web de cada negocio,
        # se genera una nueva columna que contiene la cantidad de visitas. 
        checkin_filtrado['num_visitas'] = checkin_filtrado['date'].apply(lambda x: len(x.split(',')))

        # Eliminando columana no utilizada
        checkin_filtrado.drop(columns='date', inplace=True)

        #Guardemos el dataset filtrado
        checkin_filtrado.to_parquet(r'gs://' +destination_bucket_name + '/yelp/checkin.parquet')

    return print(f"Archivo {source_file_name} del bucket {source_bucket_name} procesado con éxito.")
