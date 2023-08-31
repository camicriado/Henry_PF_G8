import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

from google.cloud import bigquery
import os
import db_dtypes
from wordcloud import WordCloud
import matplotlib.pyplot as plt

import funciones as f

st.set_option('deprecation.showPyplotGlobalUse', False)


st.write('# SISTEMA DE RECOMENDACION')

st.markdown('''
A pedido del Cliente se realiza un análisis Exhaustivo de las reviews de las plataformas 
Google Bussiness y YELP! para obtener información relevante que ayude a los empresarios a mejorar sus resultados.            
''')

# variables de entorno para acceder a BigQuery
os.environ ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

@st.cache_data
def load_data():
    #cargamos dataset de bigquery
    client = bigquery.Client()
    sql_query = ('''
            SELECT *
            FROM proyectogrupal-393822.datawarehouse.dim_business
            ''')
    business = client.query(sql_query).to_dataframe()

    sql_query = ('''
            SELECT b.business_id, b.name, b.categoria, r.useful as rating, r.sentimiento_score, r.text, r.stars, r.date, b.city,
            b.latitude, b.longitude
            FROM proyectogrupal-393822.datawarehouse.review r
            INNER JOIN proyectogrupal-393822.datawarehouse.dim_business b
                ON r.business_id = b.business_id
            WHERE b.state = "IL"
            ''')
    review = client.query(sql_query).to_dataframe()

    sql_query = ('''
                SELECT * FROM `proyectogrupal-393822.datawarehouse.checkin`
            ''')
    checkin = client.query(sql_query).to_dataframe()

    sql_query = ('''
            SELECT * 
            FROM `proyectogrupal-393822.datawarehouse.dim_sitios_google`
            ''')
    sitios = client.query(sql_query).to_dataframe()

    sql_query = ('''
            SELECT * 
            FROM `proyectogrupal-393822.datawarehouse.review_google`
            WHERE estado = "IL"
            ''')
    review_google = client.query(sql_query).to_dataframe()

    sql_query = ('''
            SELECT DISTINCT s.name, s.latitude, s.longitude
            FROM `proyectogrupal-393822.datawarehouse.dim_sitios_google` s
            INNER JOIN `proyectogrupal-393822.datawarehouse.review_google` r
                ON s.gmap_id = r.gmap_id
            WHERE estado = "IL"
            LIMIT 1000
            ''')
    review_name_google = client.query(sql_query).to_dataframe()
    
    return business, review, checkin, sitios, review_google, review_name_google

business, review, checkin, sitios, review_google, review_name_google = load_data()

#unimos los dataset para unmejor manejo
datos_google = sitios.merge(review_google, on="gmap_id")

plataforma = ['Yelp', 'Google Business']
selected_plataforma = st.selectbox(label='Seleccione una Plataform', options=plataforma)

if selected_plataforma == 'Yelp':
    business_filtro = review['name'].unique()[2:1000]
else:
    business_filtro = review_name_google['name']
    
selected_business = st.selectbox(label='Seleccione un Negocio', options=business_filtro)
    
with st.form('form_recomendacion'):
    
    
    submitted = st.form_submit_button('Buscar')

    if submitted:
        if selected_business == None:
            st.write('Debe seleccionar algun negocio')
            exit()
            
    # return {'Informacion del negocio': name,
    # 'Estrellas promedio del negocio': Estrellas, 
    # 'palabras más comunes en las reseñas positivas':  palabras_positivas if palabras_positivas else "No se encontraron reseñas positivas", 
    # 'palabras más comunes en las reseñas negativas': palabras_negativas if palabras_negativas else "No se encontraron reseñas negativas",
    # 'crecimiento promedio de la percepción positiva por año':  mensaje_percepcion,
    # 'proporción de reseñas positivas': f'el {Proporcion_positiva}% de las reseñas son positivas',
    # 'sentimiento usual de las reseñas': mensaje,
    # 'Ubicación': f'este negocio está ubicado en el estado de {nombre_estado}, en  la ciudad {ciudad}, en la direccion { direccion}',
    # 'Peso o relevancia de Reseñas': peso_review,
    # 'peso o relevancia de visitas': peso_visitas,
    # 'Negocios similares': f"Los negocios similares a {name} son: {negocios_similares}"}

        if selected_plataforma == 'Yelp':
            name, Estrellas, palabras_positivas, palabras_negativas, \
                mensaje_percepcion, Proporcion_positiva, mensaje, nombre_estado, \
                    ciudad, direccion, peso_review, peso_visitas, name, negocios_similares = f.Informacion_yelp(selected_business, business, review, checkin)
        else:
            name, Estrellas, palabras_positivas, palabras_negativas, \
                mensaje_percepcion, Proporcion_positiva, mensaje, nombre_estado, \
                    direccion, peso_review, name, negocios_similares  = f.Informacion_google(selected_business, sitios, datos_google)

        # google se elimino peso_visitas y ciudad
        Proporcion_positiva = round(Proporcion_positiva,2)
        
        st.subheader("Información")
        st.write('- Estrellas promedio del negocio', Estrellas)
        st.write('- Crecimiento promedio de la percepción positiva por año:', mensaje_percepcion)
        st.write('- Proporción de reseñas positivas:', f'el {Proporcion_positiva}% de las reseñas son positivas')
        st.write('- Sentimiento usual de las reseñas:', mensaje)
        st.write('- Peso o relevancia de Reseñas:', peso_review)
        if selected_plataforma == 'Yelp':
            st.write('- Peso o relevancia de visitas:', peso_visitas)

        # Sistema de recomendacion
        st.subheader("Negocios similares")
        for i in negocios_similares:
                st.markdown("- " + i)
        
        # Word Cloud
        if len(palabras_positivas) > 0:
            text = palabras_positivas
            wordcloud = WordCloud(width=800, height=400).generate(text)
            #plt.figure(figsize=(10, 6))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.show()
            plt.title('Palabras positivas')
            st.pyplot()
        else:
            st.write('No tiene palabras positivas')

        # Word Cloud
        if len(palabras_negativas) > 0:
            text = palabras_negativas
            wordcloud = WordCloud(width=800, height=400).generate(text)
            #plt.figure(figsize=(10, 6))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.show()
            plt.title('Palabras Negativas')
            st.pyplot()
        else:
            st.write('No tiene palabras negativas')

        if selected_plataforma == 'Yelp':
            st.write('Ubicación:', f'este negocio está ubicado en el estado de {nombre_estado}, en  la ciudad {ciudad}, en la direccion { direccion}')
        else:
            st.write('Ubicación:', f'este negocio está ubicado en el estado de {nombre_estado}, en la direccion { direccion}')
        
        # Ubicacion en el mapa
        if selected_plataforma == 'Yelp':
            st.map(review[review['name'] == selected_business][['latitude', 'longitude']][0:1])
        else:
            st.map(review_name_google[review_name_google['name'] == selected_business][['latitude', 'longitude']][0:1])
     