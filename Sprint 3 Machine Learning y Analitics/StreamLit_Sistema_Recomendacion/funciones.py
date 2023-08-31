import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from textblob import TextBlob

import re
from unidecode import unidecode
import nltk
nltk.download('stopwords') #se descarga un conjunto de palabrasque se consideran comunes y que generalmente no aportan mucho significado
from nltk.corpus import stopwords #se limitan las palabras al ingles 
stopwords = nltk.corpus.stopwords.words('english')
# importa el módulo RegexpTokenizer de la biblioteca NLTK
from nltk.tokenize import RegexpTokenizer
regexp = RegexpTokenizer('\w+')# Creamos un tokenizador de expresiones regulares para dividir el texto en palabras
from nltk.probability import FreqDist

# FUNCION DE RECOMENDACION E INFORMACION DE YELP
def Informacion_yelp (name: str, business, review, checkin):

# Verificar si el negocio existe en la base de datos
    if name.strip().lower() not in business['name'].str.strip().str.lower().values:
        return f"El negocio '{name}' no está en la base de datos"

#punto 1
    #indicamos la cantidad de estrellas en promedio que tiene le negocio insertado en name
    Estrellas = business[business['name'] == name]['stars'].values[0]

#punto 2
    # identificamos el id del negocio ingresado
    Id_bussines = business[business['name'] == name]['business_id'].values[0]
    # utilizamos el id de negocio para encontrar las reseñas en el dataset de review
    reseñas = review.loc[review['business_id'] == Id_bussines ]
    #creamos una función rax para eliminar valores especiales en el texto
    def limpiar_texto(texto):
        texto_sin_saltos = texto.replace("\n", " ")
        texto_minusculas = texto_sin_saltos.lower()
        texto_sin_tildes = unidecode(texto_minusculas)
        patron = r'@[\w]+|#\w+|[!,".]|(\b[^\w\s]\b)|\bhttps?\S+\b'
        texto_limpio = re.sub(patron, "", texto_sin_tildes)
        return texto_limpio
    #aplicamos función al texto de las reseñas
    reseñas['texto_limpio']= reseñas['text'].apply(limpiar_texto)
    # tokenizamos el texto
    reseñas['texto_token']= reseñas['texto_limpio'].apply(regexp.tokenize)
    # eliminamos los stopwords o palabras comunes
    reseñas['texto_token_stopwords']= reseñas['texto_token'].apply(lambda texto: [palabra for palabra in texto if palabra not in stopwords])
    # planteamos que las reseñas positivas son las mayores a 4 estrellas
    reseñas_positivas = reseñas.loc[reseñas['stars'] >= 4 ]
    # Unificar todas las reseñas de tokens en una sola
    union_reseñas_positivas= [token for reseña in reseñas_positivas['texto_token_stopwords'] for token in reseña]
    # Cálculo de la frecuencia de palabras
    frecuencia = FreqDist(union_reseñas_positivas)
    # Obtención de las palabras más comunes (atributos elogiados)
    atributos_comunes = frecuencia.most_common(10) # Cambia el número para obtener más o menos palabras
    # en este punto se creó una lista de tuplas con las 10 palabras más frecuentes y la cantidad de veces que aparece
    # por eso se elegirá solo el primer valor de las tuplas, es decir mostrado solo las palabras, no su frecuencia de repetición
    palabras_positivas = [tupla[0] for tupla in atributos_comunes]
    palabras_positivas = ' '.join(palabras_positivas) #se unen las palabras, para mostrar en una sola línea
    
# punto 3
    # planteamos que las reseñas negativas son las menores a 3 estrellas
    reseñas_negativas = reseñas.loc[reseñas['stars'] <= 3 ]
    # Unificar todas las listas de tokens en una sola
    union_reseñas_negativas= [token for reseña in reseñas_negativas['texto_token_stopwords'] for token in reseña]
    # Cálculo de la frecuencia de palabras
    frecuencia = FreqDist(union_reseñas_negativas)
    # Obtención de las palabras más comunes (atributos elogiados)
    atributos_comunes = frecuencia.most_common(10) # Cambia el número para obtener más o menos palabras
    # en este punto se creó una lista de tuplas con las 10 palabras más frecuentes y la cantidad de veces que aparece
    # por eso se elegirá solo el primer valor de las tuplas, es decir mostrado solo las palabras, no su frecuencia de repetición
    palabras_negativas = [tupla[0] for tupla in atributos_comunes]
    palabras_negativas =' '.join(palabras_negativas)#se unen las palabras, para mostrar en una sola línea

# punto 4
    # Convertir la columna 'date' a tipo datetime si aún no está en ese formato
    reseñas['date'] = pd.to_datetime(reseñas['date'])
    # Crear la columna 'año' que contiene el año extraído de la columna 'date'
    reseñas['año'] = reseñas['date'].dt.year
    # Convertir la columna 'date' a tipo datetime si aún no está en ese formato
    reseñas_positivas['date'] = pd.to_datetime(reseñas_positivas['date'])
    # Crear la columna 'año' que contiene el año extraído de la columna 'date'
    reseñas_positivas['año'] = reseñas_positivas['date'].dt.year
    #agrupamos las reseñas totales y las reseñas positivas por año y se guarda el resultado en cada variable
    resultado_reseñas= reseñas['año'].value_counts().sort_index()
    resultado_positivos = reseñas_positivas['año'].value_counts().sort_index()
    #se genera un df con los resultados obtenidos previamente.
    porporcion = pd.DataFrame({
        'Año': resultado_reseñas.index,
        'Numero Reseñas Totales': resultado_reseñas,
        'Numero Reseñas Positivas': resultado_positivos
    })
    # se genera una columna con la percepción positiva de cada año respecto al total de reseñas de ese año
    porporcion['Proporcion_anual']= (porporcion['Numero Reseñas Positivas']/porporcion['Numero Reseñas Totales'])*100
    #creamos una columna que contenga el porcentaje de crecimiento año a año de la percepcion positiva del negocio
    porporcion['Crecimiento_persepcion_positiva'] = (porporcion['Proporcion_anual'].diff() / porporcion['Proporcion_anual'].shift())*100
    # sacamos el promedio del crecimiento año a año para crear una aproximado de crecimeinto anual
    Crecimiento_percepcion_positiva = round(porporcion['Crecimiento_persepcion_positiva'].mean(), 2)
    
    def percepcion (Proporcion_positiva):
        if Crecimiento_percepcion_positiva > 0:
            return f'este negocio tiene un crecimiento de {Crecimiento_percepcion_positiva}% en promedio cada año'
        elif Crecimiento_percepcion_positiva < 0:
            return f'este negocio tiene un decrecimiento de {Crecimiento_percepcion_positiva}% en promedio cada año'
        else:
            return f'este negocio tiene un crecimiento nulo de {Crecimiento_percepcion_positiva}% en promedio cada año'
    #se asigna el mensaje de respuesta
    mensaje_percepcion = percepcion(Crecimiento_percepcion_positiva)  

#punto 5
    #se calcula en porcentajes cual es el equivalente de reseñas positivas en relación a total de reseñas
    if len(reseñas) == 0:
        Proporcion_positiva = 0
    else:
        Proporcion_positiva =  (len(reseñas_positivas)/ len(reseñas) ) *100

#punto 6
    #se crea una función para analizar sentimientos juntos con e modulo TextBlob
    def analisis_sentimiento(texto):
        analisis = TextBlob(texto)
        return analisis.sentiment.polarity
    #Se aplica la función al texto de las reseñas, con un valor de -1 a 1
        # 1 = sentimiento positivo
        # 0 = sentimiento neutro
        # -1 = Sentimiento negativo
    reseñas['sentimiento'] = reseñas['text'].apply(analisis_sentimiento)
    # se saca el promedio de los sentimientos, para identificar cual es el sentimiento promedio de las reseñas
    sentimiento_predominante = reseñas['sentimiento'].mean()
    #se crea una función para analizar la polaridad de los sentimientos encontrados, convirtiendo el resultado de numero a texto
    def interpretar_sentimiento(polaridad):
        if polaridad > 0.5:
            return "Los sentimientos de las reseñas son positivos"
        elif polaridad < -0.5:
            return "Los sentimientos de las reseñas son negativos"
        else:
            return "Los sentimientos de las reseñas son neutros"
    #se asigna el mensaje de respuesta
    mensaje = interpretar_sentimiento(sentimiento_predominante)

#punto 7
    # Definir el mapeo de códigos de estados a nombres
    mapeo_estados = {
        'NV': 'Nevada',
        'FL': 'Florida',
        'CA': 'California',
        'IL': 'Illinois'
    }

    # Obtener información de la dirección
    estado_clave = business[business['name'] == name]['state'].values[0]
    ciudad = business[business['name'] == name]['city'].values[0]
    direccion = business[business['name'] == name]['address'].values[0]
    # Obtener el nombre del estado a partir del código
    nombre_estado = mapeo_estados.get(estado_clave)

#punto 9
    #unimos el df de business con el de checkin mediante la columna de 'bisiness_id'
    merge_business_checkin = business.merge(checkin, on="business_id")
    # Ordenar el DataFrame por la columna "review_count" de forma descendente
    merge_business_checkin = merge_business_checkin.sort_values(by="review_count", ascending=False)

    # Restablecer los índices del DataFrame
    merge_business_checkin = merge_business_checkin.reset_index(drop=True)

    # Agregar la columna "peso_review" utilizando la columna "review_count"
    merge_business_checkin["peso_review"] = merge_business_checkin["review_count"]

    # Agregar la columna "peso_visitas" utilizando la columna "num_visitas"
    merge_business_checkin['peso_visitas'] = merge_business_checkin['num_visitas']
   
    # Definir la función de recomendación para review
    def peso_review(name):
        # Calcular la media de los pesos paa review
        media = merge_business_checkin["peso_review"].mean()
        #obtenemos el negocio a analizar
        negocio = merge_business_checkin[merge_business_checkin["name"] == name]
        # verificamos que el negocio este en la base de datos
        if negocio.empty:
            return "No se encontró el negocio en la base de datos."
        #indicamos el peso del negocio y lo comparamos con la media
        peso = negocio["peso_review"].iloc[0]
        if peso < media:
            return "Su negocio tiene un peso por debajo de la media. Esto significa que tiene menos check-ins y reseñas que otros negocios similares. Le recomendamos que mejore su estrategia de marketing para atraer más clientes y aumentar su popularidad."
        else:
            return "Su negocio tiene un peso igual o mayor a la media. Esto significa que tiene más check-ins y reseñas que otros negocios similares. Le felicitamos por su buen desempeño y le animamos a seguir mejorando su estrategia de marketing para mantener su ventaja competitiva."
    
    peso_review = peso_review(name)

    # Definir la función de recomendación para date
    def peso_visitas(name):
        # Calcular la media de los pesos paa date
        media = merge_business_checkin["peso_visitas"].mean()
        #obtenemos el negocio a analizar
        negocio = merge_business_checkin[merge_business_checkin["name"] == name]
        # verificamos que el negocio este en la base de datos
        if negocio.empty:
            return "No se encontró el negocio en la base de datos."
        #indicamos el peso del negocio y lo comparamos con la media
        peso = negocio["peso_visitas"].iloc[0]
        if peso < media:
            return "Su negocio tiene un peso por debajo de la media. Esto significa que tiene menos check-ins y reseñas que otros negocios similares. Le recomendamos que mejore su estrategia de marketing para atraer más clientes y aumentar su popularidad."
        else:
            return "Su negocio tiene un peso igual o mayor a la media. Esto significa que tiene más check-ins y reseñas que otros negocios similares. Le felicitamos por su buen desempeño y le animamos a seguir mejorando su estrategia de marketing para mantener su ventaja competitiva."

    peso_visitas = peso_visitas(name)

#punto 10
    #creamos una funcion para encontrar negocios simialres
    def recomendacion(name: str):

        # Reemplazar los valores NaN por un espacio en blanco
        business['categories'].fillna(' ', inplace=True)

        # Obtener el índice del negocio objetivo
        indice = business[business['name'].str.strip().str.strip().str.lower() == name.strip().strip().lower()].index[0]

        # Obtener la matriz TF-IDF de las caracteristicas de cada negocio
        tfidf_vectorizacion = TfidfVectorizer() #se crea una isntancia de la calse TfidfVectorizer
        tfidf_matrix = tfidf_vectorizacion.fit_transform(business['categories'].values) #se combierten las palabras en numeros

        # Calcular las similitudes entre todos los negocios a aprtir del negocio inicial
        Similitudes = cosine_similarity(tfidf_matrix[indice], tfidf_matrix)

        # Obtener los negocios mas similares en relacion al negocio objetivo
        similar_negocios_indices = np.argsort(Similitudes[0])[::-1][1:6]  # Obtener los 5 negocios más similares
        similar_negocios = business['name'].iloc[similar_negocios_indices].values.tolist()

        return similar_negocios
    
    negocios_similares = recomendacion(name)

    return name, Estrellas, palabras_positivas, palabras_negativas, \
        mensaje_percepcion, Proporcion_positiva, mensaje, nombre_estado, \
            ciudad, direccion, peso_review, peso_visitas, name, negocios_similares
   
# FUNCION DE RECOMENDACION E INFORMACION DE YELP
def Informacion_google (name: str, sitios, datos_google):

#punto 1
    #indicamos la cantidad de estrellas en promedio que tiene le negocio incertado en name
    Estrellas = datos_google[datos_google['name_x'] == name]['avg_rating'].values[0]

#punto 2
    # identificamos el id del negocio ingresado
    gmap_id = datos_google[datos_google['name_x'] == name]['gmap_id'].values[0]
    # utilizamos el id de negocio para encontrar las reseñas en el dtaset
    reseñas = datos_google.loc[datos_google['gmap_id'] == gmap_id ]
    #creamos una función rax para eliminar valores especiales en el texto
    def limpiar_texto(texto):
        texto_sin_saltos = texto.replace("\n", " ")
        texto_minusculas = texto_sin_saltos.lower()
        texto_sin_tildes = unidecode(texto_minusculas)
        patron = r'@[\w]+|#\w+|[!,".]|(\b[^\w\s]\b)|\bhttps?\S+\b'
        texto_limpio = re.sub(patron, "", texto_sin_tildes)
        return texto_limpio
    #aplicamos función al texto de las reseñas
    reseñas['texto_limpio']= reseñas['text'].apply(limpiar_texto)
    # tokenizamos el texto
    reseñas['texto_token']= reseñas['texto_limpio'].apply(regexp.tokenize)
    # eliminamos los stopwords o palabras comunes
    reseñas['texto_token_stopwords']= reseñas['texto_token'].apply(lambda texto: [palabra for palabra in texto if palabra not in stopwords])
    # planteamos que las reseñas positivas son las mayores a 4 estrellas
    reseñas_positivas = reseñas.loc[reseñas['rating'] >= 4 ]
    # Unificar todas las reseñas de tokens en una sola
    union_reseñas_positivas= [token for reseña in reseñas_positivas['texto_token_stopwords'] for token in reseña]
    # Cálculo de la frecuencia de palabras
    frecuencia = FreqDist(union_reseñas_positivas)
    # Obtención de las palabras más comunes (atributos elogiados)
    atributos_comunes = frecuencia.most_common(10) # Cambia el número para obtener más o menos palabras
    # en este punto se creó una lista de tuplas con las 10 palabras más frecuentes y la cantidad de veces que aparece
    # por eso se elegirá solo el primer valor de las tuplas, es decir mostrado solo las palabras, no su frecuencia de repetición
    palabras_positivas = [tupla[0] for tupla in atributos_comunes]
    palabras_positivas = ' '.join(palabras_positivas) #se unen las palabras, para mostrar en una sola línea

# punto 3
    # planteamos que las reseñas negativas son las menores a 3 estrellas
    reseñas_negativas = reseñas.loc[reseñas['rating'] <= 3 ]
    # Unificar todas las listas de tokens en una sola
    union_reseñas_negativas= [token for reseña in reseñas_negativas['texto_token_stopwords'] for token in reseña]
    # Cálculo de la frecuencia de palabras
    frecuencia = FreqDist(union_reseñas_negativas)
    # Obtención de las palabras más comunes (atributos elogiados)
    atributos_comunes = frecuencia.most_common(10) # Cambia el número para obtener más o menos palabras
    # en este punto se creó una lista de tuplas con las 10 palabras más frecuentes y la cantidad de veces que aparece
    # por eso se elegirá solo el primer valor de las tuplas, es decir mostrado solo las palabras, no su frecuencia de repetición
    palabras_negativas = [tupla[0] for tupla in atributos_comunes]
    palabras_negativas =' '.join(palabras_negativas)#se unen las palabras, para mostrar en una sola línea

# punto 4
    # Convertir la columna 'date' a tipo datetime si aún no está en ese formato
    reseñas['date'] = pd.to_datetime(reseñas['date'])
    # Crear la columna 'año' que contiene el año extraído de la columna 'date'
    reseñas['año'] = reseñas['date'].dt.year
    # Convertir la columna 'date' a tipo datetime si aún no está en ese formato
    reseñas_positivas['date'] = pd.to_datetime(reseñas_positivas['date'])
    # Crear la columna 'año' que contiene el año extraído de la columna 'date'
    reseñas_positivas['año'] = reseñas_positivas['date'].dt.year
    #agrupamos las reseñas totales y las reseñas positivas por año y se guarda el resultado en cada variable
    resultado_reseñas= reseñas['año'].value_counts().sort_index()
    resultado_positivos = reseñas_positivas['año'].value_counts().sort_index()
    #se genera un df con los resultados obtenidos previamente.
    porporcion = pd.DataFrame({
        'Año': resultado_reseñas.index,
        'Numero Reseñas Totales': resultado_reseñas,
        'Numero Reseñas Positivas': resultado_positivos
    })
    # se genera una columna con la percepción positiva de cada año respecto al total de reseñas de ese año
    porporcion['Proporcion_anual']= (porporcion['Numero Reseñas Positivas']/porporcion['Numero Reseñas Totales'])*100
    #creamos una columna que contenga el porcentaje de crecimiento año a año de la percepcion positiva del negocio
    porporcion['Crecimiento_persepcion_positiva'] = (porporcion['Proporcion_anual'].diff() / porporcion['Proporcion_anual'].shift())*100
    # sacamos el promedio del crecimiento año a año para crear una aproximado de crecimeinto anual
    Crecimiento_percepcion_positiva = round(porporcion['Crecimiento_persepcion_positiva'].mean(), 2)

    def percepcion (Proporcion_positiva):
        if Crecimiento_percepcion_positiva > 0:
            return f'este negocio tiene un crecimiento de {Crecimiento_percepcion_positiva}% en promedio cada año'
        elif Crecimiento_percepcion_positiva < 0:
            return f'este negocio tiene un decrecimiento de {Crecimiento_percepcion_positiva}% en promedio cada año'
        else:
            return f'este negocio tiene un crecimiento nulo de {Crecimiento_percepcion_positiva}% en promedio cada año'
    #se asigna el mensaje de respuesta
    mensaje_percepcion = percepcion(Crecimiento_percepcion_positiva)  

#punto 5
    #se calcula en porcentajes cual es el equivalente de reseñas positivas en relación a total de reseñas
    if len(reseñas) == 0:
        Proporcion_positiva = 0
    else:
        Proporcion_positiva =  (len(reseñas_positivas)/ len(reseñas) ) *100
     
#punto 6
    #se crea una función para analizar sentimientos juntos con e modulo TextBlob
    def analisis_sentimiento(texto):
        analisis = TextBlob(texto)
        return analisis.sentiment.polarity
    #Se aplica la función al texto de las reseñas, con un valor de -1 a 1
        # 1 = sentimiento positivo
        # 0 = sentimiento neutro
        # -1 = Sentimiento negativo
    reseñas['sentimiento'] = reseñas['text'].apply(analisis_sentimiento)
    # se saca el promedio de los sentimientos, para identificar cual es el sentimiento promedio de las reseñas
    sentimiento_predominante = reseñas['sentimiento'].mean()
    #se crea una función para analizar la polaridad de los sentimientos encontrados, convirtiendo el resultado de numero a texto
    def interpretar_sentimiento(polaridad):
        if polaridad > 0.5:
            return "Los sentimientos de las reseñas son positivos"
        elif polaridad < -0.5:
            return "Los sentimientos de las reseñas son negativos"
        else:
            return "Los sentimientos de las reseñas son neutros"
    #se asigna el mensaje de respuesta
    mensaje = interpretar_sentimiento(sentimiento_predominante)

#punto 7
    # Definir el mapeo de códigos de estados a nombres
    mapeo_estados = {
        'NV': 'Nevada',
        'FL': 'Florida',
        'CA': 'California',
        'IL': 'Illinois'
    }

    # Obtener información de la dirección
    estado_clave = reseñas[reseñas['name_x'] == name]['estado'].values[0]
    direccion = reseñas[reseñas['name_x'] == name]['address'].values[0]

    # Obtener el nombre del estado a partir del código
    nombre_estado = mapeo_estados.get(estado_clave)

#punto 9
    # Agregar la columna "peso_review" utilizando la columna "review_count"
    datos_google["peso_review"] = datos_google["num_of_reviews"]

    # Definir la función de recomendación para review
    def peso_review(name):

        # Calcular la media de los pesos para review
        media = datos_google["peso_review"].mean()
        #obtenemos el negocio a analizar
        # Filtrar según la condición de 'name_x' igual a name
        negocio = datos_google['name_x'] == name
        peso_review_name = datos_google.loc[negocio, 'peso_review']

        suma_pesos = peso_review_name.sum()

        # verificamos que el negocio este en la base de datos
        if negocio.empty:
            return "No se encontró el negocio en la base de datos."
        #indicamos el peso del negocio y lo comparamos con la media
        if suma_pesos < media:
            return "Su negocio tiene un peso por debajo de la media. Esto significa que tiene menos check-ins y reseñas que otros negocios similares. Le recomendamos que mejore su estrategia de marketing para atraer más clientes y aumentar su popularidad."
        else:
            return "Su negocio tiene un peso igual o mayor a la media. Esto significa que tiene más check-ins y reseñas que otros negocios similares. Le felicitamos por su buen desempeño y le animamos a seguir mejorando su estrategia de marketing para mantener su ventaja competitiva."
        
    peso_review = peso_review("Starbucks")

#punto 10
    # Eliminar duplicados basados en la columna "name"
    sitio_sin_duplicados = sitios.drop_duplicates(subset='name')
    #creamos una funcion para encontrar negocios simialres
    def recomendacion(name: str):

        # Reemplazar los valores NaN por un espacio en blanco
        sitio_sin_duplicados['categories'].fillna(' ', inplace=True)

        # Obtener el índice del negocio objetivo
        indice = sitio_sin_duplicados[sitio_sin_duplicados['name'].str.strip().str.strip().str.lower() == name.strip().strip().lower()].index[0]

        # Obtener la matriz TF-IDF de las caracteristicas de cada negocio
        tfidf_vectorizacion = TfidfVectorizer() #se crea una isntancia de la calse TfidfVectorizer
        tfidf_matrix = tfidf_vectorizacion.fit_transform(sitio_sin_duplicados['categories'].values) #se combierten las palabras en numeros

        # Calcular las similitudes entre todos los negocios a aprtir del negocio inicial
        Similitudes = cosine_similarity(tfidf_matrix[indice], tfidf_matrix)
    
        # Obtener los negocios mas similares en relacion al negocio objetivo
        similar_negocios_indices = np.argsort(Similitudes[0])[::-1][1:6]  # Obtener los 5 negocios más similares
        similar_negocios = sitio_sin_duplicados['name'].iloc[similar_negocios_indices].values.tolist()

        return similar_negocios
    
    negocios_similares = recomendacion(name)

    return name, Estrellas, palabras_positivas, palabras_negativas, \
        mensaje_percepcion, Proporcion_positiva, mensaje, nombre_estado, \
            direccion, peso_review, name, negocios_similares
    