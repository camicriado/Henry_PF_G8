##  Producción en servidor local - preparación del entorno

Creamos un entorno con python 3.11, e instalamos las dependencias necesarias.

# Crear entorno de trabajo
    $   python -m venv streamlib_env
    $   .\streamlib_env\Scripts\activate -- activar enviromen
    $   pip3 freeze > requirements.txt
    $   pip3 install -r .\requirements.txt --install library
    $   deactivate -- salir

    $   streamlit run app.py
    
##  Producción en servidor remoto

    *   Activar una cuenta en google cloud
    *   Crear proyecto en google cloud
    *   Instalar GoogleCloudSDK
        (https://cloud.google.com/sdk/docs/install)
    *   Ejecutar en la terminal:
    
    $ gcloud init
    $ gcloud app deploy app.yaml --project proyectogrupal-393822 
    