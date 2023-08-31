from google.cloud import bigquery

project_name = 'proyectogrupal-393822'
datawarehouse = 'datawarehouse'

schemas_id = {
                'review':[
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("business_id", "STRING"),
                bigquery.SchemaField("stars", "FLOAT64"),
                bigquery.SchemaField("useful", "INT64"),
                bigquery.SchemaField("funny", "INT64"),
                bigquery.SchemaField("cool", "INT64"),
                bigquery.SchemaField("text", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("sentimiento_score", "FLOAT64"),],
                'dim_user':[
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("review_count", "INT64"),
                bigquery.SchemaField("useful", "INT64"),
                bigquery.SchemaField("funny", "INT64"),
                bigquery.SchemaField("cool", "INT64"),
                bigquery.SchemaField("average_stars", "FLOAT64"),],
                'tip':[
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("business_id", "STRING"),
                bigquery.SchemaField("text", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("sentimiento_score", "FLOAT64"),],
                'checkin':[
                bigquery.SchemaField("business_id", "STRING"),
                bigquery.SchemaField("num_visitas", "INT64"),],
                'dim_business':[
                bigquery.SchemaField("business_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("postal_code", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("stars", "FLOAT64"),
                bigquery.SchemaField("review_count", "INT64"),
                bigquery.SchemaField("categories", "STRING"),
                bigquery.SchemaField("categoria", "STRING"),],
                'dim_sitios_google':[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField("gmap_id", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("categories", "STRING"),
                bigquery.SchemaField("avg_rating", "FLOAT64"),
                bigquery.SchemaField("num_of_reviews", "INT64"),
                bigquery.SchemaField("categoria", "STRING"),],
                'review_google':[
                bigquery.SchemaField("user_id", "FLOAT64"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("rating", "INT64"),
                bigquery.SchemaField("text", "STRING"),
                bigquery.SchemaField("gmap_id", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("estado", "STRING"),
                bigquery.SchemaField("sentimiento_score", "FLOAT64"),]
            }

def load_datawarehouse(event, context):
    file = event
    file_name=file['name']   #se saca el nombre del archivo nuevo en el bucket ETL
    table_name = file_name.split("/")[-1] # separando
    table_name = table_name.split(".")[0] # nombre del archivo sin el .parquet

    print(f"Se detectó que se subió el archivo {file_name} en el bucket {file['bucket']}.")
    source_bucket_name = file['bucket'] #nombre del bucket donde esta el archivo
    
    if "google/metadata_sitios" in file_name:
        table_name = 'dim_sitios_google'
    elif "google/reviews_estados" in file_name:
        table_name = 'review_google'
    elif "business" in file_name:
        table_name = 'dim_business'
    elif "yelp/review" in file_name:
        table_name = 'review'
    elif "yelp/user" in file_name:
        table_name = 'dim_user'
    elif "tip" in file_name:
        table_name = 'tip'
    elif "checkin" in file_name:
        table_name = 'checkin'

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    source_path = "gs://"+source_bucket_name+"/"+file_name # ruta al archivo cargado en el bucket de stage
    table_id = project_name + "." + datawarehouse + "." + table_name 
    # table_id - ruta a la tabla a carga en big query: "nombre_del_proyecto"."nombre_de_la_base_de_datos"."nombre de la tabla"
    # cambiar nombre del proyecto y nombre de la base de datos en bigquery arriba (al inicio)  
    print(f"Archivo source_bucket_name : {source_path}.")
    print(f"Archivo table_name : {table_name}.")
    print(f"Archivo table_id : {table_id}.")


    job_config = bigquery.LoadJobConfig(
        schema= schemas_id[table_name],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.PARQUET,
    )
    #poner ubicación de archivo customers
    uri = source_path

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
