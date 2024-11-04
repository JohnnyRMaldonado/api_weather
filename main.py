import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import secretmanager

def access_secret_version(project_id, secret_id, version_id="latest"):
    # Crear el cliente de Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Configurar el nombre completo del secreto y la versión (por defecto 'latest')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Acceder a la versión del secreto
    response = client.access_secret_version(request={"name": name})

    # El valor del secreto está en 'payload.data'
    secret_value = response.payload.data.decode("UTF-8")

    return secret_value

# Configura tus variables
project_id = "proyect-pma"
api_key = "weather_api_key"

# Obtener la clave S
api_key = access_secret_version(project_id, api_key)

def get_coordinates(city_name):
    """Obtener las coordenadas (latitud y longitud) de una ciudad."""
    url = f'http://api.openweathermap.org/geo/1.0/direct'
    params = {
        'q': city_name,
        'appid': api_key,
        'limit': 1 
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Verifica errores HTTP
        data = response.json()
        
        if data:
            return data[0]['lat'], data[0]['lon']
        else:
            print("No se encontraron resultados para la ciudad especificada.")
            return None, None
            
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None, None

def fetch_weather_data(city_name):
    """Obtener datos de clima según el nombre de la ciudad."""
    lat, lon = get_coordinates(city_name)
    
    if lat is None or lon is None:
        print("No se pudieron obtener las coordenadas.")
        return None
    
    url = f'https://api.openweathermap.org/data/2.5/weather'
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'metric'
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        weather_data = {
            'city': data.get('name', 'N/A'),
            'temperature': data.get('main', {}).get('temp', 'N/A'),
            'weather': data.get('weather', [{}])[0].get('description', 'N/A') if data.get('weather') else 'N/A',
            'humidity': data.get('main', {}).get('humidity', 'N/A'),
            'wind_speed': data.get('wind', {}).get('speed', 'N/A')
        }

        return weather_data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def run_pipeline():
    city_name = "barranquilla"
    weather_info = fetch_weather_data(city_name)

    if weather_info:
        df = pd.DataFrame([weather_info])
        print(df)
    else:
        print("No se pudieron obtener los datos de clima.")

#configuración de Bquery
    project_id = "proyect-pma"
    dataset_id = "Johnny_ETLs"
    table_id = "weather"

    # Configuración del trabajo de carga
    cliente = bigquery.Client(project=project_id)
    table_ref = cliente.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True


    # Cargar el DataFrame a la tabla en BigQuery
    job = cliente.load_table_from_dataframe(df, table_ref, job_config=job_config)
        
    # job.result() # Esperar a que el trabajo se complete
    job.result()

    # Imprimir confirmación de éxito
    print(f"Datos cargados exitosamente en la tabla {table_id}")


run_pipeline()


            
