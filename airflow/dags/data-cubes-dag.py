from datetime import datetime
from urllib import request
import ssl

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from operators.care_providers import care_providers_data_cube
from operators.population import population_data_cube

URL_CARE_PROVIDERS = "https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"
URL_POPULATION = "https://www.czso.cz/documents/10180/184344914/130141-22data2021.csv"
URL_LAU_NUTS_MAP = "https://skoda.projekty.ms.mff.cuni.cz/ndbi046/seminars/02/%C4%8D%C3%ADseln%C3%ADk-okres%C5%AF-vazba-101-nad%C5%99%C3%ADzen%C3%BD.csv"

DATA_DIR = "./airflow/data/"
DATA_CUBES_DIR = "./airflow/data/data-cubes/"

CARE_PROVIDERS_CSV = "care-providers.csv"
POPULATION_CSV = "population.csv"
LAU_NUTS_MAP_CSV = "lau-nuts-map.csv"

# @task(task_id="download-data-care-providers-csv")
def download_data_care_providers():
    print(f"Downloading... {CARE_PROVIDERS_CSV}")
    request.urlretrieve(URL_CARE_PROVIDERS, DATA_DIR + CARE_PROVIDERS_CSV)
    print("DONE")

# @task(task_id="download-data-population-csv")
def download_data_population():
    print(f"Downloading... {POPULATION_CSV}")
    request.urlretrieve(URL_POPULATION, DATA_DIR + POPULATION_CSV)
    print("DONE")

# @task(task_id="download-data-lau-nuts-map-csv")
def download_data_lau_nuts_map():
    print(f"Downloading... {LAU_NUTS_MAP_CSV}")
    request.urlretrieve(URL_LAU_NUTS_MAP, DATA_DIR + LAU_NUTS_MAP_CSV)
    print("DONE")

# @task(task_id="create_care_providers_datacube")
def create_care_providers_datacube(**kwargs):
    output_dir = kwargs["dag_run"].conf.get("output_path", DATA_CUBES_DIR)
    care_providers_data_cube()

# @task(task_id="create_population_datacube")
def create_population_datacube(**kwargs):
    output_dir = kwargs["dag_run"].conf.get("output_path", DATA_CUBES_DIR)
    population_data_cube()

ssl._create_default_https_context = ssl._create_unverified_context

default_args = {
    'start_date': datetime(year=2023, month=3, day=17)
}

with DAG(
    dag_id='data_cubes',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Create data cubes'
) as dag:

    task_download_data_care_providers = PythonOperator(
        task_id='download_data_care_providers_csv',
        python_callable=download_data_care_providers
    )

    task_download_data_population = PythonOperator(
        task_id='download_data_population_csv',
        python_callable=download_data_population
    )

    task_download_data_lau_nuts_map = PythonOperator(
        task_id='download_data_lau_nuts_map_csv',
        python_callable=download_data_lau_nuts_map
    )

    task_create_care_providers_datacube = PythonOperator(
        task_id='create_care_providers_datacube',
        python_callable=create_care_providers_datacube
    )

    task_create_population_datacube = PythonOperator(
        task_id='create_population_datacube',
        python_callable=create_population_datacube
    )
    
    task_download_data_care_providers >> task_create_care_providers_datacube
    [task_download_data_population, task_download_data_lau_nuts_map] >> task_create_population_datacube