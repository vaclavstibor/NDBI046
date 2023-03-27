from datetime import datetime
from urllib import request
import ssl

from airflow.decorators import dag, task

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

ssl._create_default_https_context = ssl._create_unverified_context

@dag(
    dag_id="data-cubes",
    schedule=None,
    start_date=datetime(2023, 3, 27),
)

def main():
    @task(task_id="download-data-care-providers-csv")
    def download_data_care_providers():
        print(f"Downloading... {CARE_PROVIDERS_CSV}")
        request.urlretrieve(URL_CARE_PROVIDERS, DATA_DIR + CARE_PROVIDERS_CSV)
        print("DONE")

    @task(task_id="download-data-population-csv")
    def download_data_population():
        print(f"Downloading... {POPULATION_CSV}")
        request.urlretrieve(URL_POPULATION, DATA_DIR + POPULATION_CSV)
        print("DONE")

    @task(task_id="download-data-lau-nuts-map-csv")
    def download_data_lau_nuts_map():
        print(f"Downloading... {LAU_NUTS_MAP_CSV}")
        request.urlretrieve(URL_LAU_NUTS_MAP, DATA_DIR + LAU_NUTS_MAP_CSV)
        print("DONE")

    @task(task_id="create_care_providers_datacube")
    def create_care_providers_datacube(**kwargs):
        output_dir = kwargs["dag_run"].conf.get("output_path", DATA_CUBES_DIR)
        care_providers_data_cube()

    @task(task_id="create_population_datacube")
    def create_population_datacube(**kwargs):
        output_dir = kwargs["dag_run"].conf.get("output_path", DATA_CUBES_DIR)
        population_data_cube()

    main >> download_data_care_providers() >> create_care_providers_datacube()
    main >> [download_data_population(), download_data_lau_nuts_map()] >> create_population_datacube()
    
if __name__ == "__main__":
    main()