#!/usr/bin/env python3
import pandas as pd

from operators.datacube import create_care_providers_data_cube
from rdflib import Namespace

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def care_providers_data_cube():

    file_path = "./airflow/data/care-providers.csv"
    data = load_csv_file_as_df(file_path)
    data_cube = create_care_providers_data_cube(data)

    data_cube.serialize(format="ttl", destination="./airflow/data/data-cubes/health_care.ttl")
    
    print("-" * 5, " data-cubes/health_care.ttl CREATED ", "-" * 5)

def load_csv_file_as_df(file_path: str):

    data_frame = pd.read_csv(file_path, low_memory=False)
    return data_frame