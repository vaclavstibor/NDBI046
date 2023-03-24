#!/usr/bin/env python3
import pandas as pd
import os

from datacube import create_care_providers_data_cube
from rdflib import Namespace

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def main():

    file_path = "./data/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"
    data = load_csv_file_as_df(file_path)
    data_cube = create_care_providers_data_cube(data)

    if not os.path.exists('./data/data-cubes'):
        os.makedirs('./data/data-cubes')

    data_cube.serialize(format="ttl", destination="./data/data-cubes/care-providers.ttl")
    
    print("-" * 5, " ./data/data-cubes/care-providers.ttl CREATED ", "-" * 5)

def load_csv_file_as_df(file_path: str):

    data_frame = pd.read_csv(file_path, low_memory=False)
    return data_frame

if __name__ == "__main__":
    main()