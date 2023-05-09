#!/usr/bin/env python3

import pandas as pd
import os
import ssl

from urllib import request
from rdflib import Namespace
from datacube import create_mean_population_data_cube

URL_POPULATION = "https://www.czso.cz/documents/10180/184344914/130141-22data2021.csv"
URL_LAU_NUTS_MAP = "https://skoda.projekty.ms.mff.cuni.cz/ndbi046/seminars/02/%C4%8D%C3%ADseln%C3%ADk-okres%C5%AF-vazba-101-nad%C5%99%C3%ADzen%C3%BD.csv"

DATA_DIR = "./certificate-sign/data/"
DATA_CUBES_DIR = "./certificate-sign/data/data-cubes/"

POPULATION_CSV = "population.csv"
LAU_NUTS_MAP_CSV = "lau-nuts-map.csv"

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

ssl._create_default_https_context = ssl._create_unverified_context  

def main():
    download_data_population()
    download_data_lau_nuts_map()
    create_data_cube()

def download_data_population():
    print(f"Downloading... {POPULATION_CSV}")
    request.urlretrieve(URL_POPULATION, DATA_DIR + POPULATION_CSV)
    print("DONE")

def download_data_lau_nuts_map():
    print(f"Downloading... {LAU_NUTS_MAP_CSV}")
    request.urlretrieve(URL_LAU_NUTS_MAP, DATA_DIR + LAU_NUTS_MAP_CSV)
    print("DONE")

def create_data_cube():
    data = load_data(DATA_DIR + POPULATION_CSV)
    data_cube = create_mean_population_data_cube(data)
    
    data_cube.serialize(format="ttl", destination="./certificate-sign/data/data-cubes/population.ttl")

    print("-" * 5, " population.ttl CREATED ", "-" * 5)

def load_map():
    map_df = pd.read_csv(DATA_DIR + LAU_NUTS_MAP_CSV, low_memory=False)
    map_df = map_df[['CHODNOTA2', 'CHODNOTA1']] 
    map_df.columns = ['NUTS_code', 'LAU_code']
    
    return map_df

def load_data(data_path: str):
    
    map = load_map()

    data = pd.read_csv(data_path)

    data = data.loc[data["vuk"] == "DEM0004"]

    data.loc[(data['vuzemi_cis'] == 101) & (data['vuzemi_kod'] == '554782'), 'vuzemi_cis'] = 40924
    data.loc[(data['vuzemi_cis'] == 101) & (data['vuzemi_kod'] == '554782'), 'vuzemi_kod'] = '101'

    data = pd.merge(data, map, left_on='vuzemi_kod', right_on='NUTS_code')

    data['county_code'] = data['LAU_code'].str[:-1]

    care_providers_df = pd.read_csv("./airflow/data/care-providers.csv", 
                                    usecols=["Kraj", "OkresCode"])

    data["county_txt"] = data["LAU_code"].apply(
        lambda x: care_providers_df.loc[care_providers_df["OkresCode"] == x]["Kraj"].values[0]
    )

    # TODO Drop unnecessary columns

    return data

if __name__ == '__main__':
    main()