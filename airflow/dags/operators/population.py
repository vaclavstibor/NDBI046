#!/usr/bin/env python3
import pandas as pd
import os

from rdflib import Namespace
from operators.datacube import create_mean_population_data_cube

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def population_data_cube():

    file_path = "./airflow/data/population.csv"
    data = load_data(file_path)
    data_cube = create_mean_population_data_cube(data)

    data_cube.serialize(format="ttl", destination="./airflow/data/data-cubes/population.ttl")

    print("-" * 5, " population.ttl CREATED ", "-" * 5)

def load_map():

    file_path = "./airflow/data/lau-nuts-map.csv"

    map_df = pd.read_csv(file_path, low_memory=False)
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