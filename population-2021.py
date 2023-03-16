#!/usr/bin/env python3
import pandas as pd
import os

from rdflib import Namespace
from datacube import create_mean_population_data_cube

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def main():

    file_path = "./data/130141-22data2021.csv"
    data = load_data(file_path)
    data_cube = create_mean_population_data_cube(data)

    if not os.path.exists('./data/data-cubes'):
        os.makedirs('./data/data-cubes')

    data_cube.serialize(format="ttl", destination="./data/data-cubes/population-2021.ttl")

    print("-" * 5, " DataCube Created ", "-" * 5)

def load_map():

    file_path = "./data/číselník-okresů-vazba-101-nadřízený.csv"

    map_df = pd.read_csv(file_path)
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

    care_providers_df = pd.read_csv("./data/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv", 
                                    usecols=["Kraj", "OkresCode"])

    data["county_txt"] = data["LAU_code"].apply(
        lambda x: care_providers_df.loc[care_providers_df["OkresCode"] == x]["Kraj"].values[0]
    )

    # TODO Drop unnecessary columns

    return data

if __name__ == "__main__":
    main()