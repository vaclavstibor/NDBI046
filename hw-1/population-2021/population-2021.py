#!/usr/bin/env python3
import sys
import pandas as pd

from rdflib import Graph, BNode, Literal, Namespace
from rdflib.namespace import QB, RDF, XSD, SKOS, OWL, DCTERMS

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def main():
    """
    if len(sys.argv) == 1:
        print(f"{sys.argv[0]}: Input data file argument is required.", file=sys.stderr)
        return

    file_name = sys.argv[1]
    """

    file_name = "/Users/stiborv/Documents/LS2223/NDBI046/hw-1/population-2021/data/130141-22data2021.csv"
    data_frame = load_csv_file_as_df(file_name)
    NUTS_LAU_mapping = load_NUTS_to_LAU_mapping()
    #data_cube = as_data_cube(data_frame)
    #data_cube.serialize(format="ttl", destination="/Users/stiborv/Documents/LS2223/NDBI046/hw-1/population-2021/data/population-2021.ttl")
    print("-" * 80)

def load_csv_file_as_df(file_path: str):
    data_frame = pd.read_csv(file_path, low_memory=False)
    data_frame = data_frame.loc[data_frame["vuk"] == "DEM0004"]
    print(data_frame)
    return data_frame

def load_NUTS_to_LAU_mapping():
    NUTS_to_LAU_mapping = dict()

    df = pd.read_csv("/Users/stiborv/Documents/LS2223/NDBI046/hw-1/population-2021/data/číselník-okresů-vazba-101-nadřízený.csv")

    for index, row in df.iterrows():
        if row["CHODNOTA2"]:
            NUTS_to_LAU_mapping[row["CHODNOTA2"]] = row["CHODNOTA1"]

    print(NUTS_to_LAU_mapping)
    return NUTS_to_LAU_mapping

if __name__ == "__main__":
    main()