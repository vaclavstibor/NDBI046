#!/usr/bin/env python3

import sys
import pandas as pd

from rdflib import Graph, BNode, Literal, Namespace
from rdflib.namespace import QB, RDF, XSD, SKOS, DCTERMS

from certificate import create_certificate

NS = Namespace("https://vaclavstibor.github.io/ontology#")
NSR = Namespace("https://vaclavstibor.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")

def create_graph():

    graph = Graph()

    graph.bind("nso", NS)
    graph.bind("dct", DCTERMS)
    graph.bind("qb", QB)
    graph.bind("skos", SKOS)
    graph.bind("sdmx-dimension", SDMX_DIMENSION)
    graph.bind("sdmx-concept", SDMX_CONCEPT)
    graph.bind("sdmx-measure", SDMX_MEASURE)
    graph.bind("sdmx-code", SDMX_CODE)

    return graph

# Datacubes

# 1
def create_care_providers_data_cube(data):

    graph = create_graph()

    create_location_schemes(graph)
    create_field_of_care_scheme(graph)

    create_location_resources(graph, data)
    create_field_of_care_resource(graph, data)

    location_dimensions = create_location_dimensions(graph)
    field_of_care_dimension = create_field_of_care_dimension(graph)
    dimensions = location_dimensions + field_of_care_dimension

    measures = create_care_providers_measure(graph)

    structure = create_structure(graph, dimensions, measures)
    dataset = create_care_providers_metadata(graph, structure)
    
    create_care_providers_observations(graph, dataset, data)

    return graph

# 2
def create_mean_population_data_cube(data):

    graph = create_graph()

    create_certificate(graph)

    create_location_schemes(graph)

    create_location_resources_NUTS_LAU(graph, data)

    dimensions = create_location_dimensions(graph)

    measures = create_mean_population_measure(graph)

    structure = create_structure(graph, dimensions, measures)
    dataset = create_mean_population_metadata(graph, structure)

    create_mean_population_observations(graph, dataset, data)

    return graph

# Conceptual schemes

def create_location_schemes(collector: Graph):

    concepts = [{
                    'uri': NSR.county,
                    'label_cs': 'OkresCode',
                    'label_en': 'CountyCode',
                    'prefLabel_cs': 'OkresCode',
                    'prefLabel_en': 'CountyCode',
                    'isDefinedBy': NSR.County
                },
                {
                    'uri': NSR.region,
                    'label_cs': 'KrajCode',
                    'label_en': 'RegionCode',
                    'prefLabel_cs': 'KrajCode',
                    'prefLabel_en': 'RegionCode',
                    'isDefinedBy': NSR.Region
                }]

    for concept in concepts:
        uri = concept['uri']
        label_cs = concept['label_cs']
        label_en = concept['label_en']
        prefLabel_cs = concept['prefLabel_cs']
        prefLabel_en = concept['prefLabel_en']
        isDefinedBy = concept['isDefinedBy']
        
        collector.add((uri, RDF.type, SKOS.ConceptScheme))
        collector.add((uri, RDFS.isDefinedBy, isDefinedBy))
        collector.add((uri, RDFS.label, Literal(label_cs, lang="cs")))
        collector.add((uri, RDFS.label, Literal(label_en, lang="en")))
        collector.add((uri, SKOS.prefLabel, Literal(prefLabel_cs, lang="cs")))
        collector.add((uri, SKOS.prefLabel, Literal(prefLabel_en, lang="en")))

def create_field_of_care_scheme(collector: Graph):

    concepts = [{
                    'uri': NSR.fieldOfCare,
                    'label_cs': 'Obor péče',
                    'label_en': 'Field of care',
                    'prefLabel_cs': 'Obor péče',
                    'prefLabel_en': 'Field of care',
                    'isDefinedBy': NSR.FieldOfCare
                }]

    for concept in concepts:
        uri = concept['uri']
        label_cs = concept['label_cs']
        label_en = concept['label_en']
        prefLabel_cs = concept['prefLabel_cs']
        prefLabel_en = concept['prefLabel_en']
        isDefinedBy = concept['isDefinedBy']
    
    collector.add((uri, RDF.type, SKOS.ConceptScheme))
    collector.add((uri, RDFS.isDefinedBy, isDefinedBy))
    collector.add((uri, RDFS.label, Literal(label_cs, lang="cs")))
    collector.add((uri, RDFS.label, Literal(label_en, lang="en")))
    collector.add((uri, SKOS.prefLabel, Literal(prefLabel_cs, lang="cs")))
    collector.add((uri, SKOS.prefLabel, Literal(prefLabel_en, lang="en")))


# Resources

def create_location_resources(collector: Graph, df: pd.DataFrame):

    create_resource_classes(collector, [(NSR.County, "Okres", "County"), (NSR.Region, "Kraj", "Region")])

    # Resource: County
    counties = df.drop_duplicates("OkresCode")[["OkresCode", "Okres"]].dropna(subset="OkresCode")

    [collector.add((NSR[f"county/{c['OkresCode']}"], RDF.type, SKOS.Concept)) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], RDF.type, SDMX_CODE.Area)) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], RDF.type, NSR.County)) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], RDFS.label, Literal(c["Okres"], lang="cs"))) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], SKOS.prefLabel, Literal(c["Okres"], lang="cs"))) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], SKOS.inScheme, NSR.county)) 
        and collector.add((NSR[f"county/{c['OkresCode']}"], SKOS.inScheme, SDMX_CODE.area)) 
        for _, c in counties.iterrows()]

    # Resource: Region
    regions = df.drop_duplicates("KrajCode")[["KrajCode", "Kraj"]].dropna(subset="KrajCode")

    [collector.add((NSR[f"region/{r['KrajCode']}"], RDF.type, SKOS.Concept)) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], RDF.type, SDMX_CODE.Area)) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], RDF.type, NSR.Region)) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], RDFS.label, Literal(r["Kraj"], lang="cs"))) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], SKOS.prefLabel, Literal(r["Kraj"], lang="cs"))) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], SKOS.inScheme, NSR.region)) 
        and collector.add((NSR[f"region/{r['KrajCode']}"], SKOS.inScheme, SDMX_CODE.area)) 
        for _, r in regions.iterrows()]

def create_location_resources_NUTS_LAU(collector: Graph, df: pd.DataFrame):

    create_resource_classes(collector, [(NSR.County, "Okres", "County"), (NSR.Region, "Kraj", "Region")])

    # Resource: County
    counties = df

    [collector.add((NSR[f"county/{c['LAU_code']}"], RDF.type, SKOS.Concept)) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], RDF.type, SDMX_CODE.Area)) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], RDF.type, NSR.County)) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], RDFS.label, Literal(c["vuzemi_txt"], lang="cs"))) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], SKOS.prefLabel, Literal(c["vuzemi_txt"], lang="cs"))) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], SKOS.inScheme, NSR.county)) 
        and collector.add((NSR[f"county/{c['LAU_code']}"], SKOS.inScheme, SDMX_CODE.area)) 
        for _, c in counties.iterrows()]

    # Resource: Region
    regions = df.drop_duplicates("county_code")

    [collector.add((NSR[f"region/{r['county_code']}"], RDF.type, SKOS.Concept)) 
        and collector.add((NSR[f"region/{r['county_code']}"], RDF.type, SDMX_CODE.Area)) 
        and collector.add((NSR[f"region/{r['county_code']}"], RDF.type, NSR.Region)) 
        and collector.add((NSR[f"region/{r['county_code']}"], RDFS.label, Literal(r["county_txt"], lang="cs"))) 
        and collector.add((NSR[f"region/{r['county_code']}"], SKOS.prefLabel, Literal(r["county_txt"], lang="cs"))) 
        and collector.add((NSR[f"region/{r['county_code']}"], SKOS.inScheme, NSR.region)) 
        and collector.add((NSR[f"region/{r['county_code']}"], SKOS.inScheme, SDMX_CODE.area)) 
        for _, r in regions.iterrows()]

def create_field_of_care_resource(collector: Graph, df: pd.DataFrame):
    
    create_resource_classes(collector, [(NSR.FieldOfCare, "Obor péče", "Field of care")])

    # Resource: Field of care
    fields_of_care = df["OborPece"].unique()
    foc_index = pd.Index(fields_of_care)
    df["OborPeceCode"] = df["OborPece"].apply(lambda x: foc_index.get_loc(x))
    [collector.add((NSR[f"fieldOfCare/{index}"], RDF.type, SKOS.Concept)) 
        and collector.add((NSR[f"fieldOfCare/{index}"], RDF.type, NSR.FieldOfCare)) 
        and collector.add((NSR[f"fieldOfCare/{index}"], RDFS.label, Literal(fields_of_care[index], lang="cs"))) 
        and collector.add((NSR[f"fieldOfCare/{index}"], SKOS.prefLabel, Literal(fields_of_care[index], lang="cs"))) 
        and collector.add((NSR[f"fieldOfCare/{index}"], SKOS.inScheme, NSR.fieldOfCare)) 
        for index in range(len(fields_of_care))]

def create_resource_classes(collector: Graph, resource_classes: list):

    for res_class in resource_classes:
        res_uri, label_cs, label_en = res_class
        collector.add((res_uri, RDF.type, RDFS.Class))
        collector.add((res_uri, RDFS.label, Literal(label_cs, lang="cs")))
        collector.add((res_uri, RDFS.label, Literal(label_en, lang="en")))
        collector.add((res_uri, SKOS.prefLabel, Literal(label_cs, lang="cs")))
        collector.add((res_uri, SKOS.prefLabel, Literal(label_en, lang="en")))
        collector.add((res_uri, RDFS.isDefinedBy, NSR[res_class[0].split(".")[-1].lower()]))


# Dimensions

def create_location_dimensions(collector: Graph):

    dimensions = []

    # Dimension: County
    county = NS.county

    collector.add((county, RDF.type, RDFS.Property))
    collector.add((county, RDF.type, QB.DimensionProperty))
    collector.add((county, RDF.type, QB.CodedProperty))
    collector.add((county, RDFS.label, Literal("Okres", lang="cs")))
    collector.add((county, RDFS.label, Literal("County", lang="en")))
    collector.add((county, RDFS.range, NSR.County))
    collector.add((county, QB.codeList, NSR.county))
    collector.add((county, RDFS.subPropertyOf, SDMX_DIMENSION.refArea))
    collector.add((county, QB.concept, SDMX_CONCEPT.refArea))

    dimensions.append(county)

    # Dimension: Region
    region = NS.region

    collector.add((region, RDF.type, RDFS.Property))
    collector.add((region, RDF.type, QB.DimensionProperty))
    collector.add((region, RDF.type, QB.CodedProperty))
    collector.add((region, RDFS.label, Literal("Kraj", lang="cs")))
    collector.add((region, RDFS.label, Literal("Region", lang="en")))
    collector.add((region, RDFS.range, NSR.Region))
    collector.add((region, QB.codeList, NSR.region))
    collector.add((county, RDFS.subPropertyOf, SDMX_DIMENSION.refArea))
    collector.add((region, QB.concept, SDMX_CONCEPT.refArea))

    dimensions.append(region)

    return dimensions

def create_field_of_care_dimension(collector: Graph):
    
    # Dimension: Field of care
    field_of_care = NS.fieldOfCare

    collector.add((field_of_care, RDF.type, RDFS.Property))
    collector.add((field_of_care, RDF.type, QB.DimensionProperty))
    collector.add((field_of_care, RDF.type, QB.CodedProperty))
    collector.add((field_of_care, RDFS.label, Literal("Obor péče", lang="cs")))
    collector.add((field_of_care, RDFS.label, Literal("Field of care", lang="en")))
    collector.add((field_of_care, RDFS.range, NSR.FieldOfCare))

    return [field_of_care]

# Measures

def create_care_providers_measure(collector: Graph):

    # Measure: Number of care providers
    num_of_care_providers = NS.numberOfCareProviders

    collector.add((num_of_care_providers, RDF.type, RDFS.Property))
    collector.add((num_of_care_providers, RDF.type, QB.MeasureProperty))
    collector.add((num_of_care_providers, RDFS.label, Literal("Počet poskytovatelů péče", lang="cs")))
    collector.add((num_of_care_providers, RDFS.label, Literal("Number of care providers", lang="en")))
    collector.add((num_of_care_providers, RDFS.range, XSD.integer))
    collector.add((num_of_care_providers, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

    return [num_of_care_providers]

def create_mean_population_measure(collector: Graph):

    # Measure: Mean population
    mean_population = NS.meanPopulation

    collector.add((mean_population, RDF.type, RDFS.Property))
    collector.add((mean_population, RDF.type, QB.MeasureProperty))
    collector.add((mean_population, RDFS.label, Literal("Střední stav obyvatel", lang="cs")))
    collector.add((mean_population, RDFS.label, Literal("Mean population", lang="en")))
    collector.add((mean_population, RDFS.range, XSD.integer))
    collector.add((mean_population, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

    return [mean_population]

# Structure

def create_structure(collector: Graph, dimensions: list, measures: list):

    structure = NS.structure
    collector.add((structure, RDF.type, QB.DataStructureDefinition))

    for dimension in dimensions:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.dimension, dimension))

    for measure in measures:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.measure, measure))

    return structure

# Dataset / Metadata 

def create_care_providers_metadata(collector: Graph, structure):

    dataset = NSR.CareProviders
    
    collector.add((dataset, RDF.type, QB.DataSet))
    collector.add((dataset, RDFS.label, Literal("Poskytovatelé zdravotních služeb", lang="cs")))
    collector.add((dataset, RDFS.label, Literal("Care Providers", lang="en")))
    collector.add((dataset, DCTERMS.title, Literal("Poskytovatelé zdravotních služeb", lang="cs")))
    collector.add((dataset, DCTERMS.title, Literal("Care Providers", lang="en")))
    collector.add((dataset, DCTERMS.description, Literal("Datová kostka poskytovatelů zdravotních služeb v České republice.", lang="cs")))
    collector.add((dataset, DCTERMS.description, Literal("Datacube of health care providers in the Czech Republic.", lang="en")))
    collector.add((dataset, DCTERMS.publisher, Literal("https://github.com/vaclavstibor", datatype=XSD.anyURI)))
    collector.add((dataset, DCTERMS.modified, Literal("2023-03-14", datatype=XSD.date)))    
    collector.add((dataset, DCTERMS.issued, Literal("2023-03-14", datatype=XSD.date)))
    collector.add((dataset, DCTERMS.license, Literal("https://opensource.org/license/mit/", datatype=XSD.anyURI)))
    collector.add((dataset, QB.structure, structure))

    return dataset

def create_mean_population_metadata(collector: Graph, structure):

    dataset = NSR.MeanPopulation
    
    collector.add((dataset, RDF.type, QB.DataSet))
    collector.add((dataset, RDFS.label, Literal("Střední stav obyvatel - okresy 2021", lang="cs")))
    collector.add((dataset, RDFS.label, Literal("Mean Population - county 2021", lang="en")))
    collector.add((dataset, DCTERMS.title, Literal("Střední stav obyvatel - okresy 2021", lang="cs")))
    collector.add((dataset, DCTERMS.title, Literal("Mean Population - county 2021", lang="en")))
    collector.add((dataset, DCTERMS.description, Literal("Datová kostka středního stavu obyvatel pro okresy v roce 2021.", lang="cs")))
    collector.add((dataset, DCTERMS.description, Literal("Datacube of mean population per county in 2021.", lang="en")))
    collector.add((dataset, DCTERMS.publisher, Literal("https://github.com/vaclavstibor", datatype=XSD.anyURI)))
    collector.add((dataset, DCTERMS.modified, Literal("2023-03-14", datatype=XSD.date)))    
    collector.add((dataset, DCTERMS.issued, Literal("2023-03-14", datatype=XSD.date)))
    collector.add((dataset, DCTERMS.license, Literal("https://opensource.org/license/mit/", datatype=XSD.anyURI)))
    collector.add((dataset, QB.structure, structure))

    return dataset   

# Observations

def create_care_providers_observations(collector: Graph, dataset, data: pd.DataFrame):
    grouped = data.groupby(["OkresCode", "KrajCode", "OborPeceCode"]).size().reset_index(name="PocetPoskytovaluPece")
    for index, row in grouped.iterrows():
        resource = NSR["observation-" + str(index).zfill(4)]
        create_care_providers_observation(collector, dataset, resource, row)


def create_care_providers_observation(collector: Graph, dataset, resource, data):
    collector.add((resource, RDF.type, QB.Observation))
    collector.add((resource, QB.dataSet, dataset))
    collector.add((resource, NS.county, NSR[f"county/{data['OkresCode']}"]))
    collector.add((resource, NS.region, NSR[f"region/{data['KrajCode']}"]))
    collector.add((resource, NS.fieldOfCare, NSR[f"fieldOfCare/{data['OborPeceCode']}"]))
    collector.add((resource, NS.numberOfCareProviders, Literal(data["PocetPoskytovaluPece"], datatype=XSD.integer)))

def create_mean_population_observations(collector: Graph, dataset, data: pd.DataFrame):
    mean_population_data = data.reset_index()
    for index, row in mean_population_data.iterrows():
        resource = NSR["observation-" + str(index).zfill(2)]
        create_mean_population_observation(collector, dataset, resource, row)


def create_mean_population_observation(collector: Graph, dataset, resource, data):
    collector.add((resource, RDF.type, QB.Observation))
    collector.add((resource, QB.dataSet, dataset))
    collector.add((resource, NS.county, NSR[f"county/{data['LAU_code']}"]))
    collector.add((resource, NS.region, NSR[f"region/{data['county_code']}"]))
    collector.add((resource, NS.meanPopulation, Literal(data["hodnota"], datatype=XSD.integer)))