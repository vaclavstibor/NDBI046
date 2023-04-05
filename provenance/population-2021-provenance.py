#!/usr/bin/env python3

from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDF, FOAF, XSD, PROV

NSR = Namespace("https://vaclavstibor.github.io/resources/")
NSP = Namespace("https://vaclavstibor.github.io/provenance/")

def construct_graph() -> Graph:
    graph = Graph(bind_namespaces="rdflib") # bind_namespaces={"prov": PROV, "nsr": NSR, "nsp": NSP}

    create_entities(graph)
    create_agents(graph)
    create_activities(graph)

    graph.add((NSP.Creator, RDF.type, PROV.Role))

    return graph

def create_entities(collector: Graph) -> None:
    
    data_cube = NSR.Population2021
    collector.add((data_cube, RDF.type, PROV.Entity))
    collector.add((data_cube, PROV.wasGeneratedBy, NSP.Population2021Script))
    collector.add((data_cube, PROV.wasDerivedFrom, NSP.Population2021Dataset))
    collector.add((data_cube, PROV.wasAttributedTo, NSP.Population2021Script))

    datasetOfCareProviders = NSP.CareProvidersDataset
    collector.add((datasetOfCareProviders, RDF.type, PROV.Entity))
    collector.add((datasetOfCareProviders, PROV.wasGeneratedBy, NSP.MinistryOfHealth))

    datasetOfPopulation2021 = NSP.Population2021Dataset
    collector.add((datasetOfPopulation2021, RDF.type, PROV.Entity))
    collector.add((datasetOfPopulation2021, PROV.wasGeneratedBy, NSP.CzechStatisticalOffice))

    datasetOfCountyCodelistMap = NSP.CountyCodelistMap
    collector.add((datasetOfCountyCodelistMap, RDF.type, PROV.Entity))
    collector.add((datasetOfCountyCodelistMap, PROV.wasAttributedTo, NSP.CzechStatisticalOffice))

def create_agents(collector: Graph) -> None:

    # script agent
    script = NSP.Population2021Script
    collector.add((script, RDF.type, PROV.Agent))
    collector.add((script, RDF.type, PROV.SoftwareAgent))
    collector.add((script, PROV.actedOnBehalfOf, NSP.VaclavStibor))
    collector.add((script, PROV.wasAssociatedWith, NSP.CareProvidersDataset))
    collector.add((script, PROV.wasAssociatedWith, NSP.Population2021Dataset))
    collector.add((script, PROV.wasAssociatedWith, NSP.CountyCodelistMap))
    collector.add((script, PROV.atLocation, URIRef("data-cubes-dag.py")))

    # organization agent
    mff_cuni = NSP.MFF_CUNI
    collector.add((mff_cuni, RDF.type, PROV.Agent))
    collector.add((mff_cuni, RDF.type, PROV.Organization))
    collector.add((mff_cuni, FOAF.name, Literal("Matematicko-fyzikální fakulta, Univerzita Karlova", lang="cs")))
    collector.add((mff_cuni, FOAF.schoolHomepage, Literal("https://www.mff.cuni.cz/", datatype=XSD.anyURI)))
    
    # person agent
    teacher = NSP.PetrSkoda
    collector.add((teacher, RDF.type, PROV.Person))
    collector.add((teacher, RDF.type, PROV.Agent))
    collector.add((teacher, PROV.actedOnBehalfOf, NSP.MFF_CUNI))
    collector.add((teacher, FOAF.name, Literal("Mgr. Petr Škoda, Ph.D.", lang="cs")))
    collector.add((teacher, FOAF.mbox, URIRef("mailto:petr.skoda@matfyz.cuni.cz")))

    # person agent
    author = NSP.VaclavStibor
    collector.add((teacher, RDF.type, FOAF.Person))
    collector.add((teacher, RDF.type, PROV.Agent))
    collector.add((teacher, PROV.actedOnBehalfOf, NSP.PetrSkoda))
    collector.add((teacher, FOAF.name, Literal("Václav Stibor", lang="cs")))
    collector.add((teacher, FOAF.mbox, URIRef("mailto:vasa20017@seznam.cz")))

    # organization agent
    ministry_of_health = NSP.MinistryOfHealth
    collector.add((ministry_of_health, RDF.type, PROV.Agent))
    collector.add((ministry_of_health, RDF.type, PROV.Organization))
    collector.add((ministry_of_health, FOAF.name, Literal("Ministerstvo zdravotnictví České republiky", lang="cs")))
    collector.add((ministry_of_health, FOAF.homepage, Literal("https://www.mzcr.cz/", datatype=XSD.anyURI)))
   
    # organization agent
    ministry_of_health = NSP.CzechStatisticalOffice
    collector.add((ministry_of_health, RDF.type, PROV.Agent))
    collector.add((ministry_of_health, RDF.type, PROV.Organization))
    collector.add((ministry_of_health, FOAF.name, Literal("Český statistický úřad", lang="cs")))
    collector.add((ministry_of_health, FOAF.homepage, Literal("https://www.czso.cz/", datatype=XSD.anyURI)))

def create_activities(collector: Graph) -> None:
    activity = NSP.CreateCareProvidersDataCube
    collector.add((activity, RDF.type, PROV.Activity))
    collector.add((activity, PROV.used, NSP.CareProvidersDataset))
    collector.add((activity, PROV.wasAssociatedWith, NSP.VaclavStibor))
    collector.add((activity, PROV.startedAtTime, Literal("2023-04-05T12:00:00", datatype=XSD.dateTime)))
    collector.add((activity, PROV.endedAtTime, Literal("2023-04-05T12:05:00", datatype=XSD.dateTime)))

    datasets = [
        (NSP.CareProvidersDataset, NSP.Creator),
        (NSP.Population2021Dataset, NSP.Creator),
        (NSP.CountyCodelistMap, NSP.Creator)
    ]

    for dataset, role in datasets:
        usage = BNode()
        collector.add((activity, PROV.qualifiedUsage, usage))
        collector.add((usage, RDF.type, PROV.Usage))
        collector.add((usage, PROV.entity, dataset))
        collector.add((usage, PROV.hadRole, role))


def create_provenance_document() -> None:
    result = construct_graph()
    result.serialize(format="trig", destination="./provenance/documents/population-2021-provenance.trig")

if __name__ == "__main__":
    create_provenance_document()