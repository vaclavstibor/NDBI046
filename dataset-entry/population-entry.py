#!/usr/bin/env python3

from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import DCAT, RDF, DCTERMS, RDFS

NSR = Namespace('https://vaclavstibor.github.io/resources/')

def main():
    collector = Graph(bind_namespaces="rdflib")
    create_dataset_entry(collector)
    collector.serialize(format='ttl', encoding='UTF-8', destination="./dataset-entry/population-dataset")


def create_dataset_entry(collector: Graph):
    collector.add((NSR.PopulationDataCube, RDF.type, DCAT.Dataset))
    collector.add((NSR.PopulationDataCube, DCTERMS.title, Literal('Population data cube', lang='en')))
    collector.add((NSR.PopulationDataCube, RDFS.label, Literal('Population data cube', lang='en')))
    collector.add((NSR.PopulationDataCube, DCTERMS.description, Literal('Data cube that presents the average population figures for Czech counties and regions in 2021', lang='en')))
    collector.add((NSR.PopulationDataCube, DCTERMS.spatial, URIRef('http://publications.europa.eu/resource/authority/country/CZE')))
    collector.add((NSR.PopulationDataCube, DCTERMS.accrualPeriodicity, URIRef('http://publications.europa.eu/resource/authority/frequency/NEVER')))
    collector.add((NSR.PopulationDataCube, DCTERMS.publisher, URIRef('https://github.com/vaclavstibor')))
    collector.add((NSR.PopulationDataCube, DCTERMS.creator, URIRef('https://github.com/vaclavstibor')))
    collector.add((NSR.PopulationDataCube, DCAT.keyword, Literal('population', lang='en')))
    collector.add((NSR.PopulationDataCube, DCAT.keyword, Literal('mean population', lang='en')))
    collector.add((NSR.PopulationDataCube, DCAT.keyword, Literal('population of Czech counties', lang='en')))   
    collector.add((NSR.PopulationDataCube, DCAT.keyword, Literal('counties', lang='en')))     
    collector.add((NSR.PopulationDataCube, DCAT.keyword, Literal('region', lang='en')))
    collector.add((NSR.PopulationDataCube, DCAT.theme, URIRef('http://eurovoc.europa.eu/2908')))

    distribution = NSR['PopulationDataCube.ttl']
    collector.add((NSR.PopulationDataCube, DCAT.distribution, distribution))
    collector.add((distribution, RDF.type, DCAT.Distribution))
    collector.add((distribution, DCAT.accessURL, distribution))
    collector.add((distribution, DCTERMS.format, URIRef('http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE')))
    collector.add((distribution, DCAT.mediaType, URIRef('http://www.iana.org/assignments/media-types/text/turtle')))

if __name__ == '__main__':
    main()