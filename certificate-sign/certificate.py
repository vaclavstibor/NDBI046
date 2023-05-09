"""
hash: 493ec96bb36c6e3e24c41203f2eacae6fb2fb4b6

> Generate SHA1 hash
openssl dgst -sha1 population.ttl

> Generate certificate request 
openssl req -new -newkey rsa:2048 -nodes -keyout private.key -out request-file.csr

> Generate self signed certificate
openssl req -x509 -newkey rsa:4096 -sha256 -nodes -keyout private.key -out certificate.crt

> Sign document
openssl dgst -sha256 -sign private.key -out signature-file.sha256 population.ttl
openssl base64 -in signature-file.sha256 -out file-with-save-to-share-signature.sign

"""

from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD

# Define the namespaces
NSR = Namespace('https://vaclavstibor.github.io/resources/')
SPDX = Namespace("http://spdx.org/rdf/terms#")

# Create an empty RDF graph
g = Graph(bind_namespaces="rdflib")

def create_certificate(collector: Graph):

    # Create a new distribution resource
    distribution = collector.resource(
        identifier="_:distribution"
    )
    distribution.add(RDF.type, NSR.Distribution)

    # Create a new SPDX checksum resource
    checksum = collector.resource(
        identifier="_:checksum"
    )
    
    checksum.add(RDF.type, SPDX.Checksum)
    checksum.add(SPDX.algorithm, SPDX.checksumAlgorithm_sha256)
    checksum.add(SPDX.checksumValue, Literal("493ec96bb36c6e3e24c41203f2eacae6fb2fb4b6", datatype=XSD.hexBinary))

    # Add the checksum to the distribution
    distribution.add(SPDX.checksum, checksum)

    #print(g.serialize(format="turtle"))


create_certificate(g)