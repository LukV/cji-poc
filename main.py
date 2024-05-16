# main.py
from lib.linked_data_api import LinkedDataAPI
from lib.data_cruncher import DataCruncher
from lib.etl_pipeline import ETLPipeline
import argparse

def head(limit: int = 5):
    return f"""
            SELECT * 
            WHERE {{
                {{ GRAPH ?g {{ ?s ?p ?o }} }}
                UNION
                {{ ?s ?p ?o .
                    FILTER NOT EXISTS {{ GRAPH ?g {{ ?s ?p ?o }} }}
                }}
            }} 
            LIMIT {limit}
        """

def count():
    return """
            SELECT (COUNT(DISTINCT ?s) AS ?subjectCount)
            WHERE {
                {
                    GRAPH ?g {  
                        ?s a <https://data.vlaanderen.be/ns/gebouw#Gebouw> .
                    }
                }
                UNION
                {
                    GRAPH ?g {  
                        ?s a <http://purl.org/dc/terms/Location> .
                    }
                }
                UNION  
                {  
                    ?s a <https://data.vlaanderen.be/ns/gebouw#Gebouw> .
                }
                UNION  
                {  
                    ?s a <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur> .
                }
                UNION
                {
                    ?s a <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Buitenruimte> .
                }
            }

        """

endpoints = {
        "publiq": "https://data.uitwisselingsplatform.be/be.publiq.vrijetijdsparticipatie/publiq-uit-locaties/placessparql",
        "jeugdmaps": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/jeugdmaps-infrastructuur/jeugdmapsinfra",
        "kiosk": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/kiosk-infrastructuur/infrastructuur",
        "kampas": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/kampas-infrastructuur/kampasinfrastructuur",
        "anb": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/anb-speelzones-infrastructuur/anbterreinen",
        "erfgoed": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/erfgoedkaart-infrastructuur/infrastructuurerfgoed",
        "terra": "https://data.uitwisselingsplatform.be/be.dcjm.infrastructuur/terra-infrastructuur/terrainfra"
    }

queries = {
    "head": head,
    "count": count,
    "publiq": """
        SELECT ?subject 
            (SAMPLE(?inLocationName) AS ?locationName) 
            (SAMPLE(?inLocationType) AS ?locationType) 
            (SAMPLE(?inFullAddress) AS ?fullAddress)
            (SAMPLE(?inThoroughfare) AS ?thoroughfare)
            (SAMPLE(?inHuisnummer) AS ?huisnummer)
            (SAMPLE(?inBusnummer) AS ?busnummer)
            (SAMPLE(?inPostcode) AS ?postCode) 
            (SAMPLE(?inPostName) AS ?city) 
            (SAMPLE(?inGml) AS ?gml)
            (SAMPLE(?inPoint) AS ?point)
            (SAMPLE(?inBron) AS ?bron)
            (SAMPLE(?inUdbLocationType) AS ?udbLocationType)     
        WHERE {
        GRAPH ?subject {
                ?subject a <http://purl.org/dc/terms/Location> .
                ?subject <http://www.w3.org/ns/locn#locatorName> ?inLocationName .
                ?subject <http://www.w3.org/ns/locn#address> ?address .
                ?address <http://www.w3.org/ns/locn#postcode> ?inPostcode .
                ?address <http://www.w3.org/ns/locn#postName> ?inPostName .
                ?address <http://www.w3.org/ns/locn#fullAddress> ?inFullAddress .
                ?address <http://www.w3.org/ns/locn#thoroughfare> ?inThoroughfare .
                ?address <http://www.w3.org/ns/locn#locatorDesignator> ?inHuisnummer .
                ?subject <http://www.w3.org/ns/locn#geometry> ?geoCoords .
                ?geoCoords <http://www.opengis.net/ont/geosparql#asGML> ?inPoint .
                ?subject <http://purl.org/dc/terms/type> ?inUdbLocationType .
                BIND("" AS ?inBusnummer) .
                BIND("" AS ?ingml) .
                BIND("" AS ?inLocationType) .
                BIND("https://data.uitwisselingsplatform.be/be.publiq.vrijetijdsparticipatie/publiq-uit-locaties/placessparql" AS ?inBron) .
                FILTER NOT EXISTS { ?subject <http://www.w3.org/ns/prov#invalidatedAtTime> ?deletedTime . }
            }
        }
        GROUP BY ?subject
        """,
    "jeugdmaps": """
        SELECT ?subject ?locationName ?locationType ?thoroughfare ?huisnummer ?busnummer ?fullAddress ?postCode ?city ?gml ?point ?bron
        WHERE {
        {
            ?node <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <http://www.w3.org/2004/02/skos/core#notation> ?subject .
            ?node <https://data.vlaanderen.be/ns/gebouw#gebouwnaam> ?locationName .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.type> ?locationType .
            ?node <https://data.vlaanderen.be/ns/gebouw#Gebouw.adres> ?addressNode .
            ?addressNode <http://www.w3.org/ns/locn#fullAddress> ?fullAddress .
            ?addressNode <http://www.w3.org/ns/locn#postCode> ?postCode .
            ?addressNode <https://data.vlaanderen.be/ns/adres#gemeentenaam> ?city .
            ?addressNode <http://www.w3.org/ns/locn#thoroughfare> ?thoroughfare .
            ?addressNode <https://data.vlaanderen.be/ns/adres#adresvoorstelling.huisnummer> ?huisnummer .
            BIND("" AS ?busnummer)
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
            BIND("" AS ?gml)
            BIND("" AS ?point)  
        }
        UNION
        {
            ?node <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <http://www.w3.org/2004/02/skos/core#notation> ?subject .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.naam> ?locationName .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.type> ?locationType .
            ?node <http://www.w3.org/ns/locn#address> ?addressNode .
            ?addressNode <http://www.w3.org/ns/locn#fullAddress> ?fullAddress .
            ?addressNode <http://www.w3.org/ns/locn#postCode> ?postCode .
            ?addressNode <http://www.w3.org/ns/locn#postName> ?city .
            ?addressNode <http://www.w3.org/ns/locn#thoroughfare> ?thoroughfare .
            BIND("" AS ?huisnummer)
            BIND("" AS ?busnummer)
            ?node <http://www.w3.org/ns/locn#geometry> ?geoNode .
            ?geoNode <http://www.w3.org/ns/locn#gml> ?gml .
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
            BIND("" AS ?point)  
        }
        # FILTER (STR(?city) = "Grobbendonk")
        }""",
    "kiosk": """
        SELECT ?subject ?locationName ?locationType ?thoroughfare ?huisnummer ?busnummer ?fullAddress ?postCode ?city ?gml ?point ?bron
        WHERE {
            ?subject a <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur> .
            ?subject <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.locatie> ?address .
            ?address <http://www.w3.org/ns/locn#fullAddress> ?fullAddress .
            ?address <http://www.w3.org/ns/locn#thoroughfare> ?thoroughfare .
            ?address <https://data.vlaanderen.be/ns/adres#Adresvoorstelling.huisnummer> ?huisnummer .
            BIND("" AS ?busnummer)
            ?address <http://www.w3.org/ns/locn#postCode> ?postCode .
            ?address <https://data.vlaanderen.be/ns/adres#gemeentenaam> ?city .
            BIND("" AS ?gml)
            BIND("" AS ?point)
            ?subject <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
        }""",
    "erfgoed": """
        SELECT ?subject ?locationName ?locationType ?thoroughfare ?huisnummer ?busnummer ?fullAddress ?postCode ?city ?gml ?point ?bron
        WHERE {
            ?node <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <http://www.w3.org/2004/02/skos/core#notation> ?subject .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.naam> ?locationName .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.type> ?locationType .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.locatie> ?addressNode .
            ?addressNode <http://www.w3.org/ns/locn#thoroughfare> ?thoroughfare .
            ?addressNode <https://data.vlaanderen.be/ns/adres#Adresvoorstelling.huisnummer> ?huisnummer .
            BIND("" AS ?busnummer)
            ?addressNode <http://www.w3.org/ns/locn#fullAddress> ?fullAddress .
            ?addressNode <http://www.w3.org/ns/locn#postCode> ?postCode .
            ?addressNode <https://data.vlaanderen.be/ns/adres#gemeentenaam> ?city .
            BIND("" AS ?gml)
            BIND("" AS ?point)
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
        }""",
    "kampas": """
        SELECT ?subject ?locationName ?locationType ?thoroughfare ?huisnummer ?busnummer ?fullAddress ?postCode ?city ?gml ?point ?bron
        WHERE {
            ?node <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <http://www.w3.org/2004/02/skos/core#notation> ?subject .
            ?node <https://data.vlaanderen.be/ns/gebouw#gebouwnaam> ?locationName .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.type> ?locationType .
            ?node <https://data.vlaanderen.be/ns/gebouw#adres> ?addressNode .
            ?addressNode <http://www.w3.org/ns/locn#thoroughfare> ?thoroughfare .
            ?addressNode <https://data.vlaanderen.be/ns/adres#Adresvoorstelling.huisnummer> ?huisnummer .
            BIND("" AS ?busnummer)
            ?addressNode <http://www.w3.org/ns/locn#fullAddress> ?fullAddress .
            ?addressNode <http://www.w3.org/ns/locn#postCode> ?postCode .
            ?addressNode <https://data.vlaanderen.be/ns/adres#gemeentenaam> ?city .
            BIND("" AS ?gml)
            BIND("" AS ?point)
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
        }""",
    "anb": """
        SELECT ?subject ?locationName ?locationType ?thoroughfare ?huisnummer ?busnummer ?fullAddress ?postCode ?city ?gml ?point ?bron
        WHERE {
            ?node <http://www.w3.org/ns/adms#identifier> ?identifierNode .
            ?identifierNode <http://www.w3.org/2004/02/skos/core#notation> ?subject .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.naam> ?locationName .
            ?node <https://data.vlaanderen.be/ns/cultuur-en-jeugd/infrastructuur#Infrastructuur.type> ?locationType .
            BIND("" AS ?thoroughfare)
            BIND("" AS ?huisnummer)
            BIND("" AS ?busnummer)
            BIND("" AS ?fullAddress)
            BIND("" AS ?postCode)
            BIND("" AS ?city)
            BIND("" AS ?point)
            ?node <http://www.w3.org/ns/locn#geometry> ?geoNode .
            ?geoNode <http://www.w3.org/ns/locn#gml> ?gml .
            ?identifierNode <https://data.uitwisselingsplatform.be/ns/platform#bron> ?bron .
        }"""
}

def main():
        parser = argparse.ArgumentParser()
        parser.add_argument('-p', '--product', help='the data product to fetch.')
        parser.add_argument('-f', '--file', help='the file to write the data to.')
        parser.add_argument('-m', '--merge', nargs='+', help='the CSV files to merge.')
        parser.add_argument('--etl', help='CJI specifc ETL pipeline to cleanse the data.')
        parser.add_argument('-i', '--input', help='Input CSV file for the ETL pipeline.')

        args = parser.parse_args()

        if args.etl:
            print("Running ETL pipeline")
            
            if not args.input:
                print("Please provide an input CSV file for the ETL pipeline.")
                return
            
            pipeline = ETLPipeline(args.input, "udbmappings.json")
            pipeline.load_data()

            initial_count = pipeline.load_data()
            print(f"Initial number of records: {initial_count}")

            filtered_count = pipeline.filter_udb_location_type()
            print(f"Number of records after filter by UDB type: {filtered_count}")

            grouped_count = pipeline.group_by_id()
            print(f"Number of records after grouping by ID: {grouped_count}")

            pipeline.advanced_cleanup_strings()
            pipeline.fill_empty_location_type()

            quarantine_count = pipeline.remove()
            print(f"Number of records after removing quarantined records (see to_remove.txt): {quarantine_count}")

            deduplicated_count = pipeline.deduplicate_data()
            print("Number of rows per certainty ratio:")
            print(deduplicated_count)

            pipeline.save_data()
            return

        if args.merge:
            print("Merging files: ", args.merge)
            DataCruncher.merge(args.merge)
            return

        if not args.product:
            print("Please provide a data product to fetch: publiq, jeugdmaps, kiosk, erfgoed, kampas, anb")
            return

        api = LinkedDataAPI()
        api.set_data_endpoint(endpoints[args.product])
        api.set_query(queries[args.product])
        df = api.fetch_data()

        # If a filename is provided as a command line argument, write the data to a CSV file
        if args.file:
            df.to_csv(args.file, index=False)

        print(df)

if __name__ == "__main__":
        main()
