import requests
import mysql.connector
from elasticsearch import Elasticsearch
import json
import os
import time
import logging
from urllib.parse import urlencode
from pathlib import Path

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Helper function to retry a function call every 5 sec up to 5 attempts.
def retry(fn, attempts=5, delay=5):
    last_error = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as err:
            last_error = err
            logger.warning(f"Attempt {i + 1} failed: {err}")
            if i < attempts - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    logger.error(f"All {attempts} attempts failed: {last_error}")
    return None

# Configuration
NCBI_API = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
mysqlConfig = {
    "host": "mysql",
    "user": "root",
    "password": "password",
    "database": "ncbi_virus",
}
es_client = Elasticsearch(["http://elasticsearch:9200"])

def fetchMetadata(query: str):
    searchUrl = NCBI_API + "esearch.fcgi"
    searchParams = {
        "db": "nuccore",
        "term": query,
        "retmode": "json",
        "retmax": "1000",
    }
    completeSearchUrl = f"{searchUrl}?{urlencode(searchParams)}"
    logger.info(f"NCBI Search URL: {completeSearchUrl}")
    logger.info("Making request to NCBI API for metadata search")
    
    response = requests.get(searchUrl, params=searchParams)
    searchResponse = response.json()
    ids = searchResponse.get("esearchresult", {}).get("idlist", [])
    if not ids:
        logger.warning("No IDs returned from NCBI search.")
        return []
    
    summaryUrl = NCBI_API + "esummary.fcgi"
    metadataCacheDir = Path(os.getcwd()) / "cache" / "metadata"
    metadataCacheDir.mkdir(parents=True, exist_ok=True)
    
    records = []
    for id_ in ids:
        cacheFile = metadataCacheDir / f"{id_}.json"
        record = None
        if cacheFile.exists():
            try:
                with open(cacheFile, "r", encoding="utf-8") as f:
                    record = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache for ID {id_}: {e}")
        else:
            summaryParams = {"db": "nuccore", "id": id_, "retmode": "json"}
            completeSummaryUrl = f"{summaryUrl}?{urlencode(summaryParams)}"
            logger.info(f"NCBI Summary URL for ID {id_}: {completeSummaryUrl}")
            
            def fetch_summary():
                resp = requests.get(summaryUrl, params=summaryParams)
                summaryResponse = resp.json()
                return summaryResponse.get("result", {}).get(id_)
            
            record = retry(fetch_summary, attempts=5, delay=5)
            if record:
                logger.info(f"Fetched record for ID {id_}")
                try:
                    with open(cacheFile, "w", encoding="utf-8") as f:
                        json.dump(record, f)
                except Exception as e:
                    logger.error(f"Error writing cache file for ID {id_}: {e}")
            else:
                logger.error(f"Failed to fetch metadata for ID {id_} after 5 attempts")
                continue
        if record:
            records.append(record)
    return records

def fetchFasta(ids: list):
    fastaCacheDir = Path(os.getcwd()) / "cache" / "fasta"
    fastaCacheDir.mkdir(parents=True, exist_ok=True)
    
    results = []
    for uid in ids:
        cacheFile = fastaCacheDir / f"{uid}.fasta"
        fasta = ""
        if cacheFile.exists():
            try:
                with open(cacheFile, "r", encoding="utf-8") as f:
                    fasta = f.read()
            except Exception as e:
                logger.warning(f"Failed to read cache for FASTA ID {uid}: {e}")
        else:
            fastaUrl = NCBI_API + "efetch.fcgi"
            params = {
                "db": "nuccore",
                "id": uid,
                "rettype": "fasta",
                "retmode": "text",
            }
            completeFastaUrl = f"{fastaUrl}?{urlencode(params)}"
            logger.info(f"NCBI FASTA URL for ID {uid}: {completeFastaUrl}")
            
            def fetch_fasta_func():
                resp = requests.get(fastaUrl, params=params)
                return resp.text
            
            fasta = retry(fetch_fasta_func, attempts=5, delay=5) or ""
            if fasta:
                logger.info(f"Fetched FASTA for ID {uid}")
                try:
                    with open(cacheFile, "w", encoding="utf-8") as f:
                        f.write(fasta)
                except Exception as e:
                    logger.error(f"Error writing FASTA cache for ID {uid}: {e}")
            else:
                logger.warning(f"Failed to fetch FASTA for ID {uid} after 5 attempts; using empty FASTA.")
        results.append({"uid": uid, "fasta": fasta})
    return results

def deduplicate(metadata: list):
    unique = {}
    for record in metadata:
        uid = record.get("uid")
        if uid:
            unique[uid] = record
    return list(unique.values())

def loadToMySQLFull(metadata: list, fastaData: list):
    logger.info("Loading data to MySQL")
    try:
        conn = mysql.connector.connect(**mysqlConfig)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ncbi_records (
                uid VARCHAR(255) PRIMARY KEY,
                metadata JSON,
                fasta TEXT
            )
        """)
        conn.commit()
        # Map FASTA data by uid
        fastaMap = {item["uid"]: item["fasta"] for item in fastaData}
        for record in metadata:
            uid = record.get("uid")
            fasta = fastaMap.get(uid)
            query = "REPLACE INTO ncbi_records (uid, metadata, fasta) VALUES (%s, %s, %s)"
            cursor.execute(query, (uid, json.dumps(record), fasta))
        conn.commit()
    except Exception as e:
        logger.error(f"MySQL error: {e}")
    finally:
        cursor.close()
        conn.close()

def loadToElasticSearch(metadata: list, fastaData: list):
    logger.info("Loading data to ElasticSearch")
    indexName = "ncbi_records"
    
    # Check if index exists; create it if it doesn't.
    if not es_client.indices.exists(index=indexName):
        es_client.indices.create(index=indexName)
        logger.info(f"Created index {indexName} in ElasticSearch")
    
    logger.info("Indexing data to ElasticSearch")
    fastaMap = {item["uid"]: item["fasta"] for item in fastaData}
    
    bulk_ops = []
    for record in metadata:
        uid = record.get("uid")
        fasta = fastaMap.get(uid)
        bulk_ops.append({"index": {"_index": indexName, "_id": uid}})
        bulk_ops.append({"metadata": record, "fasta": fasta})
    
    if bulk_ops:
        bulk_response = es_client.bulk(body=bulk_ops)
        if bulk_response.get("errors"):
            logger.error("Errors occurred during bulk insert to ElasticSearch")
        else:
            logger.info("Successfully loaded data to ElasticSearch")

def main():
    logger.info("Starting ETL Pipeline")
    QUERY = '(SARS-CoV-2[Organism]) AND South Dakota[Location] AND ("2023/01/01"[PDAT]:"2023/03/31"[PDAT])'
    metadata = fetchMetadata(QUERY)
    logger.info(f"Fetched {len(metadata)} metadata records")
    if not metadata:
        logger.error("No metadata found, exiting...")
        return
    
    uniqueMetadata = deduplicate(metadata)
    ids = [item.get("uid") for item in uniqueMetadata if item.get("uid")]
    logger.info(f"Deduplicated to {len(uniqueMetadata)} unique records")
    
    fastaData = fetchFasta(ids)
    logger.info(f"FASTA data length: {len(fastaData)}")
    
    loadToMySQLFull(uniqueMetadata, fastaData)
    loadToElasticSearch(uniqueMetadata, fastaData)
    
    logger.info("ETL pipeline completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logger.error("ETL failed", exc_info=err)
