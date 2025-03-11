import got from "got"
import mysql from "mysql2/promise"
import { Client } from "@elastic/elasticsearch"
import { promises as fs } from "fs"
import path from "path"
import winston from "winston"
import pLimit from "p-limit"
import { setTimeout } from "timers/promises"

// Define interfaces for metadata and FASTA data
interface MetadataRecord {
    uid: string
    [key: string]: any
}

interface FastaData {
    uid: string
    fasta: string
}

// Logger setup
const logger = winston.createLogger({
    level: "debug",
    format: winston.format.combine(
        winston.format.colorize(),
        winston.format.timestamp(),
        winston.format.printf(
            ({ timestamp, level, message }) =>
                `${timestamp} [${level}] ${message}`,
        ),
    ),
    transports: [new winston.transports.Console()],
})

// Helper function to retry a promise-returning function every 5 sec up to 5 attempts.
async function retry<T>(
    fn: () => Promise<T>,
    attempts = 5,
    delay = 5000,
): Promise<T | null> {
    let lastError: any
    for (let i = 0; i < attempts; i++) {
        try {
            return await fn()
        } catch (err: any) {
            lastError = err
            logger.warn(`Attempt ${i + 1} failed: ${err.message}`)
            if (i < attempts - 1) {
                logger.info(`Retrying in ${delay / 1000} seconds...`)
                await new Promise((resolve) => setTimeout(resolve, delay))
            }
        }
    }
    logger.error(`All ${attempts} attempts failed: ${lastError.message}`)
    return null
}

// Configuration
const NCBI_API = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
const esClient = new Client({ node: "http://elasticsearch:9200" })

const mysqlConfig = {
    host: "mysql",
    user: "root",
    password: "password",
    database: "ncbi_virus",
}

async function fetchMetadata(query: string): Promise<MetadataRecord[]> {
    const searchUrl = `${NCBI_API}esearch.fcgi`
    const searchParams = {
        db: "nuccore",
        term: query,
        retmode: "json",
        retmax: "1000",
    }

    // Log the complete URL for NCBI search
    const completeSearchUrl = `${searchUrl}?${new URLSearchParams(searchParams).toString()}`
    logger.info(`NCBI Search URL: ${completeSearchUrl}`)

    logger.info("Making request to NCBI API for metadata search")
    const searchResponse = await got(searchUrl, {
        searchParams,
        responseType: "json",
    })

    const ids: string[] = searchResponse.body.esearchresult.idlist

    if (!ids.length) {
        logger.warn("No IDs returned from NCBI search.")
        return []
    }

    const summaryUrl = `${NCBI_API}esummary.fcgi`
    const limit = pLimit(3)

    // Ensure the metadata cache directory exists
    const metadataCacheDir = path.join(process.cwd(), "cache", "metadata")
    await fs.mkdir(metadataCacheDir, { recursive: true })

    const promises = ids.map((id: string) =>
        limit(async (): Promise<MetadataRecord | null> => {
            const cacheFile = path.join(metadataCacheDir, `${id}.json`)
            let record: MetadataRecord | null = null
            try {
                const cachedData = await fs.readFile(cacheFile, "utf8")
                record = JSON.parse(cachedData)
            } catch (err) {
                // Cache miss: fetch from API with retry
                const summaryParams = { db: "nuccore", id, retmode: "json" }
                const completeSummaryUrl = `${summaryUrl}?${new URLSearchParams(summaryParams).toString()}`
                logger.info(
                    `NCBI Summary URL for ID ${id}: ${completeSummaryUrl}`,
                )

                record = await retry(
                    async () => {
                        const summaryResponse = await got(summaryUrl, {
                            searchParams: summaryParams,
                            responseType: "json",
                            retry: {
                                limit: 5,
                            },
                        })
                        return summaryResponse.body.result[id]
                    },
                    5,
                    5000,
                )

                if (record) {
                    logger.info(`Fetched record for ID ${id}`)
                    await fs.writeFile(cacheFile, JSON.stringify(record))
                } else {
                    logger.error(
                        `Failed to fetch metadata for ID ${id} after 5 attempts`,
                    )
                    return null
                }
            }
            return record
        }),
    )

    const results = await Promise.all(promises)
    return results.filter((record): record is MetadataRecord => Boolean(record))
}

async function fetchFasta(ids: string[]): Promise<FastaData[]> {
    const fastaCacheDir = path.join(process.cwd(), "cache", "fasta")
    await fs.mkdir(fastaCacheDir, { recursive: true })
    const limit = pLimit(3)

    const promises = ids.map((uid: string) =>
        limit(async (): Promise<FastaData> => {
            const cacheFile = path.join(fastaCacheDir, `${uid}.fasta`)
            let fasta: string = ""
            try {
                fasta = await fs.readFile(cacheFile, "utf8")
            } catch (err) {
                // Cache miss: fetch from API with retry
                const fastaUrl = `${NCBI_API}efetch.fcgi`
                const params = {
                    db: "nuccore",
                    id: uid,
                    rettype: "fasta",
                    retmode: "text",
                }
                const completeFastaUrl = `${fastaUrl}?${new URLSearchParams(params).toString()}`
                logger.info(`NCBI FASTA URL for ID ${uid}: ${completeFastaUrl}`)

                fasta =
                    (await retry(
                        async () => {
                            const fastaResponse = await got(fastaUrl, {
                                searchParams: params,
                                retry: {
                                    limit: 5,
                                },
                                throwHttpErrors: false,
                            })
                            return fastaResponse.body
                        },
                        5,
                        5000,
                    )) || ""

                if (fasta) {
                    logger.info(`Fetched FASTA for ID ${uid}`)
                    await fs.writeFile(cacheFile, fasta)
                } else {
                    // Instead of returning null, we fallback to an empty FASTA string
                    logger.warn(
                        `Failed to fetch FASTA for ID ${uid} after 5 attempts; using empty FASTA.`,
                    )
                }
            }
            return { uid, fasta }
        }),
    )

    const results = await Promise.all(promises)
    return results
}

function deduplicate(metadata: MetadataRecord[]): MetadataRecord[] {
    const unique = new Map<string, MetadataRecord>()
    metadata.forEach((record) => unique.set(record.uid, record))
    return Array.from(unique.values())
}

async function loadToMySQLFull(
    metadata: MetadataRecord[],
    fastaData: FastaData[],
): Promise<void> {
    logger.info("Loading data to MySQL")
    const connection = await mysql.createConnection(mysqlConfig)
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS ncbi_records (
         uid VARCHAR(255) PRIMARY KEY,
         metadata JSON,
         fasta TEXT
      )
  `)

    // Create a map for FASTA data based on uid
    const fastaMap = new Map<string, string>()
    fastaData.forEach((item) => {
        fastaMap.set(item.uid, item.fasta)
    })

    const insertPromises = metadata.map((record) => {
        const uid = record.uid
        const fasta = fastaMap.get(uid) || null
        return connection.execute(
            `REPLACE INTO ncbi_records (uid, metadata, fasta) VALUES (?, ?, ?)`,
            [uid, JSON.stringify(record), fasta],
        )
    })

    await Promise.all(insertPromises)
    await connection.end()
}

async function loadToElasticSearch(
    metadata: MetadataRecord[],
    fastaData: FastaData[],
): Promise<void> {
    logger.info("Loading data to ElasticSearch")
    const indexName = "ncbi_records"

    // Check if index exists; create it if it doesn't.
    const existsResult = await esClient.indices.exists({ index: indexName })
    if (!existsResult) {
        await esClient.indices.create({ index: indexName })
        logger.info(`Created index ${indexName} in ElasticSearch`)
    }

    logger.info("Indexing data to ElasticSearch")

    // Create a map for FASTA data based on uid
    const fastaMap = new Map<string, string>()
    fastaData.forEach((item) => {
        try {
            fastaMap.set(item.uid, item.fasta)
        } catch (e) {
            console.log("Error in setting fastaMap", e)
        }
    })

    // Prepare bulk operations for ES indexing
    const bulkOps: any[] = []
    metadata.forEach((record) => {
        const uid = record.uid
        const fasta = fastaMap.get(uid) || null
        bulkOps.push({ index: { _index: indexName, _id: uid } })
        bulkOps.push({ metadata: record, fasta })
    })

    if (bulkOps.length > 0) {
        const bulkResponse = await esClient.bulk({ body: bulkOps })
        if (bulkResponse.errors) {
            logger.error("Errors occurred during bulk insert to ElasticSearch")
            // Optionally: parse and handle individual errors here.
        } else {
            logger.info("Successfully loaded data to ElasticSearch")
        }
    }
}

async function main(): Promise<void> {
    logger.info("Starting ETL Pipeline")

    const QUERY =
        '(SARS-CoV-2[Organism]) AND South Dakota[Location] AND ("2023/01/01"[PDAT]:"2023/03/31"[PDAT])'
    const metadata = await fetchMetadata(QUERY)
    logger.info(`Fetched ${metadata.length} metadata records`)
    if (!metadata.length) {
        logger.error("No metadata found, exiting...")
        return
    }

    const uniqueMetadata = deduplicate(metadata)
    const ids = uniqueMetadata.map((item) => item.uid)
    logger.info(`Deduplicated to ${uniqueMetadata.length} unique records`)

    const fastaData = await fetchFasta(ids)
    console.log("FASTA data length:", fastaData.length)

    // Store data in MySQL
    await loadToMySQLFull(uniqueMetadata, fastaData)
    // Also index data in ElasticSearch
    await loadToElasticSearch(uniqueMetadata, fastaData)

    logger.info("ETL pipeline completed successfully!")
}

main().catch((err: any) => logger.error("ETL failed", err))
