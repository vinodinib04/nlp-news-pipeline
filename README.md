# NLP Pipeline — NewsAPI
### Project 2 

---

## Overview

This project implements a 6-layer cloud-native NLP pipeline on Microsoft Azure that ingests news articles from NewsAPI, enriches them with AI-powered language processing, computes analytics, and exposes a hybrid search API.

**Technology:** Python · Azure Logic Apps · Azure Functions · Azure AI Language · Azure OpenAI · Azure Databricks · Azure Data Factory · Azure AI Search · ADLS Gen2


---

## Architecture

```
NewsAPI
   │
   ▼
Layer 1 — Raw Ingestion (Logic Apps → ADLS Gen2 raw-landing/)
   │
   ▼
Layer 2 — NLP Enrichment (Azure Functions → Cognitive Services → silver/)
   │
   ▼
Layer 3 — Gold Analytics (Databricks → ADF → gold/)
   │
   ▼
Layer 4 — Hybrid Search Index (Azure AI Search)
   │
   ▼
Layer 5 — Search API (Azure Functions HTTP endpoint)
   │
   ▼
Layer 6 — Governance (PII flagging + Lineage documentation)
```

---

## Layer 1 — Raw Layer (Data Ingestion)

### Objective
Collect news from four categories and store original API responses without modification.

### What was built
- 4 Azure Logic Apps — one per news category:
  - Technology News
  - Business News
  - Science News
  - Health News
- Each Logic App has a **Recurrence trigger** running at scheduled intervals(6 Hours)
- HTTP action calls the NewsAPI endpoint per category
- Response stored in **ADLS Gen2** under `raw-landing/articles/` with timestamp-based filenames:
```
2026-06-20T09:59:43-tech.json
2026-06-20T10:10:47-business.json
2026-06-20T10:17:19-science.json
2026-06-20T10:21:39-health.json
```

### Output
Original NewsAPI JSON files stored exactly as received — no transformation at this layer.

### Flow
```
NewsAPI → Logic Apps → raw-landing/ (ADLS Gen2)
```

### Sample Output
See `layer1-ingestion/sample_news.json`

---

## Layer 2 — Silver Layer (NLP Enrichment)

### Objective
Transform raw news articles into enriched NLP data ready for analytics and search.

### What was built
- **Azure Function** (`fn_trigger_nlp`) triggered automatically by Event Grid whenever a new JSON file arrives in `raw-landing/`
- **Azure Function** (`fn_audit_log`) logs each batch arrival to `audit-logs/`

### Enrichment steps per article

**1. URL Hash (Deduplication)**
```
url_hash = MD5(article.url)
```
Used to skip already-processed articles on repeat runs.

**2. Sentiment Analysis** via Azure AI Language Service (F0 free tier)
```
Result:  positive / neutral / negative
Scores:  { positive: 0.88, neutral: 0.11, negative: 0.01 }
```

**3. Named Entity Recognition**
```
Extracted: Apple:Organization, India:Location, OpenAI:Organization
```

**4. Key Phrase Extraction**
```
Extracted: Prime Day, Artificial Intelligence, Apple Watch
```

**5. Embedding Generation** via Azure OpenAI (text-embedding-ada-002)
```
1536-dimension vector per article for semantic/hybrid search
```

**6. PII Detection**
```
contains_pii = true  (if Person, Email, or Location entity detected)
contains_pii = false (otherwise)
```


### Error handling
- Batches processed in groups of 5
- Each batch retried once on failure skipped if still failing
- Skipped articles remain unmarked in audit table → automatically retried next run
- Per-article embedding failure isolation — one failure does not stop the batch

### Flow
```
raw-landing/ → Event Grid → fn_trigger_nlp → Language API + OpenAI → silver/articles/
```

### Sample Input / Output
- `layer2-nlp-enrichment/ssample_input.json` — raw article from NewsAPI
- `layer2-nlp-enrichment/sample_output.json` — enriched article after NLP processing

---

## Layer 3 — Gold Layer (Analytics)

### Objective
Generate business-ready analytics from the Silver layer using Databricks.

### What was built
- **Azure Databricks** notebook (`gold_layer_pipeline.ipynb`)
- **Azure Data Factory** nightly pipeline (`nlp-nightly-pipeline`) triggers Databricks via REST API at midnight

### Analytics computed

**Step 1 — Read Silver data into Databricks**
```python
df = spark.read.format("json").load("abfss://silver@...")
```

**Step 2 — Category mapping from raw filenames**
Raw filenames already contain the category:
```
-tech.json     → Technology
-business.json → Business
-science.json  → Science
-health.json   → Health
```
Generated `url_hash + category`, joined with Silver on `url_hash` to get `raw_category` per article.

**Step 3 — Sentiment Trends by Category**
Grouped by `raw_category` + `date`, computed average positive sentiment score:
```
Category    | Date       | Avg Positive Score
Technology  | 2026-06-20 | 0.87
Business    | 2026-06-20 | 0.72
Health      | 2026-06-18 | 0.034
Science     | 2026-06-19 | 0.056
```

**Step 4 — Top Entities per Week**
Exploded entity array, grouped by `raw_category` + `week` + `entity`:
```
Category | Week | Entity       | Count
Business | 25   | AI:Skill     | 326
Business | 25   | Amazon       | 110
```

**Step 5 — Trending Keywords**
Exploded keyphrases, grouped by `raw_category` + `keyword` (7-day window):
```
AI, Prime Day, OpenAI, SpaceX
```

**Step 6 — Write to Gold Delta tables with MERGE upsert**
```
gold/sentiment_trends/
gold/top_entities/
gold/trending_keywords/
```
MERGE used instead of overwrite — prevents duplicate rows if pipeline re-runs on same day.

### ADF Pipeline
```
Web Activity → trigger Databricks job (REST API: /api/2.1/jobs/run-now)
Schedule trigger → every day at midnight (00:00)
```

### Flow
```
silver/ → Databricks Notebook → gold/ Delta tables → ADF schedule trigger
```

### Sample Output
- `layer3-batch-orchestration/outputs/

---
## Layer 4 — Azure AI Search

### What was built
- Azure AI Search resource (Free tier F)
- Hybrid search index with:
  - BM25 keyword search on title, description, content, entities, keyphrases
  - HNSW vector search on 1536-dim embeddings (text-embedding-ada-002)
  - Semantic ranker using nlp-semantic-config
- Indexer reads from silver/articles/ container
- JSON array parsing mode — matches enriched article file format

### Index fields
| Field | Type | Searchable | Purpose |
|---|---|---|---|
| id (url_hash) | String | No | Unique key, dedup |
| title | String | Yes (BM25) | Main search field |
| description | String | Yes (BM25) | Secondary search |
| category | String | No | Filter/facet |
| sentiment | String | No | Filter |
| entities | Collection | Yes (BM25) | Entity search |
| keyphrases | Collection | Yes (BM25) | Keyword search |
| embedding | Vector | Yes (HNSW) | Semantic search |
| contains_pii | Boolean | No | Governance filter |

---

## Layer 5 — API Serving

### Objective

Expose NLP-enriched news articles through a lightweight REST API that enables external applications to perform keyword and semantic search.

### What was built

- Azure Function (`fn_search`) with HTTP Trigger
- Integrated with Azure AI Search
- Accepts query parameter:

```
GET /api/v1/search?q=technology
```

- Returns structured JSON responses
- Supports full-text search over indexed news articles

### API Flow

```
Client / Browser
        │
HTTP GET /api/v1/search?q=technology
        │
        ▼
Azure Function (fn_search)
        │
        ▼
Azure AI Search
        │
        ▼
Search Index (nlp-articles)
        │
        ▼
JSON Response
```

### Sample Request

```
GET /api/v1/search?q=technology
```

### Sample Response

```json
{
  "query": "technology",
  "count": 5,
  "results": [
    {
      "title": "AI transforms healthcare",
      "category": "Technology",
      "sentiment": "positive"
    }
  ]
}
```
### Design Decisions

- Azure Functions chosen for serverless API hosting
- Stateless HTTP endpoint enables automatic scaling
- Azure AI Search performs keyword and semantic retrieval
- Function-level authorization used for development
- Azure API Management / Azure AD Easy Auth can be added for OAuth authentication, response caching, and rate limiting in production

---



## Layer 6 — Governance & Lineage

### Objective

Provide end-to-end data lineage, governance, and privacy-aware processing across the NLP pipeline.

### Architecture

```
NewsAPI
    │
    ▼
Logic Apps
    │
    ▼
Raw Layer (ADLS Gen2)
    │
    ▼
Azure Functions
    │
    ▼
Azure AI Language + Azure OpenAI
    │
    ▼
Silver Layer (ADLS Gen2)
    │
    ▼
Azure Databricks
    │
    ▼
Gold Layer (Delta Tables)
    │
    ▼
Azure AI Search
    │
    ▼
Search API
```

### Governance Features

- Medallion Architecture (Raw → Silver → Gold)
- Duplicate detection using `url_hash`
- PII detection during NLP enrichment
- Immutable Raw storage for auditability
- Delta tables for reliable analytics
- Documented end-to-end lineage


### Lineage

```
NewsAPI
   │
Logic Apps
   │
Raw Layer
   │
Azure Functions
   │
Silver Layer
   │
Databricks
   │
Gold Layer
   │
Azure AI Search
   │
Search API
```

### Design Decisions

- Medallion Architecture separates raw, enriched, and analytical datasets
- `url_hash` ensures idempotent processing and duplicate elimination
- PII detection is integrated into the enrichment stage instead of post-processing
- Governance is implemented through documented lineage and metadata
- The architecture is fully compatible with Microsoft Purview for future enterprise deployment without requiring additional code changes

---

## How to Run

### Prerequisites
```
- Azure for Students account ($100 free credit)
- NewsAPI free account (newsapi.org) — 100 requests/day
- Python 3.10 or 3.11
- VS Code + Azure Functions extension
- Azure Functions Core Tools v4
- Azure CLI
```

### Environment variables
Copy `config/sample.env` and fill in your values:
```
NEWSAPI_KEY=your_newsapi_key
STORAGE_CONNECTION_STRING=your_storage_connection_string
LANG_ENDPOINT=https://your-language-resource.cognitiveservices.azure.com
LANG_KEY=your_language_key
OPENAI_ENDPOINT=https://your-openai-resource.openai.azure.com
OPENAI_KEY=your_openai_key
OPENAI_DEPLOYMENT=text-embedding-ada-002
SEARCH_ENDPOINT=https://your-search-resource.search.windows.net
SEARCH_KEY=your_search_admin_key
DATABRICKS_TOKEN=your_databricks_token
DATABRICKS_JOB_ID=your_job_id
```

### Layer 1 — Deploy Logic Apps
```
1. Azure Portal → Logic Apps → Create (Consumption plan)
2. Import layer1-ingestion/logic_app_definition.json
3. Update NewsAPI key in HTTP action
4. Enable the Logic App → Run Trigger to test
```

### Layer 2 — Deploy Azure Functions
```
1. Azure Portal → Function App → Create (Python 3.11, Consumption)
2. Add environment variables in Portal → Configuration → Application settings
3. cd layer2-enrichment
4. func azure functionapp publish YOUR_FUNCTION_APP_NAME
```

### Layer 3 — Run Databricks notebook
```
1. Azure Portal → Azure Databricks → Create workspace
2. Create Single Node cluster (Standard_DS3_v2, auto-terminate 15 min)
3. Import layer3-orchestration/gold_layer_pipeline.ipynb
4. Update storage_account_name and storage_account_key in Cell 1
5. Run All cells
6. Verify gold/ Delta tables written to ADLS Gen2
7. Create ADF pipeline → import layer3-orchestration/adf_pipeline.json
```

---
### Layer 4 — Azure AI Search

1. Install: pip install azure-search-documents azure-core
2. Get Search admin key from Portal → vinodini-nlp-search → Keys
3. Get Storage connection string from Portal → vinodininlpstorage → Access keys
4. Fill in values in create_index.py
5. Run: python layer4-search/create_index.py

 ### Layer 5 — API Serving  
#### How to Run

```
pip install -r requirements.txt
func start
```

Open

```
http://localhost:7071/api/v1/search?q=technology
```

or deploy to Azure Functions and access

```
https://<function-app>.azurewebsites.net/api/v1/search?q=technology
```



---




---

# Design Decisions

## Why Event Grid instead of Event Hub?

The pipeline is event-driven and processes files whenever a new blob is created in Azure Data Lake Storage Gen2. Azure Event Grid provides lightweight, low-latency notifications and integrates directly with Azure Functions. Event Hub is designed for high-throughput streaming scenarios and would introduce unnecessary complexity for periodic NewsAPI ingestion.

---

## Why process Azure AI Language requests in batches?

Azure AI Language Service supports processing multiple documents in a single request. Batch processing reduces API calls, improves throughput, and optimizes overall pipeline performance while remaining within service limits.

---

## Why use `url_hash` for deduplication?

NewsAPI may return the same article across multiple executions or categories. A stable MD5 hash generated from the article URL provides a unique identifier that allows duplicate articles to be detected and skipped without comparing the entire document content.

```
url_hash = MD5(article.url)
```

This makes the pipeline idempotent and prevents duplicate processing.

---

## Why Medallion Architecture (Raw → Silver → Gold)?

The Medallion Architecture separates data according to its processing stage.

**Raw Layer**

- Stores original NewsAPI responses
- Preserves immutable source data

**Silver Layer**

- Stores NLP-enriched articles
- Improves data quality and consistency

**Gold Layer**

- Stores business-ready analytical datasets
- Optimized for reporting and downstream applications

This architecture improves maintainability, governance, and traceability.

---

## Why Azure Functions?

Azure Functions provide a serverless execution model with automatic scaling and pay-per-execution pricing.

Benefits include:

- Event-driven execution
- Low operational overhead
- Automatic scaling
- Easy integration with Event Grid and Azure AI services

---

## Why Azure AI Language Service?

Azure AI Language Service provides pre-trained NLP capabilities without requiring custom model training.

Features used:

- Sentiment Analysis
- Named Entity Recognition
- Key Phrase Extraction
- PII Detection

This enables rapid implementation while maintaining enterprise-grade accuracy.

---

## Why generate vector embeddings?

Vector embeddings convert article content into numerical representations that capture semantic meaning.

Benefits:

- Semantic Search
- Hybrid Search
- Similarity Matching
- Future Machine Learning applications

Embeddings make the search system more intelligent than traditional keyword-based retrieval.

---

## Why Azure Databricks?

Azure Databricks provides distributed data processing using Apache Spark and Delta Lake.

It enables:

- Large-scale transformations
- Efficient aggregations
- Incremental processing
- Reliable analytics pipelines

---

## Why MERGE instead of overwrite for Gold tables?

Delta Lake MERGE operations update existing records instead of replacing the entire dataset.

Advantages:

- Prevents duplicate rows
- Supports incremental updates
- Safe pipeline re-execution
- Idempotent data processing

---

## Why Azure AI Search?

Azure AI Search combines keyword search with semantic ranking and vector capabilities.

The implementation supports:

- Full-text search
- Semantic ranking
- Metadata filtering
- Vector-ready architecture

making it suitable for intelligent retrieval of NLP-enriched articles.

---

## Why Azure Data Factory?

Azure Data Factory orchestrates the nightly analytics workflow.

Responsibilities include:

- Triggering Databricks notebooks
- Managing execution order
- Scheduling batch jobs
- Automating end-to-end processing

---

## Why Azure Logic Apps?

Azure Logic Apps provide a low-code integration platform for periodic data ingestion.

They simplify:

- HTTP API integration
- Scheduled execution
- Workflow automation
- Cloud-native orchestration

without requiring custom infrastructure.

---

## Why document governance instead of fully implementing Microsoft Purview?

The project follows Purview-compatible governance principles while remaining compatible with Azure Student resources.

Governance implemented:

- Medallion Architecture
- End-to-end documented lineage
- PII detection metadata
- Immutable Raw storage
- Data traceability

The architecture can be directly integrated with Microsoft Purview Data Map and sensitivity labels in an enterprise environment without requiring pipeline changes.

---


## Security & Privacy

The pipeline follows security best practices:

- Environment variables for secrets
- No credentials stored in source code
- Duplicate detection using `url_hash`
- PII identification using Azure AI Language
- Layered storage architecture for controlled data access

---

## Future Enhancements

- Azure API Management for OAuth authentication and rate limiting
- Microsoft Purview Data Map integration
- Automated sensitivity labels
- Hybrid vector + semantic search
- MLflow model version tracking
- Real-time streaming analytics
- Power BI dashboards
- CI/CD deployment using GitHub Actions

---

## Infrastructure

All Azure resources were provisioned via Azure Portal. Exported ARM templates are included under `infra/exported-templates/` for reproducibility.

Resources created:
- Azure Storage Account (ADLS Gen2) — `vinodininlpstorage`
- Azure Key Vault — `nlp-keyvault`
- Azure Logic Apps (×4) — one per news category
- Azure Functions App — `nlp-functions` (Python 3.11, Consumption)
- Azure Cognitive Services Language — F0 free tier
- Azure OpenAI — text-embedding-ada-002
- Azure Databricks — Standard tier, single node
- Azure Data Factory — `nlp-adf`
- Azure AI Search — Free tier (F)

