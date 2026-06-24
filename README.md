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

## Design Decisions

### Why Event Grid over Event Hub for Layer 1→2 trigger?
Event Grid fires on discrete blob-created events — exactly what is needed here. Event Hub is designed for high-throughput continuous streams (millions of events/sec). Since NewsAPI delivers at most 100 articles every 6 hours per category, Event Grid is the correct, cost-effective choice.

### Why batch size of 25 for Language API calls?
Azure AI Language API accepts a maximum of 25 documents per batch request. Processing in batches of 25 maximises throughput while staying within API limits.

### Why MERGE instead of overwrite for gold Delta tables?
If ADF triggers Databricks twice in one day (re-run, failure retry), MERGE updates existing rows instead of creating duplicates. This makes the pipeline idempotent — safe to re-run at any time without corrupting analytics data.

### Why url_hash for deduplication?
The same article can appear in multiple NewsAPI calls (if it stays trending). MD5 of the URL gives a stable, consistent identifier checked against the audit table before enrichment — skipping already-processed articles without reading their content.

### Why Azure AD Easy Auth instead of Azure API Management (Layer 5)?
APIM Developer tier costs ~$50/month — not viable for a student account. Azure AD Easy Auth is free, built into Azure Functions, and provides the same JWT token validation at the platform level before any code executes.

### Why Databricks Free Edition / Azure Databricks over Synapse?
The project document specifically names Databricks. Azure Databricks was used (Standard tier, single-node cluster) to match the exact architecture specified, with auto-termination set to 15 minutes to minimise cost.

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

---

## Repository Structure

```
nlp-pipeline-project/
├── README.md
├── .gitignore
├── config/
│   └── sample.env
├── architecture_diagram.png
├── layer1-ingestion/
│   ├── logic_app_definition.json
│   └── samples/
│       └── sample_raw_response.json
├── layer2-enrichment/
│   ├── function_app.py
│   ├── requirements.txt
│   ├── host.json
│   ├── sample.local.settings.json
│   └── samples/
│       ├── sample_input.json
│       └── sample_output.json
├── layer3-orchestration/
│   ├── gold_layer_pipeline.ipynb
│   ├── adf_pipeline.json
│   └── samples/
│       ├── sample_gold_sentiment.json
│       ├── sample_gold_entities.json
│       ├── sample_gold_keywords.json
│       └── databricks_output_screenshot.png
├── layer4-search/
│   ├── create_index.py
│   ├── search_index_schema.json
│   └── samples/
│       └── sample_search_response.json
├── layer5-api/
│   └── fn_search_code.py
└── layer6-governance/
    └── lineage_diagram.png
```

---

## Candidate

**Name:** Vinodini Bandaru
**Role:** Intern Data Engineer Support
**Submission:** Text NLP Pipeline (Project 2 — Mandatory)
