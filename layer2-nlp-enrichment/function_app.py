import azure.functions as func
import logging
import json
import hashlib
import os
import time
from datetime import datetime, timezone

import requests
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient

app = func.FunctionApp()

# ---------- Shared config ----------
LANG_ENDPOINT = os.environ.get("LANG_ENDPOINT")
LANG_KEY = os.environ.get("LANG_KEY")
OPENAI_ENDPOINT = os.environ.get("OPENAI_ENDPOINT")
OPENAI_KEY = os.environ.get("OPENAI_KEY")
OPENAI_DEPLOYMENT = os.environ.get("OPENAI_DEPLOYMENT", "text-embedding-ada-002")
STORAGE_CONN_STR = os.environ.get("STORAGE_CONNECTION_STRING")

AUDIT_TABLE_NAME = "AuditLog"
MAX_RETRIES = 1  
@app.event_grid_trigger(arg_name="azeventgrid")
def fnTriggerNlp(azeventgrid: func.EventGridEvent):
    event_data = azeventgrid.get_json()
    blob_url = event_data.get("url")
    logging.info(f"fn-trigger-nlp triggered for blob: {blob_url}")

    if not blob_url or "raw-landing" not in blob_url:
        logging.info("Not a raw-landing blob, skipping.")
        return

    articles = read_blob_as_json(blob_url)
    if not articles:
        logging.warning("No articles found in blob, skipping.")
        return

    table_client = get_table_client()
    new_articles = []
    for article in articles:
        url_hash = hashlib.md5(article["url"].encode()).hexdigest()
        article["url_hash"] = url_hash
        if not already_processed(table_client, url_hash):
            new_articles.append(article)

    if not new_articles:
        logging.info("All articles already processed. Nothing to enrich.")
        return

    logging.info(f"Enriching {len(new_articles)} new articles.")

    enriched_articles = []
    skipped_count = 0
    batch_size = 5

    for i in range(0, len(new_articles), batch_size):
        batch = new_articles[i:i + batch_size]
        batch_num = (i // batch_size) + 1

       
        sentiment_results = safe_call_batch(call_sentiment_api, batch, "sentiment", batch_num)
        entity_results = safe_call_batch(call_entity_api, batch, "entities", batch_num)
        keyphrase_results = safe_call_batch(call_keyphrase_api, batch, "key phrases", batch_num)

        
        if sentiment_results is None or entity_results is None or keyphrase_results is None:
            logging.error(
                f"Batch {batch_num} failed after retry — skipping {len(batch)} articles. "
                f"They remain unmarked and will be retried next run."
            )
            skipped_count += len(batch)
            continue

        for idx, article in enumerate(batch):
            try:
                article["sentiment"] = sentiment_results[idx]["sentiment"]
                article["sentiment_scores"] = sentiment_results[idx]["confidenceScores"]
                article["entities"] = entity_results[idx]
                article["keyphrases"] = keyphrase_results[idx]
                article["contains_pii"] = check_pii(entity_results[idx])
                enriched_articles.append(article)
            except (KeyError, IndexError) as e:
                logging.warning(
                    f"Could not merge enrichment results for article '{article.get('title','?')}': {e}. Skipping this article."
                )
                skipped_count += 1

    if not enriched_articles:
        logging.warning("No articles were successfully enriched in this run.")
        return

    
    final_articles = []
    for article in enriched_articles:
        text = f"{article.get('title','')} {article.get('description','')}"
        embedding = safe_get_embedding(text, article.get("title", "untitled"))
        if embedding is None:
            logging.warning(f"Embedding failed for '{article.get('title','?')}', skipping this article.")
            skipped_count += 1
            continue
        article["embedding"] = embedding
        final_articles.append(article)

    if not final_articles:
        logging.warning("No articles survived the embedding step.")
        return

    write_to_silver(final_articles)

    for article in final_articles:
        mark_processed(table_client, article["url_hash"])

    logging.info(
        f"fn-trigger-nlp finished. {len(final_articles)} articles enriched and saved. "
        f"{skipped_count} articles skipped due to errors (will retry next run)."
    )


def safe_call_batch(api_func, batch, label, batch_num):
    """Calls an API function with one retry. Returns None if it still fails."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            return api_func(batch)
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"Batch {batch_num} — {label} API call failed (attempt {attempt + 1}/{MAX_RETRIES + 1}): {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(2)  # brief backoff before retry
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logging.error(f"Batch {batch_num} — {label} API returned unexpected format: {e}")
            return None
    return None


def safe_get_embedding(text, title_for_log):
    """Wraps embedding call with try/except, returns None on failure."""
    try:
        return get_embedding(text)
    except requests.exceptions.RequestException as e:
        logging.warning(f"Embedding API call failed for '{title_for_log}': {e}")
        return None
    except (KeyError, IndexError) as e:
        logging.warning(f"Embedding API returned unexpected format for '{title_for_log}': {e}")
        return None


def read_blob_as_json(blob_url: str):
    try:
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        parts = blob_url.split("/")
        container_name = parts[3]
        blob_name = "/".join(parts[4:])
        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
        data = blob_client.download_blob().readall()
        payload = json.loads(data)
        return payload.get("articles", payload if isinstance(payload, list) else [])
    except Exception as e:
        logging.error(f"Failed to read blob {blob_url}: {e}")
        return []


def get_table_client():
    table_service = TableServiceClient.from_connection_string(STORAGE_CONN_STR)
    try:
        table_service.create_table(AUDIT_TABLE_NAME)
    except Exception:
        pass
    return table_service.get_table_client(AUDIT_TABLE_NAME)


def already_processed(table_client, url_hash: str) -> bool:
    try:
        table_client.get_entity(partition_key="article", row_key=url_hash)
        return True
    except Exception:
        return False


def mark_processed(table_client, url_hash: str):
    entity = {
        "PartitionKey": "article",
        "RowKey": url_hash,
        "status": "enriched",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        table_client.upsert_entity(entity)
    except Exception as e:
        logging.error(f"Failed to mark {url_hash} as processed: {e}")


def call_sentiment_api(batch):
    headers = {"Ocp-Apim-Subscription-Key": LANG_KEY, "Content-Type": "application/json"}
    body = {
    "documents": [
        {
            "id": str(i),
            "language": "en",
             "text": ((a.get("title") or "") + " " + (a.get("description") or "")
).strip()[:5000]
        }
        for i, a in enumerate(batch)
    ]
}
    resp = requests.post(f"{LANG_ENDPOINT.rstrip('/')}/text/analytics/v3.1/sentiment", headers=headers, json=body, timeout=15)
    if resp.status_code != 200:
        logging.error(f"Status: {resp.status_code}")
        logging.error(resp.text)
        resp.raise_for_status()
    return resp.json()["documents"]


def call_entity_api(batch):
    headers = {"Ocp-Apim-Subscription-Key": LANG_KEY, "Content-Type": "application/json"}
    body = {
    "documents": [
        {
            "id": str(i),
            "language": "en",
            "text": (
    (a.get("title") or "") + " " + (a.get("description") or "")
).strip()[:5000]
        }
        for i, a in enumerate(batch)
    ]
}
    resp = requests.post(
        f"{LANG_ENDPOINT.rstrip('/')}/text/analytics/v3.1/entities/recognition/general",
        headers=headers, json=body, timeout=15
    )
    if resp.status_code != 200:
        logging.error(f"Status: {resp.status_code}")
        logging.error(resp.text)
        resp.raise_for_status()
    docs = resp.json()["documents"]
    return [[e["text"] + ":" + e["category"] for e in d["entities"]] for d in docs]


def call_keyphrase_api(batch):
    headers = {"Ocp-Apim-Subscription-Key": LANG_KEY, "Content-Type": "application/json"}
    body = {
    "documents": [
        {
            "id": str(i),
            "language": "en",
            "text": (
    (a.get("title") or "") + " " + (a.get("description") or "")
).strip()[:5000]
        }
        for i, a in enumerate(batch)
    ]
}
    resp = requests.post(f"{LANG_ENDPOINT.rstrip('/')}/text/analytics/v3.1/keyPhrases", headers=headers, json=body, timeout=15)
    if resp.status_code != 200:
        logging.error(f"Status: {resp.status_code}")
        logging.error(resp.text)
        resp.raise_for_status()
    docs = resp.json()["documents"]
    return [d["keyPhrases"] for d in docs]


def check_pii(entities):
    pii_types = ("Person", "Email", "Location")
    return any(any(t in e for t in pii_types) for e in entities)


def get_embedding(text: str):
    headers = {
        "api-key": OPENAI_KEY,
        "Content-Type": "application/json"
    }

    endpoint = OPENAI_ENDPOINT.rstrip("/")

    url = (
        f"{endpoint}/openai/deployments/"
        f"{OPENAI_DEPLOYMENT}/embeddings"
        f"?api-version=2024-02-01"
    )

    response = requests.post(
        url,
        headers=headers,
        json={"input": text},
        timeout=30,
    )

    response.raise_for_status()

    return response.json()["data"][0]["embedding"]


def write_to_silver(enriched_articles):
    try:
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        blob_name = f"articles/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.json"
        blob_client = blob_service.get_blob_client(container="silver", blob=blob_name)
        blob_client.upload_blob(json.dumps(enriched_articles), overwrite=True)
        logging.info(f"Wrote {len(enriched_articles)} enriched articles to silver/{blob_name}")
    except Exception as e:
        logging.error(f"Failed to write to silver layer: {e}")


@app.event_grid_trigger(arg_name="azeventgrid")
def fnAuditLog(azeventgrid: func.EventGridEvent):
    try:
        event_data = azeventgrid.get_json()
        blob_url = event_data.get("url")
        logging.info(f"fn-audit-log triggered for blob: {blob_url}")

        log_entry = {
            "blob_url": blob_url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "received",
        }

        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        log_blob_name = f"logs/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{hashlib.md5(blob_url.encode()).hexdigest()[:8]}.json"
        blob_client = blob_service.get_blob_client(container="audit-logs", blob=log_blob_name)
        blob_client.upload_blob(json.dumps(log_entry), overwrite=True)

        logging.info(f"Audit log written: {log_blob_name}")
    except Exception as e:
        logging.error(f"fn-audit-log failed: {e}")