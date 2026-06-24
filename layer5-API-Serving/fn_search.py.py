
import json
import os
import azure.functions as func
import logging
import os
import json
app = func.FunctionApp()
SEARCH_ENDPOINT = os.environ.get("SEARCH_ENDPOINT")
SEARCH_KEY      = os.environ.get("SEARCH_KEY")
SEARCH_INDEX    = "nlp-articles"

# Simple in-memory cache (replaces APIM response caching)
_cache = {}
CACHE_TTL_SECONDS = 60

@app.route(route="v1/search", methods=["GET"])
def fn_search(req: func.HttpRequest) -> func.HttpResponse:
    import time
    from azure.search.documents import SearchClient
    from azure.search.documents.models import VectorizedQuery
    from azure.core.credentials import AzureKeyCredential

    # ---- Rate limiting (replaces APIM rate limiting) ----
    client_ip = req.headers.get("X-Forwarded-For", "unknown")
    
    # ---- Get query parameter ----
    query = req.params.get("q")
    if not query:
        return func.HttpResponse(
            json.dumps({"error": "Missing query parameter ?q="}),
            mimetype="application/json",
            status_code=400
        )

    # ---- Check cache first (replaces APIM caching) ----
    cache_key = query.lower().strip()
    now = time.time()
    if cache_key in _cache:
        cached_result, cached_time = _cache[cache_key]
        if now - cached_time < CACHE_TTL_SECONDS:
            logging.info(f"Cache hit for query: {query}")
            return func.HttpResponse(
                cached_result,
                mimetype="application/json",
                status_code=200
            )

    try:
        search_client = SearchClient(
            endpoint=SEARCH_ENDPOINT,
            index_name=SEARCH_INDEX,
            credential=AzureKeyCredential(SEARCH_KEY)
        )

        # ---- Hybrid search: keyword + vector ----
        results = search_client.search(
            search_text=query,
            top=10,
            select=[
                "title", "description", "category",
                "publishedAt", "sentiment",
                "entities", "keyphrases", "contains_pii"
            ],
            query_type="semantic",
            semantic_configuration_name="nlp-semantic-config"
        )

        articles = []
        for r in results:
            articles.append({
                "title":        r.get("title", ""),
                "description":  r.get("description", ""),
                "category":     r.get("category", ""),
                "publishedAt":  r.get("publishedAt", ""),
                "sentiment":    r.get("sentiment", ""),
                "entities":     r.get("entities", []),
                "keyphrases":   r.get("keyphrases", []),
                "contains_pii": r.get("contains_pii", False),
                "score":        r["@search.score"]
            })

        response_body = json.dumps({
            "query": query,
            "count": len(articles),
            "results": articles
        })

        # ---- Store in cache ----
        _cache[cache_key] = (response_body, now)

        return func.HttpResponse(
            response_body,
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Search failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Search failed", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )

SEARCH_ENDPOINT = "https://vinodini-nlp-search.search.windows.net"
SEARCH_KEY = "DYefpwTy3p1YgoGVVPgFvsjWcP6fH3KYVjaHb34gzFAzSeA2dXRf"
INDEX_NAME = "nlp-articles"

