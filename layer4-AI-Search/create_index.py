from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndex, SearchField, SearchFieldDataType,
    SimpleField, SearchableField,
    VectorSearch, HnswAlgorithmConfiguration,
    VectorSearchProfile, SemanticConfiguration,
    SemanticSearch, SemanticPrioritizedFields,
    SemanticField, SearchIndexerDataSourceConnection,
    SearchIndexerDataContainer, SearchIndexer, FieldMapping
)
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
import time

SEARCH_ENDPOINT = "https://vinodini-nlp-search.search.windows.net"
SEARCH_KEY      = "searchkey"
INDEX_NAME      = "nlp-articles"
STORAGE_CONN    = "storage key"

credential = AzureKeyCredential(SEARCH_KEY)

# STEP 1: Create index
print("Step 1: Creating index...")
fields = [
    SimpleField(name="url_hash", type=SearchFieldDataType.String,
                key=True, filterable=True),
    SearchableField(name="title",
                    type=SearchFieldDataType.String,
                    analyzer_name="en.microsoft"),
    SearchableField(name="description",
                    type=SearchFieldDataType.String,
                    analyzer_name="en.microsoft"),
    SimpleField(name="category",
                type=SearchFieldDataType.String,
                filterable=True, facetable=True),
    SimpleField(name="publishedAt",
                type=SearchFieldDataType.String,
                filterable=True, sortable=True),
    SimpleField(name="sentiment",
                type=SearchFieldDataType.String,
                filterable=True),
    SearchableField(name="entities",
                    type=SearchFieldDataType.String,
                    collection=True),
    SearchableField(name="keyphrases",
                    type=SearchFieldDataType.String,
                    collection=True),
    SimpleField(name="contains_pii",
                type=SearchFieldDataType.Boolean,
                filterable=True),
    SearchField(
        name="embedding",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
        searchable=True,
        vector_search_dimensions=1536,
        vector_search_profile_name="nlp-vector-profile"
    )
]

vector_search = VectorSearch(
    algorithms=[HnswAlgorithmConfiguration(name="nlp-hnsw")],
    profiles=[VectorSearchProfile(
        name="nlp-vector-profile",
        algorithm_configuration_name="nlp-hnsw"
    )]
)

semantic_config = SemanticConfiguration(
    name="nlp-semantic-config",
    prioritized_fields=SemanticPrioritizedFields(
        content_fields=[SemanticField(field_name="description")],
        keywords_fields=[SemanticField(field_name="keyphrases")]
    )
)

index_client = SearchIndexClient(SEARCH_ENDPOINT, credential)
index = SearchIndex(
    name=INDEX_NAME,
    fields=fields,
    vector_search=vector_search,
    semantic_search=SemanticSearch(configurations=[semantic_config])
)
index_client.create_or_update_index(index)
print(" Index created successfully")

# STEP 2: Create data source
print("Step 2: Creating data source...")
indexer_client = SearchIndexerClient(SEARCH_ENDPOINT, credential)
data_source = SearchIndexerDataSourceConnection(
    name="nlp-silver-datasource",
    type="azureblob",
    connection_string=STORAGE_CONN,
    container=SearchIndexerDataContainer(name="silver",query="articles")
)
indexer_client.create_or_update_data_source_connection(data_source)
print(" Data source created")

# STEP 3: Create indexer
print("Step 3: Creating indexer...")
indexer = SearchIndexer(
    name="nlp-articles-indexer",
    data_source_name="nlp-silver-datasource",
    target_index_name=INDEX_NAME
)
indexer_client.create_or_update_indexer(indexer)
indexer_client.run_indexer("nlp-articles-indexer")
print(" Indexer created and running")

# STEP 4: Wait and test search
print("Step 4: Waiting 15 seconds for indexer...")
time.sleep(15)

search_client = SearchClient(SEARCH_ENDPOINT, INDEX_NAME, credential)
results = search_client.search(
    search_text="technology",
    top=3,
    select=["title", "category", "sentiment"]
)

print("\n Search test results:")
found = False
for r in results:
    found = True
    print(f"  → {r.get('title','no title')} | {r.get('category','?')} | {r.get('sentiment','?')}")

if not found:
    print("  No results yet — indexer may still be running. Check Portal in 2 mins.")