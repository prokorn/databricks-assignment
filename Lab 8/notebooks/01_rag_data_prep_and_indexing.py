# Databricks notebook source
# DBTITLE 1,Notebook Title & Overview
# MAGIC %md
# MAGIC # RAG Data Preparation & Indexing
# MAGIC
# MAGIC This notebook prepares a small movie knowledge corpus for Retrieval-Augmented Generation (RAG). It walks through four stages:
# MAGIC
# MAGIC 1. **Load raw documents** — Define a set of movie-related texts with metadata.
# MAGIC 2. **Chunk texts** — Split each document into smaller, overlapping chunks using a recursive character splitter.
# MAGIC 3. **Generate embeddings** — Produce dense vector embeddings for each chunk using a SentenceTransformer model.
# MAGIC 4. **Persist to Delta** — Save the chunks and embeddings to a Unity Catalog Delta table with Change Data Feed enabled.

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC ## Step 1: Load Raw Documents
# MAGIC
# MAGIC Define the catalog/schema context and create a small in-memory corpus of movie-related documents. Each document includes a title, category, and raw text that will be processed downstream.

# COMMAND ----------

# DBTITLE 1,Load Raw Documents
# --- Unity Catalog target configuration ---
CATALOG = "main"
SCHEMA = "lab_data"

# Set the active catalog/schema so downstream table operations resolve correctly
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# --- Define the raw knowledge corpus ---
# Each document contains a movie title, a category label, and the full raw text.
# This in-memory corpus simulates an internal knowledge base that a RAG system
# would retrieve from at query time.
raw_documents = [
    {
        "movie_title": "Inception",
        "category": "Production & Plot",
        "raw_text": """Inception is a 2010 science fiction film written and directed by Christopher Nolan. 
The plot follows Dom Cobb, a professional thief who steals information by infiltrating the subconscious of his targets through dream-sharing technology. 
Cobb is offered a chance to have his criminal history erased as payment for the implantation of another person's idea into a target's subconscious: a process known as inception. 
The ending remains ambiguous: Cobb spins his totem (a pewter spinning top) to test whether he is in reality or still dreaming. 
Before the top's motion resolves, the screen cuts to black. However, director Nolan revealed in interviews that the presence of Michael Caine's character indicates reality, as Miles only exists in the waking world."""
    },
    {
        "movie_title": "The Dark Knight",
        "category": "Trivia & Cast",
        "raw_text": """The Dark Knight is a 2008 superhero film directed by Christopher Nolan, starring Christian Bale as Bruce Wayne / Batman. 
Heath Ledger posthumously won the Academy Award for Best Supporting Actor for his portrayal of the Joker. 
During the iconic hospital explosion scene, the detonation was delayed momentarily; Ledger remained in character, playing with the detonator until the pyrotechnics resumed. 
The film set a standard for IMAX camera usage in commercial Hollywood blockbusters, shooting 28 minutes of footage with 65mm IMAX cameras."""
    },
    {
        "movie_title": "Interstellar",
        "category": "Scientific Accuracy & Visuals",
        "raw_text": """Interstellar (2014), directed by Christopher Nolan, explores humanity's search for a habitable planet via a wormhole near Saturn. 
Theoretical physicist Kip Thorne served as scientific consultant and executive producer. Thorne provided theoretical equations to the visual effects team at Double Negative (DNEG). 
The simulation of the supermassive black hole Gargantua produced groundbreaking scientific insights into gravitational lensing and accretion disks. 
The time dilation on Miller's planet is extreme: 1 hour spent on the surface equals 7 earth years due to the intense gravitational field."""
    },
    {
        "movie_title": "Avatar",
        "category": "Technology & Box Office",
        "raw_text": """Avatar (2009), written and directed by James Cameron, became the highest-grossing film of all time, surpassing $2.9 billion worldwide. 
The movie pioneered stereoscopic 3D cameras and fusion camera systems developed by Cameron and Vince Pace. 
It heavily relied on photorealistic performance capture technology to portray the Na'vi people and the alien ecosystem of Pandora. 
Cameron waited over a decade to produce the film until computer-generated imagery advanced sufficiently to realize his visual concept."""
    }
]

print(f"Loaded {len(raw_documents)} source knowledge documents.")

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC ## Step 2: Chunk Texts
# MAGIC
# MAGIC Split each raw document into smaller, overlapping text chunks using LangChain's `RecursiveCharacterTextSplitter`. Chunking improves retrieval granularity — each chunk is a self-contained piece of information that can be independently embedded and searched.

# COMMAND ----------

# DBTITLE 1,Chunk Texts
import uuid
from langchain_text_splitters import RecursiveCharacterTextSplitter

# --- Initialize the recursive text splitter ---
# chunk_size=300:  maximum characters per chunk
# chunk_overlap=50: overlap between adjacent chunks to preserve context across boundaries
# separators:      ordered list of split points, tried from most to least specific
#                   (paragraph break → line break → sentence → word)
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=300,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " "]
)

# --- Split each document into chunks and attach metadata ---
# Every chunk inherits its parent document's title and category,
# gets a unique ID (for deduplication / upserts later), and a
# sequential index indicating its position within the source document.
processed_chunks = []

for doc in raw_documents:
    splits = text_splitter.split_text(doc["raw_text"])
    for idx, chunk in enumerate(splits):
        processed_chunks.append({
            "chunk_id": str(uuid.uuid4()),
            "movie_title": doc["movie_title"],
            "category": doc["category"],
            "chunk_index": idx,
            "content": chunk.strip(),
            "char_count": len(chunk.strip())
        })

print(f"Generated {len(processed_chunks)} granular chunks from raw documents.")

# COMMAND ----------

# DBTITLE 1,Step 3 Header
# MAGIC %md
# MAGIC ## Step 3: Generate Dense Embeddings
# MAGIC
# MAGIC Use the `all-MiniLM-L6-v2` SentenceTransformer model to convert each text chunk into a dense vector embedding. These vectors capture semantic meaning and will be used for similarity-based retrieval at query time.

# COMMAND ----------

# DBTITLE 1,Generate Embeddings
from sentence_transformers import SentenceTransformer

# --- Load the embedding model ---
# all-MiniLM-L6-v2 is a lightweight, fast model that produces 384-dimensional
# dense vectors. It is well-suited for semantic similarity and retrieval tasks.
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Encode all chunk texts into dense vectors ---
# Batch-encode all chunks at once for efficiency, then attach each vector
# back to its corresponding chunk as a list of floats (JSON-serializable).
chunk_texts = [chunk["content"] for chunk in processed_chunks]
embeddings = embedding_model.encode(chunk_texts, show_progress_bar=True)

# Attach embedding vectors to chunk metadata
for i, chunk in enumerate(processed_chunks):
    chunk["embedding"] = embeddings[i].tolist()

print(f"Generated {len(embeddings)} dense embedding vectors (dim={len(embeddings[0])}).")

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC ## Step 4: Persist Chunks & Embeddings to Delta
# MAGIC
# MAGIC Write the processed chunks and their embedding vectors to a Unity Catalog Delta table with Change Data Feed enabled. This table serves as the searchable knowledge base for downstream RAG retrieval.

# COMMAND ----------

# DBTITLE 1,Persist to Delta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType

# --- Define the schema for the chunks table ---
# The embedding column is an array of floats matching the model's output dimension (384).
schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("movie_title", StringType(), False),
    StructField("category", StringType(), True),
    StructField("chunk_index", IntegerType(), True),
    StructField("content", StringType(), False),
    StructField("char_count", IntegerType(), True),
    StructField("embedding", ArrayType(FloatType()), False)
])

df_chunks = spark.createDataFrame(processed_chunks, schema=schema)

# --- Write to Unity Catalog as a Delta table ---
# Change Data Feed (CDF) is enabled so downstream pipelines can track row-level
# changes (inserts/updates/deletes) for incremental processing.
df_chunks.write.format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.movie_knowledge_chunks")

print(f"Successfully written chunks and embeddings to `{CATALOG}.{SCHEMA}.movie_knowledge_chunks`.")

# --- Verify: preview the first 5 rows (embedding column omitted for readability) ---
display(spark.sql(f"SELECT chunk_id, movie_title, category, chunk_index, content, char_count FROM {CATALOG}.{SCHEMA}.movie_knowledge_chunks LIMIT 5"))

# COMMAND ----------

