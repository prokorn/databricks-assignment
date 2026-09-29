# Lab 12: AI Capstone — RAG & AI-Assisted Development

> **Movie Knowledge Assistant** — an end-to-end Retrieval-Augmented Generation (RAG) system built on Databricks Unity Catalog, combined with Databricks Asset Bundle (DAB) deployment and AI coding agent workflows.

---

## 1. Overview & Business Scenario

Organizations require conversational interfaces over unstructured internal knowledge bases without leaking data or incurring unpredictable model hallucinations. 

This Capstone implements a **Movie Knowledge Assistant** answering deep trivia, production details, and scene breakdowns from raw document corpora:
- **Notebook 1:** `01_rag_data_prep_and_indexing.ipynb` — Document ingestion, recursive chunking, dense vector embeddings, and Unity Catalog storage.
- **Notebook 2:** `02_rag_qa_and_evaluation.ipynb` — Top-K cosine similarity retrieval, context-augmented QA prompt assembly, hallucination evaluation, and audit logging.
- **Bundle Asset:** `Lab 8/resources/rag_indexing_job.yml` — Automated DAB task triggering the indexing pipeline.

---

## 2. Part A: RAG Architecture & Implementation

### Data Preparation & Chunking Strategy
- **Recursive Character Splitting:** Text split into chunks of ~300 characters with a 50-character overlap to preserve semantic continuity across sentence boundaries.
- **Metadata Tagging:** Each chunk retains `chunk_id`, `movie_title`, `category`, and `chunk_index` for granular filtering.
- **Vector Embeddings:** Generated 384-dimensional dense vectors using the lightweight, high-performance `all-MiniLM-L6-v2` model.
- **Governed Storage:** Persisted to Delta table `main.lab_data.movie_knowledge_chunks` with Change Data Feed (CDF) enabled.

### Retrieval & Quality Evaluation (RAG vs Blind Prompting)

| Scenario | Prompt Strategy | Behavior & Hallucination Risk |
| :--- | :--- | :--- |
| **Without RAG (Blind)** | Zero-shot generic query to LLM | **High Risk:** The model generalizes or hallucinates scene reasons (e.g., attributing hospital detonation delays to technical failure). |
| **With RAG (Context)** | System prompt injected with Top-K retrieved chunks | **Grounded Truth:** Strict adherence to internal corpus facts (e.g., Heath Ledger remaining in character during unexpected pyrotechnic delay). |

### QA Workflow: End-to-End Question Answering

The RAG question-answering workflow follows three stages:

1. **Semantic Retrieval:** User query is encoded into a dense vector, then matched against stored chunk embeddings using cosine similarity to retrieve the top-K most relevant text chunks.
2. **Context Injection:** Retrieved chunks are injected into the LLM system prompt, grounding the model's response in verifiable facts from the knowledge base.
3. **Answer Generation:** The LLM generates a response based strictly on the provided context, with explicit instructions to say "I don't know" if the answer is not present.

#### Example QA Interactions

**Query 1: Hospital Explosion Scene**

![QA Example 1](screenshots/Lab%2012%20QA%20LLM%201.png)

**Query 2: Movie Production Details**

![QA Example 2](screenshots/Lab%2012%20QA%20LLM%202.png)

**Query 3: Technical Trivia**

![QA Example 3](screenshots/Lab%2012%20QA%20LLM%203.png)

**Query 4: Scientific Accuracy**

![QA Example 4](screenshots/Lab%2012%20QA%20LLM%204.png)

### Monitoring & Governance
All assistant interactions are written to `main.lab_data.rag_audit_log` with user query, retrieved context, and UTC timestamps to ensure compliance, auditing, and observability.

---

## 3. Part B: AI-Assisted Development & CI/CD Deployment

### Databricks AI Dev Kit & Agent Workflow
- **Acceleration:** AI coding assistants (Genie / Dev Kit MCP tools) accelerated boilerplate generation for text splitting, embedding pipelines, and initial schema definitions.
- **Asset Bundle Integration:** Extended the modular DAB configuration (`Lab 8/resources/rag_indexing_job.yml`) to orchestrate RAG indexing via Databricks Jobs.

### Bundle Sync Root Architecture Constraint

During CI/CD integration, we encountered a critical bundle validation error:

```
Error: path /Lab 12/01_rag_data_prep_and_indexing is not contained in sync root path
Name: movies_analytics_bundle
Target: dev
```

**Root Cause:** Databricks Asset Bundles can only reference files **within their sync root directory**. The bundle root is `Lab 8/` (where `databricks.yml` resides), but the RAG notebook originally lived in `Lab 12/`, which is outside the sync boundary. DABs enforce this to ensure all deployed resources are versioned and tracked within a single coherent deployment unit.

**Solution Implemented:**

1. **Created `Lab 8/notebooks/` directory** within the bundle structure to house job-executed notebooks
2. **Copied the RAG indexing notebook** from `Lab 12/` into `Lab 8/notebooks/01_rag_data_prep_and_indexing`
3. **Updated the job task path** in `rag_indexing_job.yml`:
   - **Before:** `notebook_path: ../../Lab 12/01_rag_data_prep_and_indexing`
   - **After:** `notebook_path: ../notebooks/01_rag_data_prep_and_indexing`

This ensures all job-referenced assets are contained within the bundle's deployment scope, allowing `databricks bundle validate --target dev` to pass and enabling proper CI/CD automation via GitHub Actions.

**Key Takeaway:** When building modular DAB projects, always place notebooks, scripts, and configuration files that will be referenced by jobs **inside the bundle root directory structure**. Cross-bundle references are not supported.

### Human-in-the-Loop & Critical Guardrails
While agentic tools speed up scaffolding, **human oversight was essential**:
1. **Catalog Governance:** Verifying table namespace references (`main.lab_data`) and preventing hardcoded paths.
2. **Cost & Latency Optimization:** Selecting a lightweight local embedding model instead of blindly invoking expensive remote foundation models for small batches.
3. **CI/CD Quality Gates:** AI-generated artifacts are never deployed directly. All changes must pass GitHub Actions linting, bundle schema validation (`databricks bundle validate`), and PR review before production deployment.

---

## 4. Architectural Discussion

### Why RAG Beats Prompting Alone
1. **Freshness & Private Knowledge:** Fine-tuning or system prompts cannot reflect real-time enterprise updates without continuous retraining. RAG decouples knowledge updates from model weights.
2. **Elimination of Hallucinations:** Prompting alone forces the LLM to statistically predict missing facts. Injected ground-truth context bounds the reasoning space and enforces verifiable citations.
3. **Traceability:** RAG responses point directly to specific `chunk_id` and document sources, making answers auditable.

### Chunk Size vs. Retrieval Quality Trade-offs
- **Small Chunks (\~100–200 chars):** High semantic precision (vector closely matches the exact question), but risks truncating surrounding context and losing semantic meaning.
- **Large Chunks (>1000 chars):** Rich contextual preservation, but dilutes vector embeddings with multiple topics, increases similarity noise, and inflates LLM token costs.
- **Optimal Balance:** Using 300–500 character chunks with a 50-character sliding overlap maintains continuity across sentence breaks while keeping retrieval focused.

### Governance and Cost for GenAI Workloads
- **Unity Catalog Security:** Securing raw text and embeddings at the table and row level ensures users only retrieve context they are authorized to view.
- **TCO Optimization:** Generating embeddings with a local/in-cluster transformer (`all-MiniLM-L6-v2`) eliminates per-token API costs for vectorization, paying only for the final LLM completion.