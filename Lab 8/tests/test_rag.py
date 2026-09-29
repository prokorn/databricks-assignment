"""Unit test for RAG chunking logic."""
from langchain_text_splitters import RecursiveCharacterTextSplitter

def test_rag_chunk_splitter():
    sample_text = (
        "Inception is a 2010 science fiction film directed by Christopher Nolan. "
        "The plot follows Dom Cobb, a thief who steals information by infiltrating dreams."
    )
    splitter = RecursiveCharacterTextSplitter(chunk_size=60, chunk_overlap=10)
    chunks = splitter.split_text(sample_text)
    
    assert len(chunks) > 1, "Text should be split into multiple chunks"
    assert all(len(c) <= 60 for c in chunks), "No chunk should exceed chunk_size limit"
