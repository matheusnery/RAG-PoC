import sys
import os
import json
from fastapi import FastAPI, HTTPException
from sentence_transformers import SentenceTransformer, util
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

openai_client = None
try:
    from openai import OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        openai_client = OpenAI(api_key=api_key)
except ImportError:
    pass

app = FastAPI()

# Load documents from JSON file
def load_documents():
    doc_path = os.path.join(os.path.dirname(__file__), 'documents.json')
    try:
        with open(doc_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: documents.json not found at {doc_path}", file=sys.stderr)
        return []

documents = load_documents()

@app.get("/")
def root():
    return {"status": "ok", "message": "RAG PoC service is running", "documents_loaded": len(documents)}

model = SentenceTransformer('all-MiniLM-L6-v2')
doc_embeddings = model.encode([doc.get('text', doc.get('title', '')) for doc in documents], convert_to_tensor=True)


class QueryRequest (BaseModel):
    query: str


    #def __init__(self, query: str):
    #    self.query = query


@app.post("/query")
def query_rag(request: QueryRequest):
    try:
        query_embedding = model.encode(request.query, convert_to_tensor=True)

        similarities = util.cos_sim(query_embedding, doc_embeddings)[0]
        results = []
        for idx, score in enumerate(similarities):
            results.append({
                "id": documents[idx]["id"],
                "title": documents[idx].get("title", ""),
                "text": documents[idx].get("text", ""),
                "score": float(score)
            })

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:3]
        best_result = results[0] if results else None

        if openai_client is None:
            return {"query": request.query, "results": results, "note": "OpenAI integration not available - returning top 3 relevant documents based on semantic similarity"}
        
        
        prompt = f"""You are a helpful assistant that answers questions based on the following document:

            Document:
            {best_result["text"]}

            Question:
            {request.query}

            Answer:"""

        try:
            res = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            answer = res.choices[0].message.content.strip()
            return {"answer": answer, "source_document": best_result}
        except Exception as e:
            print("OpenAI Error:", repr(e), file=sys.stderr)
            return {"results": results, "error": "OpenAI request failed"}

    except Exception as exc:
        print("ERROR:", repr(exc), file=sys.stderr)
        raise HTTPException(status_code=500, detail=str(exc))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=8000)