# RAG PoC

This repository contains a FastAPI service that exposes an endpoint for document similarity search using `sentence-transformers` and optional OpenAI completion support.

## Setup

1. Create a `.env` file in the project root with your OpenAI API key:

```env
OPENAI_API_KEY=your_openai_api_key_here
```

2. Start the application with Docker:

```powershell
docker-compose up --build
```

The service will be available at:

- `http://localhost:8000/`
- `http://localhost:8000/query`

## Quick tests

### Health check

```powershell
curl http://localhost:8000/
```

Expected response:

```json
{"status":"ok","message":"RAG PoC service is running","documents_loaded":<number>}
```

### Query similarity

```powershell
curl.exe -X POST "http://localhost:8000/query" -H "Content-Type: application/json" -d '{"query":"Paris"}'
```

The response returns the top 3 matching documents, and when OpenAI is enabled it also returns a generated answer based on the most relevant document.

## Example questions to try

- "What is the most important point in the document about Paris?"
- "Summarize the main idea from the top result."
- "What question does this document answer best?"
- "How is the document content related to local culture or history?"
- "What is the key insight from the most relevant document?"

## Run locally without Docker

If you want to run the app locally, use:

```powershell
python orchestrator.py
```

Make sure the `OPENAI_API_KEY` environment variable is set before running locally, or create a `.env` file in the project root.

## Notes

- `orchestrator.py` loads the `all-MiniLM-L6-v2` model at startup.
- The service uses `.env` for OpenAI key configuration and `.gitignore` prevents the `.env` file from being committed.
- To restart the service, stop the container (`Ctrl+C`) and run `docker-compose up --build` again.
