FROM pytorch/pytorch:2.2.0-cuda11.8-cudnn8-runtime

WORKDIR /app

# Instalar Python e pip (se necessário)
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY orchestrator.py .

# Expor porta
EXPOSE 8000

# Comando para rodar a aplicação
CMD ["uvicorn", "orchestrator:app", "--host", "0.0.0.0", "--port", "8000"]
