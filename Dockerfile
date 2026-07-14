FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# O avaliador roda apenas `docker run <imagem>`; o CMD dispara toda a ingestão.
CMD ["python", "src/main.py"]
