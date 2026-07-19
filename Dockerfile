# ==========================================
# Fase 1: Build
# ==========================================
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# Copia os arquivos do projeto
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/

# Compila o projeto em modo release
RUN cargo build --release

# ==========================================
# Fase 2: Runtime
# ==========================================
FROM debian:bookworm-slim

WORKDIR /app

# Atualiza pacotes e instala certificados, se necessário, além de limpar o cache
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copia o binário compilado do estágio anterior
COPY --from=builder /app/target/release/teste2-ingestao-no-limite /usr/local/bin/ingestao

# O avaliador roda apenas `docker run <imagem>`; o CMD dispara a ingestão
CMD ["ingestao"]
