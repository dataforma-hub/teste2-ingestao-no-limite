# dataforma-hub · Ingestão no Limite (v2)

Pipeline de ingestão dos dados de Empresas da Receita Federal para a competição
Ingestão no Limite, otimizado para
rodar em 2 GB de RAM / 2 CPUs.

Como funciona





Extração — lê os .zip em /data/ e extrai um .EMPRECSV por vez

(streaming, mantendo o uso de disco/RAM baixo).



Transformação (DuckDB) — lê o CSV bruto (ISO-8859-1, separador ;, sem

cabeçalho, todas as colunas como texto), aplica o contrato de dados e os filtros
 B2B, e grava um CSV limpo em UTF-8. O parsing é nativo em C++, multithread e
 out-of-core (não carrega tudo em memória).



Carga (PostgreSQL COPY) — carrega o CSV limpo numa tabela UNLOGGED

public.{participante}_empresas (caminho de carga mais rápido, menos WAL).

Sem dependência de rede em runtime e sem extensões do DuckDB — latin-1 é nativo
no DuckDB ≥ 1.2.

Contrato de dados aplicado







Coluna



Transformação





cnpj_basico



8 dígitos com zeros à esquerda





razao_social



UPPER + TRIM





natureza_juridica



4 dígitos com zeros à esquerda





qualificacao_responsavel



mantido, nunca nulo





capital_social



vírgula BR → ponto, DOUBLE, > 1000.00





porte_codigo



00 / 01 / 03 / 05





porte_descricao



mapeamento oficial do porte





ente_federativo



vazio → NULL

Filtros B2B: mantém apenas capital_social > 1000.00 e remove MEIs cuja
razao_social termina com 11 dígitos (CPF do titular). Isso zera as 8 regras de
Data Quality (DQ-01 a DQ-08).

Variáveis de ambiente (injetadas pelo avaliador)

PARTICIPANTE, PG_TABLE, PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB.

Opcionais de tuning: DUCKDB_MEMORY_LIMIT (padrão 1024MB), DUCKDB_THREADS
(padrão 2).

Rodar localmente

docker build -t ingestao-dataforma .
docker run --rm \
  -e PARTICIPANTE=dataforma-hub_v2 \
  -e PG_HOST=postgres_db -e PG_USER=... -e PG_PASSWORD=... -e PG_DB=db_empresas \
  -v /caminho/para/dados:/data:ro \
  --network <rede-do-postgres> \
  --cpus=2 --memory=2g \
  ingestao-dataforma

