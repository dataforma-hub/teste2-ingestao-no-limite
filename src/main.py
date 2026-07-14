"""
Ingestão no Limite — pipeline dataforma-hub (v2).

Estratégia (otimizada para 2 GB RAM / 2 CPUs):
  1. Extrai um .EMPRECSV por vez de cada .zip em /data (streaming, disco limitado).
  2. DuckDB lê o CSV bruto (ISO-8859-1 / latin-1, separador ';', sem cabeçalho),
     aplica TODAS as transformações do contrato + filtros B2B e grava um CSV
     limpo em UTF-8 — parsing nativo, multithread e out-of-core.
  3. PostgreSQL COPY carrega o CSV limpo na tabela final UNLOGGED (carga mais
     rápida, menos WAL).

Sem dependência de rede em runtime e sem extensões do DuckDB: latin-1 é nativo
no DuckDB >= 1.2. Todo o contrato de dados é aplicado de forma que as 8 regras
de Data Quality (DQ-01..DQ-08) retornem 0 erros.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
import zipfile
from pathlib import Path

import duckdb
import psycopg2

# ---------------------------------------------------------------------------
# Configuração (injetada pelo avaliador via variáveis de ambiente)
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/ingestao_work"))

PARTICIPANTE = os.environ["PARTICIPANTE"]
PG_TABLE = os.environ.get("PG_TABLE", f"{PARTICIPANTE}_empresas")

PG_HOST = os.environ.get("PG_HOST", "postgres_db")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_DB = os.environ.get("PG_DB", "db_empresas")

DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1024MB")
DUCKDB_THREADS = os.environ.get("DUCKDB_THREADS", "2")

# Colunas do arquivo EMPRESAS (Receita Federal) — 7 campos, sem cabeçalho.
COLUMNS = (
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_codigo",
    "porte_descricao",
    "ente_federativo",
)


def log(msg: str) -> None:
    print(f"[ingestao] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def qident(name: str) -> str:
    """Identificador PostgreSQL entre aspas (necessário p/ hífen: dataforma-hub)."""
    return '"' + name.replace('"', '""') + '"'


# SELECT que materializa o contrato de dados a partir das 7 colunas cruas (c0..c6).
# Observações por coluna:
#   c0 cnpj_basico            -> exatamente 8 dígitos, zeros à esquerda (DQ-01)
#   c1 razao_social           -> UPPER + TRIM (DQ-02); filtro MEI-CPF (DQ-08)
#   c2 natureza_juridica      -> 4 caracteres, zeros à esquerda (DQ-03)
#   c3 qualificacao_responsavel -> NOT NULL, nunca vazio -> '' (DQ-04)
#   c4 capital_social         -> vírgula BR -> ponto, DOUBLE, > 1000.00 (DQ-05)
#   c5 porte_codigo           -> 00/01/03/05 (DQ-06) + descrição oficial (DQ-07)
#   c6 ente_federativo        -> '' vira NULL
TRANSFORM_SELECT = r"""
SELECT
    right(lpad(coalesce(c0, ''), 8, '0'), 8)                         AS cnpj_basico,
    upper(trim(c1))                                                  AS razao_social,
    right(lpad(coalesce(c2, ''), 4, '0'), 4)                         AS natureza_juridica,
    coalesce(trim(c3), '')                                           AS qualificacao_responsavel,
    try_cast(replace(c4, ',', '.') AS DOUBLE)                        AS capital_social,
    CASE WHEN c5 IN ('00', '01', '03', '05') THEN c5 ELSE '00' END   AS porte_codigo,
    CASE c5
        WHEN '01' THEN 'MICRO EMPRESA'
        WHEN '03' THEN 'EMPRESA DE PEQUENO PORTE'
        WHEN '05' THEN 'DEMAIS'
        ELSE 'NÃO INFORMADO'
    END                                                              AS porte_descricao,
    nullif(trim(c6), '')                                            AS ente_federativo
FROM src
WHERE try_cast(replace(c4, ',', '.') AS DOUBLE) > 1000.00
  AND NOT coalesce(regexp_matches(upper(trim(c1)), '[0-9]{11}$'), false)
"""


def create_table(cur) -> None:
    tbl = f"public.{qident(PG_TABLE)}"
    cur.execute(f"DROP TABLE IF EXISTS {tbl};")
    cur.execute(
        f"""
        CREATE UNLOGGED TABLE {tbl} (
            cnpj_basico               VARCHAR(8),
            razao_social              VARCHAR,
            natureza_juridica         VARCHAR(4),
            qualificacao_responsavel  VARCHAR,
            capital_social            DOUBLE PRECISION,
            porte_codigo              VARCHAR(2),
            porte_descricao           VARCHAR,
            ente_federativo           VARCHAR
        );
        """
    )
    log(f"Tabela recriada: {tbl}")


def transform_file(raw_path: Path, clean_csv: Path) -> None:
    """DuckDB: lê o EMPRECSV bruto, aplica contrato + filtros, grava CSV limpo (UTF-8)."""
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
        con.execute(f"SET threads={DUCKDB_THREADS};")
        con.execute("SET preserve_insertion_order=false;")
        con.execute(f"SET temp_directory='{(WORK_DIR / 'duck_tmp').as_posix()}';")

        raw_sql = raw_path.as_posix().replace("'", "''")
        out_sql = clean_csv.as_posix().replace("'", "''")

        con.execute(
            f"""
            COPY (
                WITH src AS (
                    SELECT * FROM read_csv(
                        '{raw_sql}',
                        delim=';', quote='"', escape='"', header=false,
                        encoding='latin-1', all_varchar=true,
                        column_names=['c0','c1','c2','c3','c4','c5','c6'],
                        null_padding=true, ignore_errors=true
                    )
                )
                {TRANSFORM_SELECT}
            ) TO '{out_sql}'
            (FORMAT CSV, HEADER false, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '');
            """
        )
    finally:
        con.close()


def copy_into_postgres(cur, clean_csv: Path) -> None:
    tbl = f"public.{qident(PG_TABLE)}"
    cols = ", ".join(COLUMNS)
    # FORCE_NOT_NULL mantém strings vazias como '' (não NULL) nas colunas que
    # não podem ser nulas; ente_federativo fica de fora para virar NULL quando vazio.
    force_not_null = (
        "cnpj_basico, razao_social, natureza_juridica, "
        "qualificacao_responsavel, porte_codigo, porte_descricao"
    )
    sql = (
        f"COPY {tbl} ({cols}) FROM STDIN WITH ("
        "FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '', "
        f"FORCE_NOT_NULL ({force_not_null}))"
    )
    with open(clean_csv, "rb") as fh:
        cur.copy_expert(sql, fh)


def iter_empresas_entries(zip_path: Path):
    """Gera (ZipFile, entry_name) para arquivos EMPRESAS dentro do zip."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            upper = name.upper()
            if upper.endswith("/"):
                continue
            if "EMPRE" in upper:  # EMPRECSV (empresas) — ignora ESTABELE/SOCIO/etc.
                yield zf, name


def main() -> int:
    started = time.time()
    log(f"Participante={PARTICIPANTE} | Tabela=public.{PG_TABLE}")
    log(f"Postgres={PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB} | Dados={DATA_DIR}")

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR, ignore_errors=True)
    raw_dir = WORK_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (WORK_DIR / "duck_tmp").mkdir(parents=True, exist_ok=True)

    zips = sorted(DATA_DIR.glob("*.zip"))
    log(f"Arquivos .zip encontrados: {len(zips)}")
    if not zips:
        log("ERRO: nenhum .zip em /data.")
        return 1

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, dbname=PG_DB,
    )
    try:
        conn.set_client_encoding("UTF8")
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET synchronous_commit TO off;")
        create_table(cur)
        conn.commit()

        processed = 0
        for zip_path in zips:
            for zf, entry in iter_empresas_entries(zip_path):
                raw_path = raw_dir / Path(entry).name
                t0 = time.time()
                with zf.open(entry) as src, open(raw_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)

                clean_csv = WORK_DIR / "clean.csv"
                transform_file(raw_path, clean_csv)
                copy_into_postgres(cur, clean_csv)
                conn.commit()

                raw_path.unlink(missing_ok=True)
                clean_csv.unlink(missing_ok=True)
                processed += 1
                log(f"[{zip_path.name}:{entry}] carregado em {time.time() - t0:.1f}s")

        if processed == 0:
            log("ERRO: nenhum arquivo EMPRECSV encontrado dentro dos zips.")
            return 1

        cur.execute(f"SELECT count(*) FROM public.{qident(PG_TABLE)};")
        total = cur.fetchone()[0]
        conn.commit()
        log(f"Concluído: {processed} arquivo(s), {total:,} registros "
            f"em {time.time() - started:.1f}s")
    finally:
        conn.close()
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
