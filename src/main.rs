use chrono::Utc;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use postgres::{Client, NoTls};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;
use zip::ZipArchive;

/// Bitset for CNPJ básico (8 digits → 0..99_999_999). ~12.5 MiB.
struct CnpjBitset {
    words: Vec<u64>,
}

impl CnpjBitset {
    fn new() -> Self {
        Self {
            words: vec![0u64; (100_000_000 / 64) + 1],
        }
    }

    /// Returns true if the CNPJ was already present.
    fn insert(&mut self, cnpj: u32) -> bool {
        let idx = cnpj as usize;
        let word = idx / 64;
        let mask = 1u64 << (idx % 64);
        let was = self.words[word] & mask != 0;
        self.words[word] |= mask;
        was
    }
}

fn pad_digits(raw: &str, width: usize) -> String {
    let bytes = raw.as_bytes();
    if bytes.len() == width {
        return raw.to_string();
    }
    if bytes.len() > width {
        return raw[raw.len() - width..].to_string();
    }
    let mut out = String::with_capacity(width);
    for _ in 0..(width - bytes.len()) {
        out.push('0');
    }
    out.push_str(raw);
    out
}

fn parse_cnpj_u32(cnpj: &str) -> Option<u32> {
    if cnpj.len() == 8 && cnpj.bytes().all(|b| b.is_ascii_digit()) {
        cnpj.parse().ok()
    } else {
        None
    }
}

fn is_mei(razao: &str) -> bool {
    let b = razao.as_bytes();
    b.len() >= 11 && b[b.len() - 11..].iter().all(|c| c.is_ascii_digit())
}

fn parse_capital(raw: &str) -> Option<f64> {
    if raw.is_empty() {
        return None;
    }
    if !raw.as_bytes().contains(&b',') {
        return raw.parse().ok();
    }
    let mut buf = [0u8; 64];
    let bytes = raw.as_bytes();
    if bytes.len() > buf.len() {
        return raw.replace(',', ".").parse().ok();
    }
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = if b == b',' { b'.' } else { b };
    }
    std::str::from_utf8(&buf[..bytes.len()])
        .ok()
        .and_then(|s| s.parse().ok())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let data_dir_str = env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    let data_dir = Path::new(&data_dir_str);

    let participante = env::var("PARTICIPANTE").unwrap_or_else(|_| "default_user".to_string());
    let pg_table = env::var("PG_TABLE").unwrap_or_else(|_| format!("{}_empresas", participante));
    let pg_host = env::var("PG_HOST").unwrap_or_else(|_| "postgres_db".to_string());
    let pg_port = env::var("PG_PORT").unwrap_or_else(|_| "5432".to_string());
    let pg_user = env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string());
    let pg_password = env::var("PG_PASSWORD").unwrap_or_else(|_| "postgres".to_string());
    let pg_db = env::var("PG_DB").unwrap_or_else(|_| "db_empresas".to_string());

    println!(
        "[ingestao] Participante={} | Tabela=public.{}",
        participante, pg_table
    );

    let mut entries = vec![];
    if data_dir.exists() {
        for entry in std::fs::read_dir(data_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("zip") {
                entries.push(path);
            }
        }
    }
    entries.sort();

    if entries.is_empty() {
        println!("[ingestao] ERRO: nenhum .zip em /data.");
        return Ok(());
    }

    let conn_str = format!(
        "host={} port={} user={} password={} dbname={} application_name=ingestao",
        pg_host, pg_port, pg_user, pg_password, pg_db
    );
    let mut client = Client::connect(&conn_str, NoTls)?;
    client.batch_execute(
        "SET synchronous_commit TO OFF;
         SET work_mem TO '64MB';",
    )?;

    let tbl = format!("public.\"{}\"", pg_table.replace('"', "\"\""));
    // No UNIQUE during load — bitset dedupes in-stream (DQ-09). Index would kill COPY speed.
    client.execute(&format!("DROP TABLE IF EXISTS {};", tbl), &[])?;
    client.execute(
        &format!(
            "CREATE UNLOGGED TABLE {} (
            cnpj_basico               VARCHAR(8),
            razao_social              VARCHAR,
            natureza_juridica         VARCHAR(4),
            qualificacao_responsavel  VARCHAR,
            capital_social            DOUBLE PRECISION,
            porte_codigo              VARCHAR(2),
            porte_descricao           VARCHAR,
            ente_federativo           VARCHAR,
            capital_social_faixa      VARCHAR,
            is_mei                    BOOLEAN,
            natureza_juridica_grupo   VARCHAR,
            ente_federativo_presente  BOOLEAN,
            data_processamento        TIMESTAMP NOT NULL
        );",
            tbl
        ),
        &[],
    )?;

    let force_not_null = "cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, porte_codigo, porte_descricao, capital_social_faixa, is_mei, natureza_juridica_grupo, ente_federativo_presente, data_processamento";
    let copy_sql = format!(
        "COPY {} (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_codigo, porte_descricao, ente_federativo, capital_social_faixa, is_mei, natureza_juridica_grupo, ente_federativo_presente, data_processamento) FROM STDIN WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '', FORCE_NOT_NULL ({}))",
        tbl, force_not_null
    );

    let mut seen = CnpjBitset::new();
    let mut processed = 0u32;
    let mut total_kept = 0u64;
    let mut total_skipped = 0u64;

    for zip_path in &entries {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let zip_file = archive.by_index(i)?;
            let name = zip_file.name().to_string().to_uppercase();
            if name.ends_with('/') || !name.contains("EMPRE") {
                continue;
            }

            let t0 = Instant::now();
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

            let decoded = DecodeReaderBytesBuilder::new()
                .encoding(Some(WINDOWS_1252))
                .build(zip_file);

            let mut rdr = ReaderBuilder::new()
                .delimiter(b';')
                .quote(b'"')
                .has_headers(false)
                .flexible(true)
                .from_reader(decoded);

            let mut copy_writer = client.copy_in(&copy_sql)?;
            let mut kept = 0u64;
            let mut skipped = 0u64;
            {
                let mut buffered = BufWriter::with_capacity(8 * 1024 * 1024, &mut copy_writer);
                {
                    let mut csv_writer = WriterBuilder::new()
                        .delimiter(b',')
                        .quote(b'"')
                        .from_writer(&mut buffered);

                    let mut record = StringRecord::new();
                    loop {
                        match rdr.read_record(&mut record) {
                            Ok(true) => {}
                            Ok(false) => break,
                            Err(_) => continue,
                        }

                        let cnpj_basico = pad_digits(record.get(0).unwrap_or(""), 8);
                        if let Some(key) = parse_cnpj_u32(&cnpj_basico) {
                            if seen.insert(key) {
                                skipped += 1;
                                continue;
                            }
                        }

                        let razao_social = record.get(1).unwrap_or("").trim().to_uppercase();
                        let natureza_juridica = pad_digits(record.get(2).unwrap_or(""), 4);
                        let qualificacao_responsavel = record.get(3).unwrap_or("").trim();

                        let capital_f64 = parse_capital(record.get(4).unwrap_or(""));

                        let porte_raw = record.get(5).unwrap_or("");
                        let porte_codigo = match porte_raw {
                            "01" | "03" | "05" | "00" => porte_raw,
                            _ => "00",
                        };

                        let porte_descricao = match porte_codigo {
                            "01" => "MICRO EMPRESA",
                            "03" => "EMPRESA DE PEQUENO PORTE",
                            "05" => "DEMAIS",
                            _ => "NÃO INFORMADO",
                        };

                        let ente_federativo = record.get(6).unwrap_or("").trim();
                        let ente_federativo_presente = !ente_federativo.is_empty();

                        let capital_social_faixa = match capital_f64 {
                            None => "SEM CAPITAL",
                            Some(v) if v == 0.0 => "SEM CAPITAL",
                            Some(v) if v <= 1000.0 => "ATÉ 1K",
                            Some(v) if v <= 10000.0 => "1K A 10K",
                            Some(v) if v <= 100000.0 => "10K A 100K",
                            Some(v) if v <= 1000000.0 => "100K A 1M",
                            _ => "ACIMA DE 1M",
                        };

                        let is_mei_flag = is_mei(&razao_social);

                        let natureza_juridica_grupo =
                            match natureza_juridica.as_bytes().first().copied().unwrap_or(b' ') {
                                b'1' => "ADMINISTRAÇÃO PÚBLICA",
                                b'2' => "ENTIDADES EMPRESARIAIS",
                                b'3' => "ENTIDADES SEM FINS LUCRATIVOS",
                                b'4' => "PESSOAS FÍSICAS",
                                b'5' => "ORGANIZAÇÕES INTERNACIONAIS",
                                _ => "OUTROS",
                            };

                        let capital_str = capital_f64.map(|v| v.to_string()).unwrap_or_default();
                        let is_mei_str = if is_mei_flag { "t" } else { "f" };
                        let ente_presente_str = if ente_federativo_presente { "t" } else { "f" };

                        csv_writer.write_record(&[
                            cnpj_basico.as_str(),
                            razao_social.as_str(),
                            natureza_juridica.as_str(),
                            qualificacao_responsavel,
                            capital_str.as_str(),
                            porte_codigo,
                            porte_descricao,
                            ente_federativo,
                            capital_social_faixa,
                            is_mei_str,
                            natureza_juridica_grupo,
                            ente_presente_str,
                            ts.as_str(),
                        ])?;
                        kept += 1;
                    }
                    csv_writer.flush()?;
                }
                buffered.flush()?;
            }
            copy_writer.finish()?;

            total_kept += kept;
            total_skipped += skipped;
            processed += 1;
            let elapsed = t0.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                kept as f64 / elapsed
            } else {
                0.0
            };
            println!(
                "[ingestao] [{}:{}] {} linhas em {:.1}s ({:.0} linhas/s, {} dups pulados)",
                zip_path.display(),
                name,
                kept,
                elapsed,
                rate,
                skipped
            );
        }
    }

    if processed == 0 {
        println!("[ingestao] ERRO: nenhum arquivo EMPRECSV encontrado dentro dos zips.");
        return Ok(());
    }

    let count: i64 = client
        .query_one(&format!("SELECT count(*) FROM {};", tbl), &[])?
        .get(0);
    println!(
        "[ingestao] Concluído: {} arquivo(s), {} registros ({} dups pulados) em {:.1}s",
        processed,
        count,
        total_skipped,
        started.elapsed().as_secs_f64()
    );
    let _ = total_kept;

    Ok(())
}
