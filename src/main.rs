use chrono::Utc;
use csv::{ReaderBuilder, ByteRecord};
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use postgres::{Client, NoTls};
use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::thread;
use zip::ZipArchive;
use crossbeam_channel::bounded;
use ryu;

struct CnpjBitset {
    words: Vec<u64>,
}

impl CnpjBitset {
    fn new() -> Self {
        Self {
            words: vec![0u64; (100_000_000 / 64) + 1],
        }
    }

    fn insert(&mut self, cnpj: u32) -> bool {
        let idx = cnpj as usize;
        let word = idx / 64;
        let mask = 1u64 << (idx % 64);
        let was = self.words[word] & mask != 0;
        self.words[word] |= mask;
        was
    }
}

fn pad_digits_bytes(raw: &[u8], width: usize, out: &mut Vec<u8>) {
    out.clear();
    if raw.len() == width {
        out.extend_from_slice(raw);
    } else if raw.len() > width {
        out.extend_from_slice(&raw[raw.len() - width..]);
    } else {
        for _ in 0..(width - raw.len()) {
            out.push(b'0');
        }
        out.extend_from_slice(raw);
    }
}

fn parse_cnpj_u32(cnpj: &[u8]) -> Option<u32> {
    if cnpj.len() == 8 && cnpj.iter().all(|&b| b.is_ascii_digit()) {
        let mut v = 0;
        for &b in cnpj {
            v = v * 10 + (b - b'0') as u32;
        }
        Some(v)
    } else {
        None
    }
}

fn is_mei(razao: &[u8]) -> bool {
    let b = razao;
    b.len() >= 11 && b[b.len() - 11..].iter().all(|&c| c.is_ascii_digit())
}

fn parse_capital_f64(raw: &[u8]) -> Option<f64> {
    if raw.is_empty() {
        return None;
    }
    let mut buf = [0u8; 64];
    if raw.len() > buf.len() {
        return None;
    }
    let mut i = 0;
    for &b in raw {
        if b == b',' {
            buf[i] = b'.';
        } else {
            buf[i] = b;
        }
        i += 1;
    }
    std::str::from_utf8(&buf[..i])
        .ok()
        .and_then(|s| s.parse().ok())
}

fn trim_bytes(mut raw: &[u8]) -> &[u8] {
    while let Some(&b) = raw.first() {
        if b.is_ascii_whitespace() {
            raw = &raw[1..];
        } else {
            break;
        }
    }
    while let Some(&b) = raw.last() {
        if b.is_ascii_whitespace() {
            raw = &raw[..raw.len() - 1];
        } else {
            break;
        }
    }
    raw
}

fn write_utf8_upper(raw: &[u8], out: &mut Vec<u8>) {
    out.clear();
    let trimmed = trim_bytes(raw);
    if let Ok(s) = std::str::from_utf8(trimmed) {
        for c in s.chars() {
            for uc in c.to_uppercase() {
                let mut buf = [0u8; 4];
                out.extend_from_slice(uc.encode_utf8(&mut buf).as_bytes());
            }
        }
    } else {
        out.extend_from_slice(trimmed);
    }
}

fn write_csv_field(val: &[u8], out: &mut Vec<u8>) {
    let needs_quotes = val.contains(&b',') || val.contains(&b'"') || val.contains(&b'\n');
    if !needs_quotes {
        out.extend_from_slice(val);
    } else {
        out.push(b'"');
        for &b in val {
            if b == b'"' {
                out.push(b'"');
                out.push(b'"');
            } else {
                out.push(b);
            }
        }
        out.push(b'"');
    }
}

fn write_csv_field_str(val: &str, out: &mut Vec<u8>) {
    write_csv_field(val.as_bytes(), out);
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

    let (tx, rx) = bounded::<Vec<u8>>(100);

    let worker = thread::spawn(move || -> Result<u64, String> {
        let mut client = Client::connect(&conn_str, NoTls).map_err(|e| e.to_string())?;
        let mut copy_writer = client.copy_in(&copy_sql).map_err(|e| e.to_string())?;
        let total_kept = 0u64;
        for chunk in rx {
            copy_writer.write_all(&chunk).map_err(|e| e.to_string())?;
        }
        copy_writer.finish().map_err(|e| e.to_string())?;
        Ok(total_kept)
    });

    let seen = Arc::new(Mutex::new(CnpjBitset::new()));
    let total_skipped = Arc::new(Mutex::new(0u64));
    let total_kept = Arc::new(Mutex::new(0u64));
    let processed = Arc::new(Mutex::new(0u32));

    entries.par_iter().for_each(|zip_path| {
        let file = File::open(zip_path).expect("failed to open zip");
        let mut archive = ZipArchive::new(file).expect("failed to read zip archive");

        for i in 0..archive.len() {
            let zip_file = archive.by_index(i).expect("failed to read zip entry");
            let name = zip_file.name().to_string().to_uppercase();
            if name.ends_with('/') || !name.contains("EMPRE") {
                continue;
            }

            let t0 = Instant::now();
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
            let ts_bytes = ts.as_bytes();

            let decoded = DecodeReaderBytesBuilder::new()
                .encoding(Some(WINDOWS_1252))
                .build(zip_file);

            let mut rdr = ReaderBuilder::new()
                .delimiter(b';')
                .quote(b'"')
                .has_headers(false)
                .flexible(true)
                .from_reader(decoded);

            let mut kept = 0u64;
            let mut skipped = 0u64;

            let mut record = ByteRecord::new();
            let mut out_buffer = Vec::with_capacity(2 * 1024 * 1024);

            let mut buf_cnpj = Vec::new();
            let mut buf_razao = Vec::new();
            let mut buf_natureza = Vec::new();

            loop {
                match rdr.read_byte_record(&mut record) {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(_) => continue,
                }

                let cnpj_raw = record.get(0).unwrap_or(b"");
                pad_digits_bytes(cnpj_raw, 8, &mut buf_cnpj);

                if let Some(key) = parse_cnpj_u32(&buf_cnpj) {
                    if seen.lock().unwrap().insert(key) {
                        skipped += 1;
                        continue;
                    }
                }

                let razao_raw = record.get(1).unwrap_or(b"");
                write_utf8_upper(razao_raw, &mut buf_razao);

                let natureza_raw = record.get(2).unwrap_or(b"");
                pad_digits_bytes(natureza_raw, 4, &mut buf_natureza);

                let qualif_raw = record.get(3).unwrap_or(b"");
                let qualif_trimmed = trim_bytes(qualif_raw);

                let capital_raw = record.get(4).unwrap_or(b"");
                let capital_f64 = parse_capital_f64(capital_raw);

                let porte_raw = record.get(5).unwrap_or(b"");
                let porte_codigo = match porte_raw {
                    b"01" | b"03" | b"05" | b"00" => porte_raw,
                    _ => b"00",
                };
                let porte_descricao = match porte_codigo {
                    b"01" => "MICRO EMPRESA",
                    b"03" => "EMPRESA DE PEQUENO PORTE",
                    b"05" => "DEMAIS",
                    _ => "NÃO INFORMADO",
                };

                let ente_raw = record.get(6).unwrap_or(b"");
                let ente_trimmed = trim_bytes(ente_raw);
                let ente_federativo_presente = !ente_trimmed.is_empty();

                let capital_social_faixa = match capital_f64 {
                    None => "SEM CAPITAL",
                    Some(v) if v == 0.0 => "SEM CAPITAL",
                    Some(v) if v <= 1000.0 => "ATÉ 1K",
                    Some(v) if v <= 10000.0 => "1K A 10K",
                    Some(v) if v <= 100000.0 => "10K A 100K",
                    Some(v) if v <= 1000000.0 => "100K A 1M",
                    _ => "ACIMA DE 1M",
                };

                let is_mei_flag = is_mei(&buf_razao);

                let nat_first = buf_natureza.first().copied().unwrap_or(b' ');
                let natureza_juridica_grupo = match nat_first {
                    b'1' => "ADMINISTRAÇÃO PÚBLICA",
                    b'2' => "ENTIDADES EMPRESARIAIS",
                    b'3' => "ENTIDADES SEM FINS LUCRATIVOS",
                    b'4' => "PESSOAS FÍSICAS",
                    b'5' => "ORGANIZAÇÕES INTERNACIONAIS",
                    _ => "OUTROS",
                };

                write_csv_field(&buf_cnpj, &mut out_buffer); out_buffer.push(b',');
                write_csv_field(&buf_razao, &mut out_buffer); out_buffer.push(b',');
                write_csv_field(&buf_natureza, &mut out_buffer); out_buffer.push(b',');
                write_csv_field(qualif_trimmed, &mut out_buffer); out_buffer.push(b',');
                if let Some(v) = capital_f64 {
                    let mut s = ryu::Buffer::new();
                    out_buffer.extend_from_slice(s.format(v).as_bytes());
                }
                out_buffer.push(b',');
                write_csv_field(porte_codigo, &mut out_buffer); out_buffer.push(b',');
                write_csv_field_str(porte_descricao, &mut out_buffer); out_buffer.push(b',');
                write_csv_field(ente_trimmed, &mut out_buffer); out_buffer.push(b',');
                write_csv_field_str(capital_social_faixa, &mut out_buffer); out_buffer.push(b',');
                out_buffer.extend_from_slice(if is_mei_flag { b"t," } else { b"f," });
                write_csv_field_str(natureza_juridica_grupo, &mut out_buffer); out_buffer.push(b',');
                out_buffer.extend_from_slice(if ente_federativo_presente { b"t," } else { b"f," });
                out_buffer.extend_from_slice(ts_bytes); out_buffer.push(b'\n');

                kept += 1;

                if out_buffer.len() >= 1024 * 1024 {
                    tx.send(out_buffer).unwrap();
                    out_buffer = Vec::with_capacity(2 * 1024 * 1024);
                }
            }

            if !out_buffer.is_empty() {
                tx.send(out_buffer).unwrap();
            }

            *total_kept.lock().unwrap() += kept;
            *total_skipped.lock().unwrap() += skipped;
            *processed.lock().unwrap() += 1;
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
    });

    drop(tx);
    worker.join().unwrap().unwrap();

    let processed = *processed.lock().unwrap();
    let total_skipped = *total_skipped.lock().unwrap();

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

    Ok(())
}
