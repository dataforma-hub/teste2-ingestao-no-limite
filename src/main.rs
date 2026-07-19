use chrono::Utc;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use postgres::{Client, NoTls};
use regex::Regex;
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use zip::ZipArchive;

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

    println!("[ingestao] Participante={} | Tabela=public.{}", participante, pg_table);
    
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

    let conn_str = format!("host={} port={} user={} password={} dbname={}", pg_host, pg_port, pg_user, pg_password, pg_db);
    let mut client = Client::connect(&conn_str, NoTls)?;
    
    let tbl = format!("public.\"{}\"", pg_table.replace("\"", "\"\""));
    client.execute(&format!("DROP TABLE IF EXISTS {};", tbl), &[])?;
    client.execute(&format!(
        "CREATE UNLOGGED TABLE {} (
            cnpj_basico               VARCHAR(8) UNIQUE,
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
        );", tbl
    ), &[])?;
    
    let is_mei_re = Regex::new(r"[0-9]{11}$").unwrap();
    let force_not_null = "cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, porte_codigo, porte_descricao, capital_social_faixa, is_mei, natureza_juridica_grupo, ente_federativo_presente, data_processamento";
    
    let mut processed = 0;
    
    for zip_path in entries {
        let file = File::open(&zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        
        for i in 0..archive.len() {
            let mut zip_file = archive.by_index(i)?;
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
                .from_reader(decoded);
                
            let sql = format!("COPY {} (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_codigo, porte_descricao, ente_federativo, capital_social_faixa, is_mei, natureza_juridica_grupo, ente_federativo_presente, data_processamento) FROM STDIN WITH (FORMAT csv, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '', FORCE_NOT_NULL ({}))", tbl, force_not_null);
            
            let mut writer = client.copy_in(&sql)?;
            let mut csv_writer = WriterBuilder::new()
                .delimiter(b',')
                .quote(b'"')
                .from_writer(vec![]);
                
            for result in rdr.records() {
                let record = match result {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                
                let c0 = record.get(0).unwrap_or("");
                let mut cnpj_basico = c0.to_string();
                if cnpj_basico.len() < 8 {
                    cnpj_basico = format!("{:0>8}", cnpj_basico);
                } else if cnpj_basico.len() > 8 {
                    cnpj_basico = cnpj_basico[cnpj_basico.len()-8..].to_string();
                }
                
                let c1 = record.get(1).unwrap_or("");
                let razao_social = c1.trim().to_uppercase();
                
                let c2 = record.get(2).unwrap_or("");
                let mut natureza_juridica = c2.to_string();
                if natureza_juridica.len() < 4 {
                    natureza_juridica = format!("{:0>4}", natureza_juridica);
                } else if natureza_juridica.len() > 4 {
                    natureza_juridica = natureza_juridica[natureza_juridica.len()-4..].to_string();
                }
                
                let qualificacao_responsavel = record.get(3).unwrap_or("").trim().to_string();
                
                let c4 = record.get(4).unwrap_or("");
                let capital_f64: Option<f64> = c4.replace(',', ".").parse().ok();
                
                let c5 = record.get(5).unwrap_or("");
                let porte_codigo = match c5 {
                    "01" | "03" | "05" | "00" => c5,
                    _ => "00",
                };
                
                let porte_descricao = match porte_codigo {
                    "01" => "MICRO EMPRESA",
                    "03" => "EMPRESA DE PEQUENO PORTE",
                    "05" => "DEMAIS",
                    _ => "NÃO INFORMADO",
                };
                
                let c6 = record.get(6).unwrap_or("");
                let ente_federativo = c6.trim();
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
                
                let is_mei = is_mei_re.is_match(&razao_social);
                
                let natureza_juridica_grupo = match natureza_juridica.chars().next().unwrap_or(' ') {
                    '1' => "ADMINISTRAÇÃO PÚBLICA",
                    '2' => "ENTIDADES EMPRESARIAIS",
                    '3' => "ENTIDADES SEM FINS LUCRATIVOS",
                    '4' => "PESSOAS FÍSICAS",
                    '5' => "ORGANIZAÇÕES INTERNACIONAIS",
                    _ => "OUTROS",
                };
                
                let capital_str = capital_f64.map(|v| v.to_string()).unwrap_or_default();
                let is_mei_str = if is_mei { "true" } else { "false" };
                let ente_federativo_presente_str = if ente_federativo_presente { "true" } else { "false" };
                
                csv_writer.write_record(&[
                    cnpj_basico.as_str(),
                    razao_social.as_str(),
                    natureza_juridica.as_str(),
                    qualificacao_responsavel.as_str(),
                    capital_str.as_str(),
                    porte_codigo,
                    porte_descricao,
                    ente_federativo,
                    capital_social_faixa,
                    is_mei_str,
                    natureza_juridica_grupo,
                    ente_federativo_presente_str,
                    ts.as_str(),
                ])?;
                
                let bytes = csv_writer.into_inner()?;
                writer.write_all(&bytes)?;
                csv_writer = WriterBuilder::new()
                    .delimiter(b',')
                    .quote(b'"')
                    .from_writer(vec![]);
            }
            
            writer.finish()?;
            processed += 1;
            println!("[ingestao] [{}:{}] carregado em {:.1}s", zip_path.display(), name, t0.elapsed().as_secs_f64());
        }
    }
    
    if processed == 0 {
        println!("[ingestao] ERRO: nenhum arquivo EMPRECSV encontrado dentro dos zips.");
        return Ok(());
    }
    
    let count: i64 = client.query_one(&format!("SELECT count(*) FROM {};", tbl), &[])?.get(0);
    println!("[ingestao] Concluído: {} arquivo(s), {} registros em {:.1}s", processed, count, started.elapsed().as_secs_f64());
    
    Ok(())
}
