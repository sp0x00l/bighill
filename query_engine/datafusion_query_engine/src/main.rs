use std::env;
use std::io;

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let config = parse_config();

    let ctx = SessionContext::new();
    ctx.register_parquet("dataset", config.data_root, ParquetReadOptions::default())
        .await?;

    let df = ctx.sql(&config.sql).await?;
    let batches = df.collect().await?;
    let schema = if let Some(batch) = batches.first() {
        batch.schema()
    } else {
        return Err(datafusion::error::DataFusionError::Execution(
            "query returned no schema".to_string(),
        ));
    };

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut writer = StreamWriter::try_new(&mut stdout, &schema)?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

struct Config {
    data_root: String,
    sql: String,
}

fn parse_config() -> Config {
    let mut data_root =
        env::var("DATAFUSION_PARQUET_PATH").unwrap_or_else(|_| "tmp/local_s3_storage".to_string());
    let mut sql_parts: Vec<String> = Vec::new();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-root" => {
                if let Some(value) = args.next() {
                    data_root = value;
                }
            }
            "--sql" => {
                if let Some(value) = args.next() {
                    sql_parts.push(value);
                }
            }
            value => sql_parts.push(value.to_string()),
        }
    }

    let sql = sql_parts.join(" ");
    let sql = if sql.trim().is_empty() {
        "SELECT * FROM dataset LIMIT 10".to_string()
    } else {
        sql
    };

    Config { data_root, sql }
}
