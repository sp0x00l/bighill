use std::collections::HashMap;
use std::env;
use std::io;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::IcebergCatalogProvider;
use serde::Serialize;

mod s3_storage;
use s3_storage::S3StorageFactory;

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_config()?;
    match config.mode.as_str() {
        "query" => query(&config).await,
        "write-iceberg" => write_iceberg(&config).await,
        mode => Err(DataFusionError::Execution(format!(
            "unsupported mode {mode:?}"
        ))),
    }
}

async fn query(config: &Config) -> Result<()> {
    let ctx = SessionContext::new();
    match config.source.as_str() {
        "parquet" => {
            ctx.register_parquet("dataset", &config.data_root, ParquetReadOptions::default())
                .await?;
        }
        "iceberg" => register_iceberg_dataset(&ctx, config).await?,
        _ => return Err(unsupported_source(config)),
    }

    let df = ctx.sql(&config.sql).await?;
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut writer = StreamWriter::try_new(&mut stdout, &schema)?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(())
}

async fn write_iceberg(config: &Config) -> Result<()> {
    if config.source != "parquet" {
        return Err(unsupported_source(config));
    }

    validate_iceberg_config(config)?;

    let ctx = SessionContext::new();
    ctx.register_parquet("source", &config.data_root, ParquetReadOptions::default())
        .await?;

    let source = ctx.table("source").await?;
    let source_schema = source.schema().as_arrow().clone();
    let source_count = count_rows(&ctx, "source").await?;

    let catalog = load_rest_catalog(config).await?;
    let namespace = namespace_ident(&config.namespace)?;
    ensure_namespace(catalog.as_ref(), &namespace).await?;

    let provider = IcebergCatalogProvider::try_new(catalog.clone())
        .await
        .map_err(to_datafusion_error)?;
    let schema_provider = provider.schema(&config.namespace).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "iceberg namespace {:?} is not available",
            config.namespace
        ))
    })?;

    if schema_provider.table(&config.table).await?.is_some() {
        schema_provider.deregister_table(&config.table)?;
    }

    let arrow_schema = Arc::new(source_schema);
    let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
    let empty_table = Arc::new(MemTable::try_new(arrow_schema, vec![vec![empty_batch]])?);
    schema_provider.register_table(config.table.clone(), empty_table)?;
    let table_provider = schema_provider.table(&config.table).await?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "created iceberg table {:?} was not loadable",
            config.table
        ))
    })?;

    ctx.register_table("dataset", table_provider)?;
    ctx.sql("INSERT INTO dataset SELECT * FROM source")
        .await?
        .collect()
        .await?;

    let result = IcebergWriteResult {
        catalog: config.catalog_name.clone(),
        namespace: config.namespace.clone(),
        table: config.table.clone(),
        warehouse: config.warehouse.clone(),
        source_rows: source_count,
        table_rows: count_rows(&ctx, "dataset").await?,
    };

    serde_json::to_writer(io::stdout().lock(), &result)
        .map_err(|err| DataFusionError::Execution(format!("write iceberg result: {err}")))?;
    Ok(())
}

async fn register_iceberg_dataset(ctx: &SessionContext, config: &Config) -> Result<()> {
    validate_iceberg_config(config)?;
    let catalog = load_rest_catalog(config).await?;
    let provider = IcebergCatalogProvider::try_new(catalog)
        .await
        .map_err(to_datafusion_error)?;
    let schema_provider = provider.schema(&config.namespace).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "iceberg namespace {:?} is not available",
            config.namespace
        ))
    })?;
    let table_provider = schema_provider.table(&config.table).await?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "iceberg table {:?}.{:?} is not available",
            config.namespace, config.table
        ))
    })?;
    ctx.register_table("dataset", table_provider)?;
    Ok(())
}

async fn load_rest_catalog(config: &Config) -> Result<Arc<dyn Catalog>> {
    validate_iceberg_config(config)?;
    let mut props = HashMap::from([
        (
            REST_CATALOG_PROP_URI.to_string(),
            normalize_catalog_uri(&config.catalog_uri),
        ),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            config.warehouse.clone(),
        ),
        ("prefix".to_string(), config.catalog_name.clone()),
    ]);
    if !config.catalog_credential.is_empty() {
        props.insert("credential".to_string(), config.catalog_credential.clone());
    }
    if !config.catalog_token.is_empty() {
        props.insert("token".to_string(), config.catalog_token.clone());
    }
    if !config.catalog_scope.is_empty() {
        props.insert("scope".to_string(), config.catalog_scope.clone());
    }

    let _ = warehouse_bucket(&config.warehouse)?;
    let storage_factory = S3StorageFactory {
        endpoint: config.s3_endpoint.clone(),
        region: config.s3_region.clone(),
        access_key_id: config.s3_access_key_id.clone(),
        secret_access_key: config.s3_secret_access_key.clone(),
        path_style: config.s3_path_style,
    };

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(storage_factory))
        .load(config.catalog_name.clone(), props)
        .await
        .map_err(to_datafusion_error)?;

    Ok(Arc::new(catalog))
}

async fn ensure_namespace(catalog: &dyn Catalog, namespace: &NamespaceIdent) -> Result<()> {
    if catalog
        .namespace_exists(namespace)
        .await
        .map_err(to_datafusion_error)?
    {
        return Ok(());
    }
    match catalog.create_namespace(namespace, HashMap::new()).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
        Err(err) => Err(to_datafusion_error(err)),
    }
}

async fn count_rows(ctx: &SessionContext, table: &str) -> Result<u64> {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) AS row_count FROM {table}"))
        .await?;
    let batches = df.collect().await?;
    let batch = batches.first().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "count query for table {table:?} returned no batches"
        ))
    })?;
    let column = batch.column(0);
    if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(values.value(0));
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return u64::try_from(values.value(0)).map_err(|err| {
            DataFusionError::Execution(format!(
                "count query for table {table:?} was negative: {err}"
            ))
        });
    }
    Err(DataFusionError::Execution(format!(
        "count query for table {table:?} returned unsupported type {}",
        column.data_type()
    )))
}

fn validate_iceberg_config(config: &Config) -> Result<()> {
    if config.catalog != "polaris" {
        return Err(DataFusionError::Execution(format!(
            "unsupported iceberg catalog {:?}",
            config.catalog
        )));
    }
    let required = [
        ("catalog-uri", &config.catalog_uri),
        ("catalog-name", &config.catalog_name),
        ("warehouse", &config.warehouse),
        ("namespace", &config.namespace),
        ("table", &config.table),
        ("s3-endpoint", &config.s3_endpoint),
        ("s3-access-key-id", &config.s3_access_key_id),
        ("s3-secret-access-key", &config.s3_secret_access_key),
        ("s3-region", &config.s3_region),
    ];
    for (name, value) in required {
        if value.trim().is_empty() {
            return Err(DataFusionError::Execution(format!(
                "iceberg {name} is required"
            )));
        }
    }
    if config.catalog_credential.trim().is_empty() && config.catalog_token.trim().is_empty() {
        return Err(DataFusionError::Execution(
            "iceberg catalog-credential or catalog-token is required".to_string(),
        ));
    }
    Ok(())
}

fn namespace_ident(namespace: &str) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(namespace.split('.').filter(|part| !part.trim().is_empty()))
        .map_err(to_datafusion_error)
}

fn warehouse_bucket(warehouse: &str) -> Result<String> {
    let rest = warehouse.strip_prefix("s3://").ok_or_else(|| {
        DataFusionError::Execution("iceberg warehouse must use s3://".to_string())
    })?;
    let bucket = rest
        .split('/')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    if bucket.is_empty() {
        return Err(DataFusionError::Execution(
            "iceberg warehouse bucket is required".to_string(),
        ));
    }
    Ok(bucket)
}

fn normalize_catalog_uri(uri: &str) -> String {
    let uri = uri.trim().trim_end_matches('/');
    if uri.ends_with("/api/catalog") {
        uri.to_string()
    } else {
        format!("{uri}/api/catalog")
    }
}

fn to_datafusion_error(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

fn unsupported_source(config: &Config) -> DataFusionError {
    DataFusionError::Execution(format!(
        "unsupported data source {:?} for catalog {:?} catalog-uri {:?} catalog-name {:?} warehouse {:?} namespace {:?} table {:?}",
        config.source,
        config.catalog,
        config.catalog_uri,
        config.catalog_name,
        config.warehouse,
        config.namespace,
        config.table
    ))
}

#[derive(Serialize)]
struct IcebergWriteResult {
    catalog: String,
    namespace: String,
    table: String,
    warehouse: String,
    source_rows: u64,
    table_rows: u64,
}

struct Config {
    mode: String,
    source: String,
    data_root: String,
    catalog: String,
    catalog_uri: String,
    catalog_name: String,
    warehouse: String,
    namespace: String,
    table: String,
    catalog_credential: String,
    catalog_token: String,
    catalog_scope: String,
    s3_endpoint: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    s3_region: String,
    s3_path_style: bool,
    sql: String,
}

fn parse_config() -> Result<Config> {
    let mut data_root =
        env::var("DATAFUSION_PARQUET_PATH").unwrap_or_else(|_| "tmp/local_s3_storage".to_string());
    let mut mode = "query".to_string();
    let mut source = "parquet".to_string();
    let mut catalog = String::new();
    let mut catalog_uri = String::new();
    let mut catalog_name = String::new();
    let mut warehouse = String::new();
    let mut namespace = String::new();
    let mut table = String::new();
    let mut catalog_credential = String::new();
    let mut catalog_token = String::new();
    let mut catalog_scope = String::new();
    let mut s3_endpoint = String::new();
    let mut s3_access_key_id = String::new();
    let mut s3_secret_access_key = String::new();
    let mut s3_region = String::new();
    let mut s3_path_style = true;
    let mut sql_parts: Vec<String> = Vec::new();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode = next_arg(&mut args, "--mode")?.trim().to_lowercase();
            }
            "--source" => {
                source = next_arg(&mut args, "--source")?.trim().to_lowercase();
            }
            "--catalog" => {
                catalog = next_arg(&mut args, "--catalog")?;
            }
            "--catalog-uri" => {
                catalog_uri = next_arg(&mut args, "--catalog-uri")?;
            }
            "--catalog-name" => {
                catalog_name = next_arg(&mut args, "--catalog-name")?;
            }
            "--warehouse" => {
                warehouse = next_arg(&mut args, "--warehouse")?;
            }
            "--namespace" => {
                namespace = next_arg(&mut args, "--namespace")?;
            }
            "--table" => {
                table = next_arg(&mut args, "--table")?;
            }
            "--catalog-credential" => {
                catalog_credential = next_arg(&mut args, "--catalog-credential")?;
            }
            "--catalog-token" => {
                catalog_token = next_arg(&mut args, "--catalog-token")?;
            }
            "--catalog-scope" => {
                catalog_scope = next_arg(&mut args, "--catalog-scope")?;
            }
            "--s3-endpoint" => {
                s3_endpoint = next_arg(&mut args, "--s3-endpoint")?;
            }
            "--s3-access-key-id" => {
                s3_access_key_id = next_arg(&mut args, "--s3-access-key-id")?;
            }
            "--s3-secret-access-key" => {
                s3_secret_access_key = next_arg(&mut args, "--s3-secret-access-key")?;
            }
            "--s3-region" => {
                s3_region = next_arg(&mut args, "--s3-region")?;
            }
            "--s3-path-style" => {
                s3_path_style = parse_bool_flag(&next_arg(&mut args, "--s3-path-style")?)?;
            }
            "--data-root" => {
                data_root = next_arg(&mut args, "--data-root")?;
            }
            "--sql" => {
                sql_parts.push(next_arg(&mut args, "--sql")?);
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

    Ok(Config {
        mode,
        source,
        data_root,
        catalog,
        catalog_uri,
        catalog_name,
        warehouse,
        namespace,
        table,
        catalog_credential,
        catalog_token,
        catalog_scope,
        s3_endpoint,
        s3_access_key_id,
        s3_secret_access_key,
        s3_region,
        s3_path_style,
        sql,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| DataFusionError::Execution(format!("{flag} requires a value")))
}

fn parse_bool_flag(value: &str) -> Result<bool> {
    match value.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(DataFusionError::Execution(format!(
            "boolean flag value {other:?} is invalid"
        ))),
    }
}
