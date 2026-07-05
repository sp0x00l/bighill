use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergStaticTableProvider};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::Serialize;

const IPC_HEADER: &[u8; 8] = b"BHIPC001";
const IPC_FOOTER: &[u8; 8] = b"BHIPCEND";

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
            validate_local_literal_data_root(config)?;
            ctx.register_parquet("dataset", &config.data_root, ParquetReadOptions::default())
                .await?;
        }
        "iceberg" => register_iceberg_dataset(&ctx, config).await?,
        _ => return Err(unsupported_source(config)),
    }

    let df = ctx.sql(&config.sql).await?;
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;
    let expected_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout
        .write_all(IPC_HEADER)
        .map_err(to_io_datafusion_error)?;
    stdout
        .write_all(&expected_rows.to_le_bytes())
        .map_err(to_io_datafusion_error)?;
    let mut writer = StreamWriter::try_new(&mut stdout, &schema)?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    stdout
        .write_all(IPC_FOOTER)
        .map_err(to_io_datafusion_error)?;
    stdout.flush().map_err(to_io_datafusion_error)?;
    Ok(())
}

async fn write_iceberg(config: &Config) -> Result<()> {
    if config.source != "parquet" {
        return Err(unsupported_source(config));
    }

    validate_iceberg_config(config)?;
    validate_local_literal_data_root(config)?;

    let ctx = SessionContext::new();
    ctx.register_parquet("source", &config.data_root, ParquetReadOptions::default())
        .await?;

    let source = ctx.table("source").await?;
    let source_schema = source.schema().as_arrow().clone();
    let source_count = count_rows(&ctx, "source").await?;

    let catalog = load_rest_catalog(config).await?;
    let namespace = namespace_ident(&config.namespace)?;
    ensure_namespace(catalog.as_ref(), &namespace).await?;
    let table_ident = TableIdent::new(namespace.clone(), config.table.clone());
    if catalog
        .table_exists(&table_ident)
        .await
        .map_err(to_datafusion_error)?
    {
        return Err(DataFusionError::Execution(format!(
            "iceberg table {table_ident} already exists; overwrite/rematerialize requires a snapshot-replace commit and will not drop existing data"
        )));
    }

    let provider = IcebergCatalogProvider::try_new(catalog.clone())
        .await
        .map_err(to_datafusion_error)?;
    let schema_provider = provider.schema(&config.namespace).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "iceberg namespace {:?} is not available",
            config.namespace
        ))
    })?;

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
    let namespace = namespace_ident(&config.namespace)?;
    let table_ident = TableIdent::new(namespace, config.table.clone());
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(to_datafusion_error)?;
    let table_provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .map_err(to_datafusion_error)?;
    ctx.register_table("dataset", Arc::new(table_provider))?;
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
            rest_catalog_warehouse(config),
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
    props.insert(S3_ENDPOINT.to_string(), config.s3_endpoint.clone());
    props.insert(
        S3_ACCESS_KEY_ID.to_string(),
        config.s3_access_key_id.clone(),
    );
    props.insert(
        S3_SECRET_ACCESS_KEY.to_string(),
        config.s3_secret_access_key.clone(),
    );
    props.insert(S3_REGION.to_string(), config.s3_region.clone());
    props.insert(
        S3_PATH_STYLE_ACCESS.to_string(),
        config.s3_path_style.to_string(),
    );
    props.insert(S3_DISABLE_EC2_METADATA.to_string(), "true".to_string());
    props.insert(S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string());

    let _ = warehouse_bucket(&config.warehouse)?;
    let storage_factory = OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
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

fn validate_local_literal_data_root(config: &Config) -> Result<()> {
    let data_root = config.data_root.trim();
    if data_root.contains("://")
        || data_root.contains('*')
        || data_root.contains('?')
        || data_root.contains('[')
    {
        return Ok(());
    }
    if !Path::new(data_root).exists() {
        return Err(DataFusionError::Execution(format!(
            "data root {:?} does not exist",
            config.data_root
        )));
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

fn rest_catalog_warehouse(config: &Config) -> String {
    if config.warehouse.trim().starts_with("s3://") {
        return config.catalog_name.clone();
    }
    config.warehouse.clone()
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

fn to_io_datafusion_error(err: io::Error) -> DataFusionError {
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
