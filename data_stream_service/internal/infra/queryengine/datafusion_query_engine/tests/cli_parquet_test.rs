use std::error::Error;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

#[test]
fn queries_parquet_and_emits_expected_arrow_ipc_rows() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .arg("--data-root")
        .arg(&parquet_path)
        .arg("--sql")
        .arg("SELECT feature, value FROM dataset ORDER BY value")
        .output()?;

    assert_success(&output);
    let (fields, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(fields, vec!["feature", "value"]);
    assert_eq!(
        rows,
        vec![("age".to_string(), 42), ("score".to_string(), 9001)]
    );
    Ok(())
}

#[test]
fn uses_default_sql_when_sql_is_omitted() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .arg("--data-root")
        .arg(&parquet_path)
        .output()?;

    assert_success(&output);
    let (fields, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(fields, vec!["feature", "value"]);
    assert_eq!(
        rows,
        vec![("age".to_string(), 42), ("score".to_string(), 9001)]
    );
    Ok(())
}

#[test]
fn reads_parquet_path_from_environment() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .env("DATAFUSION_PARQUET_PATH", &parquet_path)
        .arg("--sql")
        .arg("SELECT feature, value FROM dataset ORDER BY value DESC")
        .output()?;

    assert_success(&output);
    let (_, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(
        rows,
        vec![("score".to_string(), 9001), ("age".to_string(), 42)]
    );
    Ok(())
}

#[test]
fn accepts_sql_as_positional_arguments() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .arg("--data-root")
        .arg(&parquet_path)
        .arg("SELECT")
        .arg("feature,")
        .arg("value")
        .arg("FROM")
        .arg("dataset")
        .arg("WHERE")
        .arg("value")
        .arg(">")
        .arg("100")
        .output()?;

    assert_success(&output);
    let (_, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(rows, vec![("score".to_string(), 9001)]);
    Ok(())
}

#[test]
fn reads_parquet_dataset_directories() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    write_parquet_rows(
        &temp_dir.path().join("part-000.parquet"),
        &[("age", 42), ("score", 9001)],
    )?;
    write_parquet_rows(&temp_dir.path().join("part-001.parquet"), &[("rank", 7)])?;

    let output = query_engine()
        .arg("--data-root")
        .arg(temp_dir.path())
        .arg("--sql")
        .arg("SELECT feature, value FROM dataset ORDER BY value")
        .output()?;

    assert_success(&output);
    let (_, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(
        rows,
        vec![
            ("rank".to_string(), 7),
            ("age".to_string(), 42),
            ("score".to_string(), 9001),
        ]
    );
    Ok(())
}

#[test]
fn emits_schema_for_empty_result_sets() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .arg("--data-root")
        .arg(&parquet_path)
        .arg("--sql")
        .arg("SELECT feature, value FROM dataset WHERE value < 0")
        .output()?;

    assert_success(&output);
    let (fields, rows) = read_feature_value_ipc(output.stdout)?;

    assert_eq!(fields, vec!["feature", "value"]);
    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn fails_for_invalid_sql() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = query_engine()
        .arg("--data-root")
        .arg(&parquet_path)
        .arg("--sql")
        .arg("SELECT missing FROM dataset")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "missing");
    Ok(())
}

#[test]
fn fails_for_missing_input_path() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let missing_path = temp_dir.path().join("missing.parquet");

    let output = query_engine()
        .arg("--data-root")
        .arg(&missing_path)
        .arg("--sql")
        .arg("SELECT * FROM dataset")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "missing.parquet");
    Ok(())
}

#[test]
fn fails_when_flag_values_are_missing() -> Result<(), Box<dyn Error>> {
    let output = query_engine().arg("--data-root").output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "--data-root requires a value");
    Ok(())
}

#[test]
fn fails_for_iceberg_without_required_catalog_config() -> Result<(), Box<dyn Error>> {
    let output = query_engine()
        .arg("--source")
        .arg("iceberg")
        .arg("--catalog")
        .arg("polaris")
        .arg("--sql")
        .arg("SELECT * FROM dataset")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "iceberg catalog-uri is required");
    Ok(())
}

#[test]
fn fails_for_iceberg_without_catalog_credentials() -> Result<(), Box<dyn Error>> {
    let output = query_engine()
        .arg("--source")
        .arg("iceberg")
        .arg("--catalog")
        .arg("polaris")
        .arg("--catalog-uri")
        .arg("http://polaris:8181")
        .arg("--catalog-name")
        .arg("bighill")
        .arg("--warehouse")
        .arg("s3://bighill-mlops-lakehouse/")
        .arg("--namespace")
        .arg("features")
        .arg("--table")
        .arg("movies")
        .arg("--s3-endpoint")
        .arg("http://polaris-object-store:9000")
        .arg("--s3-access-key-id")
        .arg("polaris_root")
        .arg("--s3-secret-access-key")
        .arg("polaris_pass")
        .arg("--s3-region")
        .arg("eu-west-1")
        .arg("--sql")
        .arg("SELECT * FROM dataset")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(
        &output,
        "iceberg catalog-credential or catalog-token is required",
    );
    Ok(())
}

#[test]
fn write_iceberg_fails_closed_for_non_parquet_sources() -> Result<(), Box<dyn Error>> {
    let output = query_engine()
        .arg("--mode")
        .arg("write-iceberg")
        .arg("--source")
        .arg("json")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "unsupported data source");
    Ok(())
}

#[test]
fn fails_for_unsupported_mode() -> Result<(), Box<dyn Error>> {
    let output = query_engine()
        .arg("--mode")
        .arg("delete-iceberg")
        .output()?;

    assert_failure(&output);
    assert_stderr_contains(&output, "unsupported mode");
    Ok(())
}

fn query_engine() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_datafusion_query_engine"));
    command.env_remove("DATAFUSION_PARQUET_PATH");
    command
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "query engine failed. stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "query engine unexpectedly succeeded. stdout bytes: {}, stderr:\n{}",
        output.stdout.len(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_stderr_contains(output: &Output, expected: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(expected),
        "expected stderr to contain {expected:?}, got:\n{stderr}"
    );
}

fn read_feature_value_ipc(
    bytes: Vec<u8>,
) -> Result<(Vec<String>, Vec<(String, i64)>), Box<dyn Error>> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let fields = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();

    for batch in &mut reader {
        let batch = batch?;
        let feature_column = batch.column(0);
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");

        for row in 0..batch.num_rows() {
            rows.push((
                string_value(feature_column.as_ref(), row),
                values.value(row),
            ));
        }
    }

    Ok((fields, rows))
}

fn string_value(column: &dyn datafusion::arrow::array::Array, row: usize) -> String {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values.value(row).to_string();
    }
    if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
        return values.value(row).to_string();
    }
    panic!("feature column should be Utf8 or Utf8View");
}

fn write_parquet_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    write_parquet_rows(path, &[("age", 42), ("score", 9001)])
}

fn write_parquet_rows(path: &Path, rows: &[(&str, i64)]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("feature", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let feature_values = rows.iter().map(|(feature, _)| *feature).collect::<Vec<_>>();
    let numeric_values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(feature_values)),
            Arc::new(Int64Array::from(numeric_values)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
