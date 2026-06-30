use std::error::Error;
use std::fs::File;
use std::io::Cursor;
use std::process::Command;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

#[test]
fn queries_parquet_and_emits_arrow_ipc() -> Result<(), Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let parquet_path = temp_dir.path().join("dataset.parquet");
    write_parquet_fixture(&parquet_path)?;

    let output = Command::new(env!("CARGO_BIN_EXE_datafusion_query_engine"))
        .arg("--data-root")
        .arg(&parquet_path)
        .arg("--sql")
        .arg("SELECT feature, value FROM dataset ORDER BY value")
        .output()?;

    assert!(
        output.status.success(),
        "query engine failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
    let mut total_rows = 0;
    for batch in reader {
        let batch = batch?;
        total_rows += batch.num_rows();
        assert_eq!(batch.schema().field(0).name(), "feature");
        assert_eq!(batch.schema().field(1).name(), "value");
    }

    assert_eq!(total_rows, 2);
    Ok(())
}

fn write_parquet_fixture(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("feature", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["age", "score"])),
            Arc::new(Int64Array::from(vec![42, 9001])),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
