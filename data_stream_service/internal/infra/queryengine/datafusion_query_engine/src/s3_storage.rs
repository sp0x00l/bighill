use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::S3Config as OpenDalS3Config;
use opendal::{Configurator, Operator};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3StorageFactory {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub path_style: bool,
}

#[typetag::serde(name = "bighill_s3_storage_factory")]
impl StorageFactory for S3StorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(S3Storage {
            config: self.clone(),
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct S3Storage {
    config: S3StorageFactory,
}

#[async_trait]
#[typetag::serde(name = "bighill_s3_storage")]
impl Storage for S3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative_path) = self.operator_for(path)?;
        op.exists(relative_path).await.map_err(from_opendal_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative_path) = self.operator_for(path)?;
        let meta = op.stat(relative_path).await.map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative_path) = self.operator_for(path)?;
        Ok(op
            .read(relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative_path) = self.operator_for(path)?;
        Ok(Box::new(S3Reader(
            op.reader(relative_path).await.map_err(from_opendal_error)?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, relative_path) = self.operator_for(path)?;
        op.write(relative_path, bs)
            .await
            .map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative_path) = self.operator_for(path)?;
        Ok(Box::new(S3Writer(
            op.writer(relative_path).await.map_err(from_opendal_error)?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.operator_for(path)?;
        op.delete(relative_path).await.map_err(from_opendal_error)
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative_path) = self.operator_for(path)?;
        let prefix = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        op.remove_all(&prefix).await.map_err(from_opendal_error)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

impl S3Storage {
    fn operator_for<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        let (bucket, relative_path) = split_s3_path(path)?;
        let mut builder = OpenDalS3Config::default()
            .into_builder()
            .endpoint(&self.config.endpoint)
            .access_key_id(&self.config.access_key_id)
            .secret_access_key(&self.config.secret_access_key)
            .region(&self.config.region)
            .bucket(bucket);
        if !self.config.path_style {
            builder = builder.enable_virtual_host_style();
        }
        let op = Operator::new(builder).map_err(from_opendal_error)?.finish();
        Ok((op, relative_path))
    }
}

struct S3Reader(opendal::Reader);

#[async_trait]
impl FileRead for S3Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .read(range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

struct S3Writer(opendal::Writer);

#[async_trait]
impl FileWrite for S3Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.0.write(bs).await.map_err(from_opendal_error)
    }

    async fn close(&mut self) -> Result<()> {
        self.0.close().await.map_err(from_opendal_error)?;
        Ok(())
    }
}

fn split_s3_path(path: &str) -> Result<(&str, &str)> {
    let rest = path.strip_prefix("s3://").ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("invalid s3 path {path:?}: expected s3://bucket/key"),
        )
    })?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("invalid s3 path {path:?}: missing key"),
        )
    })?;
    if bucket.trim().is_empty() || key.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("invalid s3 path {path:?}: bucket and key are required"),
        ));
    }
    Ok((bucket, key))
}

fn from_opendal_error(err: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, err.to_string()).with_source(err)
}
