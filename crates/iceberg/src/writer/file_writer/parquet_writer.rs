// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The module contains the file writer for parquet file format.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, ListArray, StructArray};
use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use itertools::Itertools;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_writer::AsyncFileWriter as ArrowAsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{from_thrift, Statistics};
use parquet::format::FileMetaData;

use super::location_generator::{FileNameGenerator, LocationGenerator};
use super::track_writer::TrackWriter;
use super::{FileWriter, FileWriterBuilder};
use crate::arrow::{
    get_parquet_stat_max_as_datum, get_parquet_stat_min_as_datum, ArrowFileReader,
    DEFAULT_MAP_FIELD_NAME,
};
use crate::io::{FileIO, FileWrite, OutputFile};
use crate::spec::{
    visit_schema, DataContentType, DataFileBuilder, DataFileFormat, Datum, ListType, MapType,
    NestedFieldRef, PrimitiveType, Schema, SchemaRef, SchemaVisitor, Struct, StructType,
    TableMetadata, Type,
};
use crate::writer::{CurrentFileStatus, DataFile};
use crate::{Error, ErrorKind, Result};

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone, Debug)]
pub struct ParquetWriterBuilder<T: LocationGenerator, F: FileNameGenerator> {
    props: WriterProperties,
    schema: SchemaRef,

    file_io: FileIO,
    location_generator: T,
    file_name_generator: F,
}

impl<T: LocationGenerator, F: FileNameGenerator> ParquetWriterBuilder<T, F> {
    /// Create a new `ParquetWriterBuilder`
    /// To construct the write result, the schema should contain the `PARQUET_FIELD_ID_META_KEY` metadata for each field.
    pub fn new(
        props: WriterProperties,
        schema: SchemaRef,
        file_io: FileIO,
        location_generator: T,
        file_name_generator: F,
    ) -> Self {
        Self {
            props,
            schema,
            file_io,
            location_generator,
            file_name_generator,
        }
    }
}

impl<T: LocationGenerator, F: FileNameGenerator> FileWriterBuilder for ParquetWriterBuilder<T, F> {
    type R = ParquetWriter;

    async fn build(self) -> crate::Result<Self::R> {
        let arrow_schema: ArrowSchemaRef = Arc::new(self.schema.as_ref().try_into()?);
        let written_size = Arc::new(AtomicI64::new(0));
        let out_file = self.file_io.new_output(
            self.location_generator
                .generate_location(&self.file_name_generator.generate_file_name()),
        )?;
        let inner_writer = TrackWriter::new(out_file.writer().await?, written_size.clone());
        let async_writer = AsyncFileWriter::new(inner_writer);
        let writer =
            AsyncArrowWriter::try_new(async_writer, arrow_schema.clone(), Some(self.props))
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "Failed to build parquet writer.")
                        .with_source(err)
                })?;

        Ok(ParquetWriter {
            schema: self.schema.clone(),
            writer,
            written_size,
            current_row_num: 0,
            out_file,
            nan_value_counts: HashMap::new(),
        })
    }
}

/// A mapping from Parquet column path names to internal field id
struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn r#struct(&mut self, _struct: &StructType, _results: Vec<Self::T>) -> Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn map(&mut self, _map: &MapType, _key_value: Self::T, _value: Self::T) -> Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> Result<Self::T> {
        let full_name = self.field_names.iter().map(String::as_str).join(".");
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(Error::new(ErrorKind::DataInvalid, format!("Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}")));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

/// `ParquetWriter`` is used to write arrow data into parquet file on storage.
pub struct ParquetWriter {
    schema: SchemaRef,
    out_file: OutputFile,
    writer: AsyncArrowWriter<AsyncFileWriter<TrackWriter>>,
    written_size: Arc<AtomicI64>,
    current_row_num: usize,
    nan_value_counts: HashMap<i32, u64>,
}

/// Used to aggregate min and max value of each column.
struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    fn update(&mut self, field_id: i32, value: Statistics) -> Result<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Composed type {} is not supported for min max aggregation.",
                    ty
                ),
            ));
        };

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {} is not match with field type {}.", value, ty),
                ));
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {} is not match with field type {}.", value, ty),
                ));
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

macro_rules! count_float_nans {
    ($t:ty, $col:ident, $self:ident, $field_id:ident) => {
        let nan_val_cnt = $col
            .as_any()
            .downcast_ref::<$t>()
            .unwrap()
            .iter()
            .filter(|value| value.map_or(false, |v| v.is_nan()))
            .count() as u64;

        match $self.nan_value_counts.entry($field_id) {
            Entry::Occupied(mut ele) => {
                let total_nan_val_cnt = ele.get() + nan_val_cnt;
                ele.insert(total_nan_val_cnt);
            }
            Entry::Vacant(v) => {
                v.insert(nan_val_cnt);
            }
        };
    };
}

// macro_rules! update_counters {
//     (add, $self:ident, $files_field:ident, $total_files_field:ident,
//      $added_size_field:ident, $total_size_field:ident, $size:expr) => {
//         $self.$files_field = $self.$files_field.saturating_add(1);
//         $self.$total_files_field = $self.$total_files_field.saturating_add(1);
//         $self.$added_size_field = $self.$added_size_field.saturating_add($size);
//         $self.$total_size_field = $self.$total_size_field.saturating_add($size);
//     };
//
//     (remove, $self:ident, $files_field:ident, $total_files_field:ident,
//      $removed_size_field:ident, $total_size_field:ident, $size:expr) => {
//         $self.$files_field = $self.$files_field.saturating_add(1);
//         $self.$total_files_field = $self.$total_files_field.saturating_sub(1);
//         $self.$removed_size_field = $self.$removed_size_field.saturating_add($size);
//         $self.$total_size_field = $self.$total_size_field.saturating_sub($size);
//     };
// }

impl ParquetWriter {
    /// Converts parquet files to data files
    #[allow(dead_code)]
    pub(crate) async fn parquet_files_to_data_files(
        file_io: &FileIO,
        file_paths: Vec<String>,
        table_metadata: &TableMetadata,
    ) -> Result<Vec<DataFile>> {
        // TODO: support adding to partitioned table
        let mut data_files: Vec<DataFile> = Vec::new();

        for file_path in file_paths {
            let input_file = file_io.new_input(&file_path)?;
            let file_metadata = input_file.metadata().await?;
            let file_size_in_bytes = file_metadata.size as usize;
            let reader = input_file.reader().await?;

            let mut parquet_reader = ArrowFileReader::new(file_metadata, reader);
            let parquet_metadata = parquet_reader.get_metadata().await.map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Error reading Parquet metadata: {}", err),
                )
            })?;
            let builder = ParquetWriter::parquet_to_data_file_builder(
                table_metadata.current_schema().clone(),
                parquet_metadata,
                file_size_in_bytes,
                file_path,
            )?;
            let data_file = builder.build().unwrap();
            data_files.push(data_file);
        }

        Ok(data_files)
    }

    fn to_data_file_builder(
        schema: SchemaRef,
        metadata: FileMetaData,
        written_size: usize,
        file_path: String,
        nan_value_counts: HashMap<i32, u64>,
    ) -> Result<DataFileBuilder> {
        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(&schema, &mut visitor)?;
            visitor
        };

        let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
            let mut per_col_size: HashMap<i32, u64> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
            let mut min_max_agg = MinMaxColAggregator::new(schema);

            for row_group in &metadata.row_groups {
                for column_chunk in row_group.columns.iter() {
                    let Some(column_chunk_metadata) = &column_chunk.meta_data else {
                        continue;
                    };
                    let physical_type = column_chunk_metadata.type_;
                    let Some(&field_id) =
                        index_by_parquet_path.get(&column_chunk_metadata.path_in_schema.join("."))
                    else {
                        // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
                        // Ignore the field if it is not in schema.
                        continue;
                    };
                    *per_col_size.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.total_compressed_size as u64;
                    *per_col_val_num.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.num_values as u64;
                    if let Some(null_count) = column_chunk_metadata
                        .statistics
                        .as_ref()
                        .and_then(|s| s.null_count)
                    {
                        *per_col_null_val_num.entry(field_id).or_insert(0_u64) += null_count as u64;
                    }
                    if let Some(statistics) = &column_chunk_metadata.statistics {
                        min_max_agg.update(
                            field_id,
                            from_thrift(physical_type.try_into()?, Some(statistics.clone()))?
                                .unwrap(),
                        )?;
                    }
                }
            }

            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                min_max_agg.produce(),
            )
        };

        let mut builder = DataFileBuilder::default();
        builder
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(metadata.num_rows as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .nan_value_counts(nan_value_counts)
            // # TODO(#417)
            // - distinct_counts
            .key_metadata(metadata.footer_signing_key_metadata)
            .split_offsets(
                metadata
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            );
        Ok(builder)
    }

    /// `ParquetMetadata` to data file builder
    pub(crate) fn parquet_to_data_file_builder(
        schema: SchemaRef,
        metadata: Arc<ParquetMetaData>,
        written_size: usize,
        file_path: String,
    ) -> Result<DataFileBuilder> {
        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(&schema, &mut visitor)?;
            visitor
        };

        let (column_sizes, value_counts, null_value_counts, (lower_bounds, upper_bounds)) = {
            let mut per_col_size: HashMap<i32, u64> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, u64> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, u64> = HashMap::new();
            let mut min_max_agg = MinMaxColAggregator::new(schema);

            for row_group in metadata.row_groups() {
                for column_chunk_metadata in row_group.columns() {
                    let parquet_path = column_chunk_metadata.column_descr().path().string();

                    let Some(&field_id) = index_by_parquet_path.get(&parquet_path) else {
                        continue;
                    };

                    *per_col_size.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.compressed_size() as u64;
                    *per_col_val_num.entry(field_id).or_insert(0) +=
                        column_chunk_metadata.num_values() as u64;

                    if let Some(statistics) = column_chunk_metadata.statistics() {
                        if let Some(null_count) = statistics.null_count_opt() {
                            *per_col_null_val_num.entry(field_id).or_insert(0) += null_count;
                        }

                        min_max_agg.update(field_id, statistics.clone())?;
                    }
                }
            }
            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                min_max_agg.produce(),
            )
        };

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(file_path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(metadata.file_metadata().num_rows() as u64)
            .file_size_in_bytes(written_size as u64)
            .column_sizes(column_sizes)
            .value_counts(value_counts)
            .null_value_counts(null_value_counts)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .split_offsets(
                metadata
                    .row_groups()
                    .iter()
                    .filter_map(|group| group.file_offset())
                    .collect(),
            );

        Ok(builder)
    }

    fn transverse(&mut self, col: &ArrayRef, field: &NestedFieldRef) {
        let dt = col.data_type();

        // TODO(feniljain):
        // Types:
        // [X] Primitive
        // [X] Struct
        // [ ] List
        // [ ] Map
        //
        // https://docs.rs/arrow-array/latest/arrow_array/array/index.html#structs
        // https://docs.rs/arrow-array/latest/arrow_array/array/index.html#types
        //
        // Implement a record batch column transversor and transverse using something
        // like this: https://chatgpt.com/share/679bca9a-646c-800c-87f9-93bd12dc9427

        match dt {
            DataType::Float32 => {
                let field_id: i32 = field.id;
                count_float_nans!(Float32Array, col, self, field_id);
            }
            DataType::Float64 => {
                let field_id: i32 = field.id;
                count_float_nans!(Float64Array, col, self, field_id);
            }
            DataType::Struct(fields) => {
                let struct_arr = col.as_any().downcast_ref::<StructArray>().unwrap();
                for (idx, arrow_field) in fields.iter().enumerate() {
                    match arrow_field.data_type() {
                        DataType::Float32 => {
                            let float_arr_ref = struct_arr.column(idx);

                            let field_id = match field.clone().field_type.deref() {
                                Type::Struct(struct_ty) => struct_ty.fields()[idx].id,
                                _ => unreachable!(),
                            };

                            count_float_nans!(Float32Array, float_arr_ref, self, field_id);
                        }
                        DataType::Float64 => {
                            let float_arr_ref = struct_arr.column(idx);

                            // TODO(feniljain): How to efficiently cast field to given type
                            let field_id = match field.clone().field_type.deref() {
                                Type::Struct(struct_ty) => struct_ty.fields()[idx].id,
                                _ => unreachable!(),
                            };

                            count_float_nans!(Float64Array, float_arr_ref, self, field_id);
                        }
                        DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::Struct(_)
                        | DataType::Map(_, _) => {
                            let arr_ref = struct_arr.column(idx);

                            let field = match field.clone().field_type.deref() {
                                Type::Struct(struct_ty) => struct_ty.fields()[idx].clone(),
                                _ => unreachable!(),
                            };

                            self.transverse(arr_ref, &field);
                        }
                        _ => {}
                    };
                }
            }
            DataType::List(arrow_field) => {
                let list_arr = col.as_any().downcast_ref::<ListArray>().unwrap();
                let field_data_typ = arrow_field.data_type();

                let n_vals = list_arr.offsets().len() - 1;

                let field = match field.clone().field_type.deref() {
                    Type::List(list_typ) => list_typ.element_field.clone(),
                    _ => unreachable!(),
                };
                let field_id = field.id;

                match field_data_typ {
                    DataType::Float32 => {
                        for idx in 0..n_vals {
                            let arr_ref = list_arr.value(idx);
                            count_float_nans!(Float32Array, arr_ref, self, field_id);
                        }
                    }
                    DataType::Float64 => {
                        for idx in 0..n_vals {
                            let arr_ref = list_arr.value(idx);
                            count_float_nans!(Float64Array, arr_ref, self, field_id);
                        }
                    }
                    DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Struct(_)
                    | DataType::Map(_, _) => {
                        for idx in 0..n_vals {
                            let arr_ref = list_arr.value(idx);
                            self.transverse(&arr_ref, &field);
                        }
                    }
                    _ => {}
                };
            }
            // DataType::LargeList => {}
            // DataType::FixedSizeList => {}
            // DataType::Map => {}
            _ => {}
        };
    }
}

impl FileWriter for ParquetWriter {
    async fn write(&mut self, batch: &arrow_array::RecordBatch) -> crate::Result<()> {
        self.current_row_num += batch.num_rows();

        let schema_c = self.schema.clone(); // TODO(feniljain): better way to do this??
        let fields = schema_c.as_struct().fields();

        for (col, field) in batch.columns().iter().zip(fields) {
            self.transverse(col, field);
        }

        self.writer.write(batch).await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to write using parquet writer.",
            )
            .with_source(err)
        })?;

        Ok(())
    }

    async fn close(self) -> crate::Result<Vec<crate::spec::DataFileBuilder>> {
        let metadata = self.writer.close().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Failed to close parquet writer.").with_source(err)
        })?;

        let written_size = self.written_size.load(std::sync::atomic::Ordering::Relaxed);

        Ok(vec![Self::to_data_file_builder(
            self.schema,
            metadata,
            written_size as usize,
            self.out_file.location().to_string(),
            self.nan_value_counts,
        )?])
    }
}

impl CurrentFileStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.out_file.location().to_string()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        self.written_size.load(std::sync::atomic::Ordering::Relaxed) as usize
    }
}

/// AsyncFileWriter is a wrapper of FileWrite to make it compatible with tokio::io::AsyncWrite.
///
/// # NOTES
///
/// We keep this wrapper been used inside only.
struct AsyncFileWriter<W: FileWrite>(W);

impl<W: FileWrite> AsyncFileWriter<W> {
    /// Create a new `AsyncFileWriter` with the given writer.
    pub fn new(writer: W) -> Self {
        Self(writer)
    }
}

impl<W: FileWrite> ArrowAsyncFileWriter for AsyncFileWriter<W> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .write(bs)
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            self.0
                .close()
                .await
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::types::{Float32Type, Int64Type};
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Int32Array, Int64Array,
        ListArray, RecordBatch, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields, SchemaRef as ArrowSchemaRef};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use rust_decimal::Decimal;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::{PrimitiveLiteral, Struct, Schema as IcebergSchema, *};
    use crate::writer::file_writer::location_generator::test::MockLocationGenerator;
    use crate::writer::file_writer::location_generator::DefaultFileNameGenerator;
    use crate::writer::tests::check_parquet_data_file;

    fn schema_for_all_type() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(0, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::optional(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
                NestedField::optional(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::optional(5, "string", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
                NestedField::optional(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
                NestedField::optional(8, "time", Type::Primitive(PrimitiveType::Time)).into(),
                NestedField::optional(9, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::optional(
                    10,
                    "timestamptz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )
                .into(),
                NestedField::optional(
                    11,
                    "timestamp_ns",
                    Type::Primitive(PrimitiveType::TimestampNs),
                )
                .into(),
                NestedField::optional(
                    12,
                    "timestamptz_ns",
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                )
                .into(),
                NestedField::optional(
                    13,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 5,
                    }),
                )
                .into(),
                NestedField::optional(14, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(15, "fixed", Type::Primitive(PrimitiveType::Fixed(10)))
                    .into(),
                // Parquet Statistics will use different representation for Decimal with precision 38 and scale 5,
                // so we need to add a new field for it.
                NestedField::optional(
                    16,
                    "decimal_38",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 38,
                        scale: 5,
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn nested_schema_for_test() -> Schema {
        // Int, Struct(Int,Int), String, List(Int), Struct(Struct(Int)), Map(String, List(Int))
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(
                    1,
                    "col1",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(5, "col_1_5", Type::Primitive(PrimitiveType::Long))
                            .into(),
                        NestedField::required(6, "col_1_6", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
                NestedField::required(2, "col2", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    3,
                    "col3",
                    Type::List(ListType::new(
                        NestedField::required(7, "element", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    )),
                )
                .into(),
                NestedField::required(
                    4,
                    "col4",
                    Type::Struct(StructType::new(vec![NestedField::required(
                        8,
                        "col_4_8",
                        Type::Struct(StructType::new(vec![NestedField::required(
                            9,
                            "col_4_8_9",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()])),
                    )
                    .into()])),
                )
                .into(),
                NestedField::required(
                    10,
                    "col5",
                    Type::Map(MapType::new(
                        NestedField::required(11, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(
                            12,
                            "value",
                            Type::List(ListType::new(
                                NestedField::required(
                                    13,
                                    "item",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            )),
                        )
                        .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_index_by_parquet_path() {
        let expect = HashMap::from([
            ("col0".to_string(), 0),
            ("col1.col_1_5".to_string(), 5),
            ("col1.col_1_6".to_string(), 6),
            ("col2".to_string(), 2),
            ("col3.list.element".to_string(), 7),
            ("col4.col_4_8.col_4_8_9".to_string(), 9),
            ("col5.key_value.key".to_string(), 11),
            ("col5.key_value.value.list.item".to_string(), 13),
        ]);
        let mut visitor = IndexByParquetPathName::new();
        visit_schema(&nested_schema_for_test(), &mut visitor).unwrap();
        assert_eq!(visitor.name_to_id, expect);
    }

    // TODO(feniljain): Remove nan value count test from here
    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = {
            let fields = vec![
                arrow_schema::Field::new("col", arrow_schema::DataType::Int64, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
                arrow_schema::Field::new("col1", arrow_schema::DataType::Float32, true)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "1".to_string(),
                    )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };
        let col = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let null_col = Arc::new(Int64Array::new_null(1024)) as ArrayRef;
        let float_col = Arc::new(Float32Array::from_iter_values((0..1024).map(|x| {
            if x % 100 == 0 {
                // There will be 11 NANs as there are 1024 entries
                f32::NAN
            } else {
                x as f32
            }
        }))) as ArrayRef;
        let to_write = RecordBatch::try_new(schema.clone(), vec![col, float_col.clone()]).unwrap();
        let to_write_null =
            RecordBatch::try_new(schema.clone(), vec![null_col, float_col]).unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;

        pw.write(&to_write).await?;
        pw.write(&to_write_null).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 2048);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([(0, 2048), (1, 2048)])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(0, Datum::long(0)), (1, Datum::float(1.0))])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(0, Datum::long(1023)), (1, Datum::float(1023.0))])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(0, 1024), (1, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(0, 0), (1, 22)]) // 22, cause we wrote float column twice
        );

        // check the written file
        let expect_batch = concat_batches(&schema, vec![&to_write, &to_write_null]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_primitive_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // TODO(feniljain): Write test for list types, both direct float fields and nested ones

        let schema_struct_float_fields =
            Fields::from(vec![Field::new("col4", DataType::Float32, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "4".to_string(),
                )]))]);

        let schema_struct_nested_float_fields =
            Fields::from(vec![Field::new("col7", DataType::Float32, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "7".to_string(),
                )]))]);

        let schema_struct_nested_fields = Fields::from(vec![Field::new(
            "col6",
            arrow_schema::DataType::Struct(schema_struct_nested_float_fields.clone()),
            false,
        )
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "6".to_string(),
        )]))]); // TODO(feniljain): add test for list and map inside struct too

        // prepare data
        let arrow_schema = {
            let fields = vec![
                Field::new("col", arrow_schema::DataType::Float32, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "0".to_string())]),
                ),
                Field::new("col2", arrow_schema::DataType::Float64, false).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
                ),
                Field::new(
                    "col3",
                    arrow_schema::DataType::Struct(schema_struct_float_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "3".to_string(),
                )])),
                Field::new(
                    "col5",
                    arrow_schema::DataType::Struct(schema_struct_nested_fields.clone()),
                    false,
                )
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "5".to_string(),
                )])),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        // let iceberg_schema = Schema::builder()
        //     .with_schema_id(1)
        //     .with_fields(vec![
        //         NestedField::required(0, "col0", Type::Primitive(PrimitiveType::Float)).into(), // for primitive float32
        //         NestedField::required(1, "col1", Type::Primitive(PrimitiveType::Float)).into(), // for primitive float64
        //         NestedField::required(
        //             2,
        //             "col2",
        //             Type::Struct(StructType::new(vec![
        //                 NestedField::required(3, "field1", Type::Primitive(PrimitiveType::Float))
        //                     .into(),
        //             ])),
        //         )
        //         .into(), // XXX struct with float field
        //         // NestedField::required(
        //         //     4,
        //         //     "col3",
        //         //     Type::List(ListType::new(
        //         //         NestedField::required(5, "element1", Type::Primitive(PrimitiveType::Float))
        //         //             .into(),
        //         //     )),
        //         // )
        //         // .into(), // list with float field
        //         // NestedField::required(
        //         //     6,
        //         //     "col4",
        //         //     Type::Struct(StructType::new(vec![NestedField::required(
        //         //         7,
        //         //         "col_6_7",
        //         //         Type::Struct(StructType::new(vec![NestedField::required(
        //         //             8,
        //         //             "col_6_7_8",
        //         //             Type::Primitive(PrimitiveType::Float),
        //         //         )
        //         //         .into()])),
        //         //     )
        //         //     .into()])),
        //         // )
        //         // .into(), // XXX struct inside struct with float field
        //         // NestedField::required(
        //         //     9,
        //         //     "col5",
        //         //     Type::Map(MapType::new(
        //         //         NestedField::required(10, "key", Type::Primitive(PrimitiveType::String))
        //         //             .into(),
        //         //         NestedField::required(
        //         //             11,
        //         //             "value",
        //         //             Type::List(ListType::new(
        //         //                 NestedField::required(
        //         //                     12,
        //         //                     "item",
        //         //                     Type::Primitive(PrimitiveType::Float),
        //         //                 )
        //         //                 .into(),
        //         //             )),
        //         //         )
        //         //         .into(),
        //         //     )), // XXX float inside list inside map
        //         // )
        //         // .into(),
        //     ])
        //     .build()
        //     .unwrap();
        //
        // let arrow_schema: ArrowSchemaRef = Arc::new((&iceberg_schema).try_into().unwrap());

        let float_32_col = Arc::new(Float32Array::from_iter_values_with_nulls(
            [1.0_f32, f32::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let float_64_col = Arc::new(Float64Array::from_iter_values_with_nulls(
            [1.0_f64, f64::NAN, 2.0, 2.0].into_iter(),
            None,
        )) as ArrayRef;

        let struct_float_field_col = Arc::new(StructArray::new(
            schema_struct_float_fields,
            vec![float_32_col.clone()],
            None,
        )) as ArrayRef;

        let struct_nested_float_field_col = Arc::new(StructArray::new(
            schema_struct_nested_fields,
            vec![Arc::new(StructArray::new(
                schema_struct_nested_float_fields,
                vec![float_32_col.clone()],
                None,
            )) as ArrayRef],
            None,
        )) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            float_32_col,
            float_64_col,
            struct_float_field_col,
            struct_nested_float_field_col,
        ])
        .unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(to_write.schema().as_ref().try_into().unwrap()),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([(0, 4), (1, 4), (4, 4), (7, 4)])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::float(1.0)),
                (1, Datum::double(1.0)),
                (4, Datum::float(1.0)),
                (7, Datum::float(1.0)),
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::float(2.0)),
                (1, Datum::double(2.0)),
                (4, Datum::float(2.0)),
                (7, Datum::float(2.0)),
            ])
        );
        assert_eq!(
            *data_file.null_value_counts(),
            HashMap::from([(0, 0), (1, 0), (4, 0), (7, 0)])
        );
        assert_eq!(
            *data_file.nan_value_counts(),
            HashMap::from([(0, 1), (1, 1), (4, 1), (7, 1)])
        );

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_nan_val_cnts_list_type() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // TODO(feniljain): Write test for list types, both direct float fields and nested ones

        let schema_list_float_field = Field::new("col1", DataType::Float32, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );

        // prepare data
        let arrow_schema = {
            let fields = vec![Field::new_list(
                "col0",
                schema_list_float_field.clone(),
                true,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "0".to_string(),
            )]))];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        println!("arrow_schema: {}", arrow_schema);

        let list_float_field_col = Arc::new({
            let list_parts = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
                Some(1.0_f32),
                Some(f32::NAN),
                Some(2.0),
                Some(2.0),
            ])])
            .into_parts();
            ListArray::new(
                {
                    println!("DEBUG: arrow_schema.field(0): {:?}", arrow_schema.field(0));
                    if let DataType::List(_) = arrow_schema.field(0).data_type() {
                        // let field1 = schema_list_float_field.with_name("colcol");
                        // println!("DEBUG: field: {:?}", field1.clone());
                        // field1.into()
                        schema_list_float_field.into()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;

        println!("DEBUG: list_float_field_col.data_type: {:?}", list_float_field_col.data_type());

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![list_float_field_col])
            .expect("Could not form record batch");

        let iceberg_schema: IcebergSchema = to_write.schema().as_ref().try_into().expect("Could not convert to iceberg schema");
        println!("DEBUG: iceberg_schema: {:?}", iceberg_schema);

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                to_write
                    .schema()
                    .as_ref()
                    .try_into()
                    .expect("Could not convert iceberg schema"),
            ),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;

        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1);
        assert_eq!(*data_file.value_counts(), HashMap::from([(1, 4)]));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([(1, Datum::float(1.0)),])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([(1, Datum::float(2.0)),])
        );
        assert_eq!(*data_file.null_value_counts(), HashMap::from([(1, 0)]));
        assert_eq!(*data_file.nan_value_counts(), HashMap::from([(1, 1)]));

        // check the written file
        let expect_batch = concat_batches(&arrow_schema, vec![&to_write]).unwrap();
        check_parquet_data_file(&file_io, &data_file, &expect_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_complex_schema() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        let schema = nested_schema_for_test();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
        let col1 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![
                Arc::new(Int64Array::from_iter_values(0..1024)),
                Arc::new(Int64Array::from_iter_values(0..1024)),
            ],
            None,
        ));
        let col2 = Arc::new(arrow_array::StringArray::from_iter_values(
            (0..1024).map(|n| n.to_string()),
        )) as ArrayRef;
        let col3 = Arc::new({
            let list_parts = arrow_array::ListArray::from_iter_primitive::<Int64Type, _, _>(
                (0..1024).map(|n| Some(vec![Some(n)])),
            )
            .into_parts();
            arrow_array::ListArray::new(
                {
                    if let DataType::List(field) = arrow_schema.field(3).data_type() {
                        field.clone()
                    } else {
                        unreachable!()
                    }
                },
                list_parts.1,
                list_parts.2,
                list_parts.3,
            )
        }) as ArrayRef;
        let col4 = Arc::new(StructArray::new(
            {
                if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                    fields.clone()
                } else {
                    unreachable!()
                }
            },
            vec![Arc::new(StructArray::new(
                {
                    if let DataType::Struct(fields) = arrow_schema.field(4).data_type() {
                        if let DataType::Struct(fields) = fields[0].data_type() {
                            fields.clone()
                        } else {
                            unreachable!()
                        }
                    } else {
                        unreachable!()
                    }
                },
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
                None,
            ))],
            None,
        ));
        let col5 = Arc::new({
            let mut map_array_builder = arrow_array::builder::MapBuilder::new(
                None,
                arrow_array::builder::StringBuilder::new(),
                arrow_array::builder::ListBuilder::new(arrow_array::builder::PrimitiveBuilder::<
                    Int64Type,
                >::new()),
            );
            for i in 0..1024 {
                map_array_builder.keys().append_value(i.to_string());
                map_array_builder
                    .values()
                    .append_value(vec![Some(i as i64); i + 1]);
                map_array_builder.append(true)?;
            }
            let (_, offset_buffer, struct_array, null_buffer, ordered) =
                map_array_builder.finish().into_parts();
            let struct_array = {
                let (_, mut arrays, nulls) = struct_array.into_parts();
                let list_array = {
                    let list_array = arrays[1]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .clone();
                    let (_, offsets, array, nulls) = list_array.into_parts();
                    let list_field = {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                if let DataType::List(list_field) = fields[1].data_type() {
                                    list_field.clone()
                                } else {
                                    unreachable!()
                                }
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    };
                    ListArray::new(list_field, offsets, array, nulls)
                };
                arrays[1] = Arc::new(list_array) as ArrayRef;
                StructArray::new(
                    {
                        if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                            if let DataType::Struct(fields) = map_field.data_type() {
                                fields.clone()
                            } else {
                                unreachable!()
                            }
                        } else {
                            unreachable!()
                        }
                    },
                    arrays,
                    nulls,
                )
            };
            arrow_array::MapArray::new(
                {
                    if let DataType::Map(map_field, _) = arrow_schema.field(5).data_type() {
                        map_field.clone()
                    } else {
                        unreachable!()
                    }
                },
                offset_buffer,
                struct_array,
                null_buffer,
                ordered,
            )
        }) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5,
        ])
        .unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            location_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 1024);
        assert_eq!(
            *data_file.value_counts(),
            HashMap::from([
                (0, 1024),
                (5, 1024),
                (6, 1024),
                (2, 1024),
                (7, 1024),
                (9, 1024),
                (11, 1024),
                (13, (1..1025).sum()),
            ])
        );
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::long(0)),
                (5, Datum::long(0)),
                (6, Datum::long(0)),
                (2, Datum::string("0")),
                (7, Datum::long(0)),
                (9, Datum::long(0)),
                (11, Datum::string("0")),
                (13, Datum::long(0))
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::long(1023)),
                (5, Datum::long(1023)),
                (6, Datum::long(1023)),
                (2, Datum::string("999")),
                (7, Datum::long(1023)),
                (9, Datum::long(1023)),
                (11, Datum::string("999")),
                (13, Datum::long(1023))
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_type_for_write() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let loccation_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // prepare data
        // generate iceberg schema for all type
        let schema = schema_for_all_type();
        let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());
        let col0 = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ])) as ArrayRef;
        let col1 = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])) as ArrayRef;
        let col3 = Arc::new(arrow_array::Float32Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col4 = Arc::new(arrow_array::Float64Array::from(vec![
            Some(0.5),
            Some(2.0),
            None,
            Some(3.5),
        ])) as ArrayRef;
        let col5 = Arc::new(arrow_array::StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("d"),
        ])) as ArrayRef;
        let col6 = Arc::new(arrow_array::LargeBinaryArray::from_opt_vec(vec![
            Some(b"one"),
            None,
            Some(b""),
            Some(b"zzzz"),
        ])) as ArrayRef;
        let col7 = Arc::new(arrow_array::Date32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col8 = Arc::new(arrow_array::Time64MicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col9 = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col10 = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col11 = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ])) as ArrayRef;
        let col12 = Arc::new(
            arrow_array::TimestampNanosecondArray::from(vec![Some(0), Some(1), None, Some(3)])
                .with_timezone_utc(),
        ) as ArrayRef;
        let col13 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(10, 5)
                .unwrap(),
        ) as ArrayRef;
        let col14 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(Uuid::from_u128(0).as_bytes().to_vec()),
                    Some(Uuid::from_u128(1).as_bytes().to_vec()),
                    None,
                    Some(Uuid::from_u128(3).as_bytes().to_vec()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ) as ArrayRef;
        let col15 = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                    Some(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
                    None,
                    Some(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
                ]
                .into_iter(),
                10,
            )
            .unwrap(),
        ) as ArrayRef;
        let col16 = Arc::new(
            arrow_array::Decimal128Array::from(vec![Some(1), Some(2), None, Some(100)])
                .with_precision_and_scale(38, 5)
                .unwrap(),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13,
            col14, col15, col16,
        ])
        .unwrap();

        // write data
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(schema),
            file_io.clone(),
            loccation_gen,
            file_name_gen,
        )
        .build()
        .await?;
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            // Put dummy field for build successfully.
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();

        // check data file
        assert_eq!(data_file.record_count(), 4);
        assert!(data_file.value_counts().iter().all(|(_, &v)| { v == 4 }));
        assert!(data_file
            .null_value_counts()
            .iter()
            .all(|(_, &v)| { v == 1 }));
        assert_eq!(
            *data_file.lower_bounds(),
            HashMap::from([
                (0, Datum::bool(false)),
                (1, Datum::int(1)),
                (2, Datum::long(1)),
                (3, Datum::float(0.5)),
                (4, Datum::double(0.5)),
                (5, Datum::string("a")),
                (6, Datum::binary(vec![])),
                (7, Datum::date(0)),
                (8, Datum::time_micros(0).unwrap()),
                (9, Datum::timestamp_micros(0)),
                (10, Datum::timestamptz_micros(0)),
                (11, Datum::timestamp_nanos(0)),
                (12, Datum::timestamptz_nanos(0)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(0))),
                (15, Datum::fixed(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(1)
                    )
                ),
            ])
        );
        assert_eq!(
            *data_file.upper_bounds(),
            HashMap::from([
                (0, Datum::bool(true)),
                (1, Datum::int(4)),
                (2, Datum::long(4)),
                (3, Datum::float(3.5)),
                (4, Datum::double(3.5)),
                (5, Datum::string("d")),
                (6, Datum::binary(vec![122, 122, 122, 122])),
                (7, Datum::date(3)),
                (8, Datum::time_micros(3).unwrap()),
                (9, Datum::timestamp_micros(3)),
                (10, Datum::timestamptz_micros(3)),
                (11, Datum::timestamp_nanos(3)),
                (12, Datum::timestamptz_nanos(3)),
                (
                    13,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 10,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
                (14, Datum::uuid(Uuid::from_u128(3))),
                (
                    15,
                    Datum::fixed(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
                ),
                (
                    16,
                    Datum::new(
                        PrimitiveType::Decimal {
                            precision: 38,
                            scale: 5
                        },
                        PrimitiveLiteral::Int128(100)
                    )
                ),
            ])
        );

        // check the written file
        check_parquet_data_file(&file_io, &data_file, &to_write).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_bound() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let loccation_gen =
            MockLocationGenerator::new(temp_dir.path().to_str().unwrap().to_string());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // test 1.1 and 2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    0,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 28,
                        scale: 10,
                    }),
                )
                .into()])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
            file_io.clone(),
            loccation_gen.clone(),
            file_name_gen.clone(),
        )
        .build()
        .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(22000000000), Some(11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(Decimal::new(22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(Decimal::new(11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test -1.1 and -2.2
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    0,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 28,
                        scale: 10,
                    }),
                )
                .into()])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema.clone(),
            file_io.clone(),
            loccation_gen.clone(),
            file_name_gen.clone(),
        )
        .build()
        .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![Some(-22000000000), Some(-11000000000)])
                .with_data_type(DataType::Decimal128(28, 10)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal_with_precision(Decimal::new(-11000000000_i64, 10), 28).unwrap())
                .as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal_with_precision(Decimal::new(-22000000000_i64, 10), 28).unwrap())
                .as_ref()
        );

        // test max and min of rust_decimal
        let decimal_max = Decimal::MAX;
        let decimal_min = Decimal::MIN;
        assert_eq!(decimal_max.scale(), decimal_min.scale());
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    0,
                    "decimal",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 38,
                        scale: decimal_max.scale(),
                    }),
                )
                .into()])
                .build()
                .unwrap(),
        );
        let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let mut pw = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            schema,
            file_io.clone(),
            loccation_gen,
            file_name_gen,
        )
        .build()
        .await?;
        let col0 = Arc::new(
            Decimal128Array::from(vec![
                Some(decimal_max.mantissa()),
                Some(decimal_min.mantissa()),
            ])
            .with_data_type(DataType::Decimal128(38, 0)),
        ) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        pw.write(&to_write).await?;
        let res = pw.close().await?;
        assert_eq!(res.len(), 1);
        let data_file = res
            .into_iter()
            .next()
            .unwrap()
            .content(crate::spec::DataContentType::Data)
            .partition(Struct::empty())
            .build()
            .unwrap();
        assert_eq!(
            data_file.upper_bounds().get(&0),
            Some(Datum::decimal(decimal_max).unwrap()).as_ref()
        );
        assert_eq!(
            data_file.lower_bounds().get(&0),
            Some(Datum::decimal(decimal_min).unwrap()).as_ref()
        );

        // test max and min for scale 38
        // # TODO
        // Readd this case after resolve https://github.com/apache/iceberg-rust/issues/669
        // let schema = Arc::new(
        //     Schema::builder()
        //         .with_fields(vec![NestedField::optional(
        //             0,
        //             "decimal",
        //             Type::Primitive(PrimitiveType::Decimal {
        //                 precision: 38,
        //                 scale: 0,
        //             }),
        //         )
        //         .into()])
        //         .build()
        //         .unwrap(),
        // );
        // let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        // let mut pw = ParquetWriterBuilder::new(
        //     WriterProperties::builder().build(),
        //     schema,
        //     file_io.clone(),
        //     loccation_gen,
        //     file_name_gen,
        // )
        // .build()
        // .await?;
        // let col0 = Arc::new(
        //     Decimal128Array::from(vec![
        //         Some(99999999999999999999999999999999999999_i128),
        //         Some(-99999999999999999999999999999999999999_i128),
        //     ])
        //     .with_data_type(DataType::Decimal128(38, 0)),
        // ) as ArrayRef;
        // let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col0]).unwrap();
        // pw.write(&to_write).await?;
        // let res = pw.close().await?;
        // assert_eq!(res.len(), 1);
        // let data_file = res
        //     .into_iter()
        //     .next()
        //     .unwrap()
        //     .content(crate::spec::DataContentType::Data)
        //     .partition(Struct::empty())
        //     .build()
        //     .unwrap();
        // assert_eq!(
        //     data_file.upper_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );
        // assert_eq!(
        //     data_file.lower_bounds().get(&0),
        //     Some(Datum::new(
        //         PrimitiveType::Decimal {
        //             precision: 38,
        //             scale: 0
        //         },
        //         PrimitiveLiteral::Int128(-99999999999999999999999999999999999999_i128)
        //     ))
        //     .as_ref()
        // );

        Ok(())
    }
}
