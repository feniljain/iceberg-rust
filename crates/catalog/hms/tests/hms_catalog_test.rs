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

//! Integration tests for hms catalog.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use ctor::{ctor, dtor};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use parquet::file::properties::WriterProperties;
use port_scanner::scan_port_addr;
use tokio::time::sleep;

const HMS_CATALOG_PORT: u16 = 9083;
const MINIO_PORT: u16 = 9000;
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);
type Result<T> = std::result::Result<T, iceberg::Error>;

// #[ctor]
// fn before_all() {
//     let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
//     let docker_compose = DockerCompose::new(
//         normalize_test_name(module_path!()),
//         format!("{}/testdata/hms_catalog", env!("CARGO_MANIFEST_DIR")),
//     );
//     docker_compose.run();
//     guard.replace(docker_compose);
// }
//
// #[dtor]
// fn after_all() {
//     let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
//     guard.take();
// }

async fn get_catalog() -> HmsCatalog {
    set_up();

    let (hms_catalog_ip, minio_ip) = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        (
            docker_compose.get_container_ip("hive-metastore"),
            docker_compose.get_container_ip("minio"),
        )
    };
    let hms_socket_addr = SocketAddr::new(hms_catalog_ip, HMS_CATALOG_PORT);
    let minio_socket_addr = SocketAddr::new(minio_ip, MINIO_PORT);
    while !scan_port_addr(hms_socket_addr) {
        log::info!("scan hms_socket_addr {} check", hms_socket_addr);
        log::info!("Waiting for 1s hms catalog to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    let props = HashMap::from([
        (
            S3_ENDPOINT.to_string(),
            format!("http://{}", minio_socket_addr),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    let config = HmsCatalogConfig::builder()
        .address(hms_socket_addr.to_string())
        .thrift_transport(HmsThriftTransport::Buffered)
        .warehouse("s3a://warehouse/hive".to_string())
        .props(props)
        .build();

    HmsCatalog::new(config).unwrap()
}

async fn set_test_namespace(catalog: &HmsCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: impl ToString, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .location(location.to_string())
        .name(name.to_string())
        .properties(HashMap::new())
        .schema(schema)
        .build();

    Ok(creation)
}

#[tokio::test]
async fn test_rename_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation: TableCreation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_rename_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table: iceberg::table::Table = catalog.create_table(namespace.name(), creation).await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    catalog.rename_table(table.identifier(), &dest).await?;

    let result = catalog.table_exists(&dest).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_table_exists".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_drop_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    catalog.drop_table(table.identifier()).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let catalog = get_catalog().await;
    // let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_update_table".into()));
    // set_test_namespace(&catalog, namespace.name()).await?;

    // let expected = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    println!("result identifier: {:?}", result.identifier());
    println!("result metadata location: {:?}", result.metadata_location());
    println!("result metadata: {:?}", result.metadata());

    // assert_eq!(result.identifier(), expected.identifier());
    // assert_eq!(result.metadata_location(), expected.metadata_location());
    // assert_eq!(result.metadata(), expected.metadata());

    Ok(())
}

#[tokio::test]
async fn test_create_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_create_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let result = catalog.create_table(namespace.name(), creation).await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(result
        .metadata_location()
        .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-")));
    assert!(
        catalog
            .file_io()
            .exists("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let catalog = get_catalog().await;
    let ns = Namespace::new(NamespaceIdent::new("test_list_tables".into()));
    let result = catalog.list_tables(ns.name()).await?;
    set_test_namespace(&catalog, ns.name()).await?;

    assert_eq!(result, vec![]);

    let creation = set_table_creation("s3a://warehouse/hive", "my_table")?;
    catalog.create_table(ns.name(), creation).await?;
    let result = catalog.list_tables(ns.name()).await?;

    assert_eq!(result, vec![TableIdent::new(
        ns.name().clone(),
        "my_table".to_string()
    )]);

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let result_no_parent = catalog.list_namespaces(None).await?;

    let result_with_parent = catalog
        .list_namespaces(Some(&NamespaceIdent::new("parent".into())))
        .await?;

    assert!(result_no_parent.contains(&NamespaceIdent::new("default".into())));
    assert!(result_with_parent.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let properties = HashMap::from([
        ("comment".to_string(), "my_description".to_string()),
        ("location".to_string(), "my_location".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "apache".to_string(),
        ),
        (
            "hive.metastore.database.owner-type".to_string(),
            "user".to_string(),
        ),
        ("key1".to_string(), "value1".to_string()),
    ]);

    let ns = Namespace::with_properties(
        NamespaceIdent::new("test_create_namespace".into()),
        properties.clone(),
    );

    let result = catalog.create_namespace(ns.name(), properties).await?;

    assert_eq!(result, ns);

    Ok(())
}

#[tokio::test]
async fn test_get_default_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let properties = HashMap::from([
        ("location".to_string(), "s3a://warehouse/hive".to_string()),
        (
            "hive.metastore.database.owner-type".to_string(),
            "Role".to_string(),
        ),
        ("comment".to_string(), "Default Hive database".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "public".to_string(),
        ),
    ]);

    let expected = Namespace::with_properties(NamespaceIdent::new("default".into()), properties);

    let result = catalog.get_namespace(ns.name()).await?;

    assert_eq!(expected, result);

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let catalog = get_catalog().await;

    let ns_exists = Namespace::new(NamespaceIdent::new("default".into()));
    let ns_not_exists = Namespace::new(NamespaceIdent::new("test_namespace_exists".into()));

    let result_exists = catalog.namespace_exists(ns_exists.name()).await?;
    let result_not_exists = catalog.namespace_exists(ns_not_exists.name()).await?;

    assert!(result_exists);
    assert!(!result_not_exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = NamespaceIdent::new("test_update_namespace".into());
    set_test_namespace(&catalog, &ns).await?;
    let properties = HashMap::from([("comment".to_string(), "my_update".to_string())]);

    catalog.update_namespace(&ns, properties).await?;

    let db = catalog.get_namespace(&ns).await?;

    assert_eq!(
        db.properties().get("comment"),
        Some(&"my_update".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("delete_me".into()));

    catalog.create_namespace(ns.name(), HashMap::new()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(result);

    catalog.drop_namespace(ns.name()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_update_table() -> Result<()> {
    let catalog = get_catalog().await;
    let namespace = Namespace::new(NamespaceIdent::new("test_update_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let creation = set_table_creation("s3a://warehouse/hive/test_update_table.db/", "my_table")?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    // =====================

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = Int32Array::from(vec![Some(0), Some(1), Some(2)]);
    let col2 = StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let _ = tx.commit(&catalog).await.unwrap();

    // =====================

    Ok(())
}

fn get_external_hms_catalog() -> (HmsCatalog, String, String) {
    let hms_ip = Ipv4Addr::new(44, 196, 250, 225);
    let hms_socket_addr = SocketAddr::new(IpAddr::V4(hms_ip), 9083);

    let warehouse = String::from("s3a://kafka-testing-files/iceberg_hive_test");

    let catalog_config = HmsCatalogConfig::builder()
        // .address(hive_catalog_addr.to_string())
        .address(hms_socket_addr.to_string())
        .warehouse(warehouse)
        .thrift_transport(HmsThriftTransport::Buffered)
        .props(HashMap::from([
            (
                "s3.endpoint".to_string(),
                "https://s3.us-east-1.amazonaws.com/".to_string(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.access-key-id".to_string(), "adming".to_string()),
            ("s3.secret-access-key".to_string(), "password".to_string()),
        ]))
        .build();

    let catalog = HmsCatalog::new(catalog_config).expect("could not create catalog");

    (
        catalog,
        String::from("risingwave_iceberg_hive"),
        String::from("local_auth_t1"),
    )
}

fn get_internal_hms_catalog() -> (HmsCatalog, String, String) {
    let warehouse = String::from("s3a://warehouse/hive");

    let catalog_config = HmsCatalogConfig::builder()
        .address("localhost:9083".to_string())
        .warehouse(warehouse)
        .thrift_transport(HmsThriftTransport::Buffered)
        .props(HashMap::from([
            (
                "s3.endpoint".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("s3.access-key-id".to_string(), "admin".to_string()),
            ("s3.secret-access-key".to_string(), "password".to_string()),
        ]))
        .build();

    let catalog = HmsCatalog::new(catalog_config).expect("could not create catalog");

    (catalog, String::from("ns"), String::from("local_auth_t1"))
}

#[tokio::test]
async fn test_load_qh_table() -> Result<()> {
    let (catalog, namespace, tbl_name) = get_internal_hms_catalog();

    let namespace_id = NamespaceIdent::new(namespace);

    let result = catalog
        .load_table(&TableIdent::new(namespace_id.clone(), tbl_name))
        .await?;

    println!("DEBUG: result identifier: {:?}", result.identifier());
    println!(
        "DEBUG: result metadata location: {:?}",
        result.metadata_location()
    );
    println!("DEBUG: result metadata: {:?}", result.metadata());

    Ok(())
}

#[tokio::test]
async fn test_write_table() -> Result<()> {
    let (catalog, namespace, tbl_name) = get_internal_hms_catalog();

    let namespace_id = NamespaceIdent::new(namespace);

    let table_id = TableIdent::new(namespace_id, tbl_name);

    let table = catalog
        .load_table(&table_id)
        .await
        .expect("could not load table");

    // =====================

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();
    let col1 = Int32Array::from(vec![Some(0), Some(1), Some(2)]);
    let col2 = StringArray::from(vec![Some("foo"), Some("bar"), Some("baz")]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
    ])
    .unwrap();

    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let mut append_action = tx.fast_append(None, vec![]).unwrap();
    append_action.add_data_files(data_file.clone()).unwrap();
    let tx = append_action.apply().await.unwrap();
    let _ = tx.commit(&catalog).await.unwrap();

    // =====================

    Ok(())
}
