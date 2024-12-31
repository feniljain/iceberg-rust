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

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread::sleep;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_hms::{HmsCatalog, HmsCatalogConfig, HmsThriftTransport};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;

const REST_CATALOG_PORT: u16 = 8181;
const HMS_CATALOG_PORT: u16 = 9083;
const MINIO_PORT: u16 = 9000;

pub struct TestFixture {
    pub _docker_compose: DockerCompose,
    pub rest_catalog: RestCatalog,
    pub hms_catalog: HmsCatalog,
}

pub async fn set_test_fixture(func: &str) -> TestFixture {
    set_up();
    let docker_compose = DockerCompose::new(
        normalize_test_name(format!("{}_{func}", module_path!())),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );

    // Start docker compose
    docker_compose.run();

    let rest_catalog_ip = docker_compose.get_container_ip("rest");
    // let hms_catalog_ip = docker_compose.get_container_ip("hive-metastore");
    let minio_ip = docker_compose.get_container_ip("minio");

    let hms_ip = Ipv4Addr::new(44, 196, 250, 225);
    let hms_socket_addr = SocketAddr::new(IpAddr::V4(hms_ip), 9083);

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}:{}", rest_catalog_ip, REST_CATALOG_PORT))
        .props(HashMap::from([
            (
                S3_ENDPOINT.to_string(),
                format!("http://{}:{}", minio_ip, MINIO_PORT),
            ),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]))
        .build();
    let rest_catalog = RestCatalog::new(config);

    println!("FENIL::rest catalog created");

    // let hms_socket_addr = SocketAddr::new(hms_ip, HMS_CATALOG_PORT);
    // while !scan_port_addr(hms_socket_addr) {
    //     println!("scan hms_socket_addr {} check", hms_socket_addr);
    //     println!("Waiting for 1s hms catalog to ready...");
    //     sleep(std::time::Duration::from_millis(1000));
    // }

    let warehouse = String::from("s3a://kafka-testing-files/iceberg_hive_test");
    // let namespace = String::from("risingwave_iceberg_hive");

    let catalog_config = HmsCatalogConfig::builder()
        .address(hms_socket_addr.to_string())
        .warehouse(warehouse)
        .thrift_transport(HmsThriftTransport::Buffered)
        .props(HashMap::from([
            (
                "s3.endpoint".to_string(),
                "https://s3.us-east-1.amazonaws.com/".to_string(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
            // (
            //     "s3.access-key-id".to_string(),
            //     "asdf".to_string(),
            // ),
            // (
            //     "s3.secret-access-key".to_string(),
            //     "adfad".to_string(),
            // ),
        ]))
        .build();
    let hms_catalog = HmsCatalog::new(catalog_config).expect("could not build HMS catalog");

    println!("FENIL::HMS catalog created");

    TestFixture {
        _docker_compose: docker_compose,
        rest_catalog,
        hms_catalog,
    }
}
