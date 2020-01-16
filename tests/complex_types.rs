extern crate hyper;
extern crate serde;
extern crate tokio;

#[macro_use]
extern crate serde_derive;

extern crate rusted_cypher;

use futures::prelude::*;
use rusted_cypher::cypher::result::Row;
use rusted_cypher::{GraphClient, Statement};
use tokio::runtime::Runtime;

const URI: &'static str = "http://neo4j:neo4j@127.0.0.1:7474/db/data/";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Language {
    name: String,
    level: String,
    safe: bool,
}

#[test]
fn save_retrieve_struct() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_1 {lang}) RETURN n")
        .with_param("lang", &rust)
        .unwrap();

    let results = rt.block_on(graph.exec(statement)).unwrap();
    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_1) DELETE n"))
        .unwrap();
}

#[test]
fn transaction_create_on_begin_commit() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_2 {lang})")
        .with_param("lang", &rust)
        .unwrap();

    let transaction = rt
        .block_on(graph.transaction().with_statement(statement).begin())
        .unwrap()
        .0;

    rt.block_on(transaction.commit()).unwrap();

    let results = rt
        .block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_2) RETURN n"))
        .unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_2) DELETE n"))
        .unwrap();
}

#[test]
fn transaction_create_after_begin_commit() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();
    let mut transaction = rt.block_on(graph.transaction().begin()).unwrap().0;

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_3 {lang})")
        .with_param("lang", &rust)
        .unwrap();

    rt.block_on(transaction.exec(statement)).unwrap();
    rt.block_on(transaction.commit()).unwrap();

    let results = rt
        .block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_3) RETURN n"))
        .unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_3) DELETE n"))
        .unwrap();
}

#[test]
fn transaction_create_on_commit() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };
    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_4 {lang})")
        .with_param("lang", &rust)
        .unwrap();

    let mut transaction = rt.block_on(graph.transaction().begin()).unwrap().0;
    transaction.add_statement(statement);
    rt.block_on(transaction.commit()).unwrap();

    let results = rt
        .block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_4) RETURN n"))
        .unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_4) DELETE n"))
        .unwrap();
}

#[test]
fn transaction_create_on_begin_rollback() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_5 {lang})")
        .with_param("lang", &rust)
        .unwrap();

    let mut transaction = rt
        .block_on(graph.transaction().with_statement(statement).begin())
        .unwrap()
        .0;

    let results = rt
        .block_on(transaction.exec("MATCH (n:NTLY_INTG_TEST_5) RETURN n"))
        .unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(transaction.rollback()).unwrap();

    let results = rt
        .block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_5) RETURN n"))
        .unwrap();

    assert_eq!(0, results.rows().count());
}

#[test]
fn transaction_create_after_begin_rollback() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let statement = Statement::new("CREATE (n:NTLY_INTG_TEST_6 {lang})")
        .with_param("lang", &rust)
        .unwrap();

    let mut transaction = rt.block_on(graph.transaction().begin()).unwrap().0;
    rt.block_on(transaction.exec(statement)).unwrap();

    let results = rt
        .block_on(transaction.exec("MATCH (n:NTLY_INTG_TEST_6) RETURN n"))
        .unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(transaction.rollback().map_err(|_| ()).map(|_| ()));

    let results = rt
        .block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_6) RETURN n"))
        .unwrap();

    assert_eq!(0, results.rows().count());
}
