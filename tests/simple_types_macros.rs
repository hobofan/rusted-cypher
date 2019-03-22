#[macro_use]
extern crate rusted_cypher;
extern crate tokio;

use rusted_cypher::cypher::result::Row;
use rusted_cypher::GraphClient;
use tokio::runtime::Runtime;

const URI: &'static str = "http://neo4j:neo4j@127.0.0.1:7474/db/data/";

#[test]
fn without_params() {
    let mut rt = Runtime::new().unwrap();
    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let stmt = cypher_stmt!("MATCH (n:INTG_TEST_MACROS_1) RETURN n").unwrap();

    let result = rt.block_on(graph.exec(stmt));
    assert!(result.is_ok());
}

#[test]
fn save_retrive_values() {
    let mut rt = Runtime::new().unwrap();
    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let stmt = cypher_stmt!(
        "CREATE (n:INTG_TEST_MACROS_2 {name: {name}, level: {level}, safe: {safe}}) RETURN n.name, n.level, n.safe", {
            "name" => "Rust",
            "level" => "low",
            "safe" => true
        }
    ).unwrap();

    let results = rt.block_on(graph.exec(stmt)).unwrap();

    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let name: String = row.get("n.name").unwrap();
    let level: String = row.get("n.level").unwrap();
    let safe: bool = row.get("n.safe").unwrap();

    assert_eq!("Rust", name);
    assert_eq!("low", level);
    assert_eq!(true, safe);

    rt.block_on(graph.exec("MATCH (n:INTG_TEST_MACROS_2) DELETE n"))
        .unwrap();
}
