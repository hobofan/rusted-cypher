extern crate serde;
extern crate tokio;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate rusted_cypher;

use rusted_cypher::cypher::result::Row;
use rusted_cypher::GraphClient;
use tokio::runtime::Runtime;

const URI: &'static str = "http://neo4j:neo4j@127.0.0.1:7474/db/data/";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Language {
    name: String,
    level: String,
    safe: bool,
}

#[test]
fn without_params() {
    let mut rt = Runtime::new().unwrap();
    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let stmt = cypher_stmt!("MATCH (n:NTLY_INTG_TEST_MACROS_1) RETURN n").unwrap();

    let result = rt.block_on(graph.exec(stmt));
    assert!(result.is_ok());
}

#[test]
fn save_retrive_struct() {
    let mut rt = Runtime::new().unwrap();
    let rust = Language {
        name: "Rust".to_owned(),
        level: "low".to_owned(),
        safe: true,
    };

    let graph = rt.block_on(GraphClient::connect(URI)).unwrap();

    let stmt = cypher_stmt!("CREATE (n:NTLY_INTG_TEST_MACROS_2 {lang}) RETURN n", {
        "lang" => &rust
    })
    .unwrap();

    let results = rt.block_on(graph.exec(stmt)).unwrap();
    let rows: Vec<Row> = results.rows().take(1).collect();
    let row = rows.first().unwrap();

    let lang: Language = row.get("n").unwrap();

    assert_eq!(rust, lang);

    rt.block_on(graph.exec("MATCH (n:NTLY_INTG_TEST_MACROS_2) DELETE n"))
        .unwrap();
}
