//! Provides structs used to interact with the cypher transaction endpoint

pub mod result;
pub mod statement;
pub mod transaction;

pub use self::result::CypherResult;
pub use self::statement::Statement;
pub use self::transaction::Transaction;

use std::collections::BTreeMap;

use futures::prelude::*;
use hyper::header::HeaderMap;
use hyper::Request;
use hyper::Response;
use serde::de::DeserializeOwned;
use serde_json::de as json_de;
use serde_json::ser as json_ser;
use serde_json::value as json_value;
use serde_json::{self, Value};
use url::Url;

use self::result::{QueryResult, ResultTrait};
use self::transaction::Client;
use crate::error::GraphError;

async fn send_query(
    client: &Client,
    endpoint: &str,
    headers: HeaderMap,
    statements: Vec<Statement>,
) -> Result<Response<hyper::Body>, GraphError> {
    let mut json = BTreeMap::new();
    json.insert("statements", statements);

    let json = serde_json::to_vec(&json).unwrap();

    debug!(
        "Sending query:\n{}",
        json_ser::to_string_pretty(&json).unwrap_or(String::new())
    );
    let mut req = Request::post(endpoint);
    for (k, v) in headers {
        req.header(k.unwrap(), v);
    }
    let req = req.body(json.into()).unwrap();

    client.request(req).await.map_err(From::from)
}

async fn parse_response<T: DeserializeOwned + ResultTrait>(
    res: &mut Response<hyper::Body>,
) -> Result<T, GraphError> {
    let body_bytes: Vec<u8> = res.body_mut().try_concat().await?.to_vec();
    let result: Value = json_de::from_slice(&body_bytes)?;

    if let Some(errors) = result.get("errors") {
        if errors.as_array().map(|a| a.len()).unwrap_or(0) > 0 {
            return Err(GraphError::Neo4j(json_value::from_value(errors.clone())?));
        }
    }

    json_value::from_value::<T>(result).map_err(|e| {
        error!("Unable to parse response: {}", &e);
        From::from(e)
    })
}

/// Represents the cypher endpoint of a neo4j server
///
/// The `Cypher` struct holds information about the cypher enpoint. It is used to create the queries
/// that are sent to the server.
pub struct Cypher {
    endpoint: Url,
    client: Client,
    headers: HeaderMap,
}

impl Cypher {
    /// Creates a new Cypher
    ///
    /// Its arguments are the cypher transaction endpoint and the HTTP headers containing HTTP
    /// Basic Authentication, if needed.
    pub fn new(endpoint: Url, client: Client, headers: HeaderMap) -> Self {
        Cypher {
            endpoint: endpoint,
            client: client,
            headers: headers,
        }
    }

    fn endpoint_commit(&self) -> String {
        format!("{}/{}", &self.endpoint, "commit")
    }

    fn client(&self) -> &Client {
        &self.client
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Creates a new `CypherQuery`
    pub fn query(&self) -> CypherQuery {
        CypherQuery {
            statements: Vec::new(),
            cypher: &self,
        }
    }

    /// Executes the given `Statement`
    ///
    /// Parameter can be anything that implements `Into<Statement>`, `Into<String>` or `Statement`
    /// itself
    pub async fn exec<S: Into<Statement>>(&self, statement: S) -> Result<CypherResult, GraphError> {
        let mut results = self.query().with_statement(statement).send().await?;

        results.pop().ok_or(GraphError::Other(
            "No results returned from server".to_owned(),
        ))
    }

    /// Creates a new `Transaction`
    pub fn transaction(&self) -> Transaction<self::transaction::Created> {
        Transaction::new(&self.endpoint.to_string(), &self.headers)
    }
}

/// Represents a cypher query
///
/// A cypher query is composed by statements, each one containing the query itself and its
/// parameters.
///
/// The query parameters must implement `Serialize` so they can be serialized into JSON in order to
/// be sent to the server
pub struct CypherQuery<'a> {
    statements: Vec<Statement>,
    cypher: &'a Cypher,
}

impl<'a> CypherQuery<'a> {
    /// Adds statements in builder style
    pub fn with_statement<T: Into<Statement>>(mut self, statement: T) -> Self {
        self.add_statement(statement);
        self
    }

    pub fn add_statement<T: Into<Statement>>(&mut self, statement: T) {
        self.statements.push(statement.into());
    }

    pub fn statements(&self) -> &Vec<Statement> {
        &self.statements
    }

    pub fn set_statements(&mut self, statements: Vec<Statement>) {
        self.statements = statements;
    }

    /// Sends the query to the server
    ///
    /// The statements contained in the query are sent to the server and the results are parsed
    /// into a `Vec<CypherResult>` in order to match the response of the neo4j api.
    pub async fn send(self) -> Result<Vec<CypherResult>, GraphError> {
        let endpoint_commit = self.cypher.endpoint_commit();
        let mut res = send_query(
            self.cypher.client(),
            &endpoint_commit,
            self.cypher.headers().clone(),
            self.statements,
        )
        .await?;

        let result: QueryResult = parse_response(&mut res).await?;
        if result.errors().len() > 0 {
            return Err(GraphError::Neo4j(result.errors().clone()));
        }

        Ok(result.results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use headers::HeaderMapExt;
    use tokio::runtime::Runtime;

    use crate::cypher::result::Row;

    fn get_cypher() -> Cypher {
        use headers::{Authorization, ContentType};
        use hyper::header::HeaderMap;
        use url::Url;

        let cypher_endpoint = Url::parse("http://localhost:7474/db/data/transaction").unwrap();

        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::basic("neo4j", "neo4j"));
        headers.typed_insert(ContentType::json());

        Cypher::new(cypher_endpoint, Client::new("http"), headers)
    }

    #[test]
    fn query_without_params() {
        let result = Runtime::new()
            .unwrap()
            .block_on(get_cypher().exec("MATCH (n:TEST_CYPHER) RETURN n"))
            .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "n");
    }

    #[test]
    fn query_with_string_param() {
        let statement = Statement::new("MATCH (n:TEST_CYPHER {name: {name}}) RETURN n")
            .with_param("name", "Neo")
            .unwrap();

        let result = Runtime::new()
            .unwrap()
            .block_on(get_cypher().exec(statement))
            .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "n");
    }

    #[test]
    fn query_with_int_param() {
        let statement = Statement::new("MATCH (n:TEST_CYPHER {value: {value}}) RETURN n")
            .with_param("value", 42)
            .unwrap();

        let result = Runtime::new()
            .unwrap()
            .block_on(get_cypher().exec(statement))
            .unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "n");
    }

    #[test]
    fn query_with_complex_param() {
        #[derive(Serialize, Deserialize)]
        pub struct ComplexType {
            pub name: String,
            pub value: i32,
        }

        let mut rt = Runtime::new().unwrap();
        let cypher = get_cypher();

        let complex_param = ComplexType {
            name: "Complex".to_owned(),
            value: 42,
        };

        let statement = Statement::new("CREATE (n:TEST_CYPHER_COMPLEX_PARAM {p})")
            .with_param("p", &complex_param)
            .unwrap();

        let result = rt.block_on(cypher.exec(statement));
        assert!(result.is_ok());

        let results = rt
            .block_on(cypher.exec("MATCH (n:TEST_CYPHER_COMPLEX_PARAM) RETURN n"))
            .unwrap();
        let rows: Vec<Row> = results.rows().take(1).collect();
        let row = rows.first().unwrap();

        let complex_result: ComplexType = row.get("n").unwrap();
        assert_eq!(complex_result.name, "Complex");
        assert_eq!(complex_result.value, 42);

        rt.block_on(cypher.exec("MATCH (n:TEST_CYPHER_COMPLEX_PARAM) DELETE n"))
            .unwrap();
    }

    #[test]
    fn query_with_multiple_params() {
        let statement =
            Statement::new("MATCH (n:TEST_CYPHER {name: {name}}) WHERE n.value = {value} RETURN n")
                .with_param("name", "Neo")
                .unwrap()
                .with_param("value", 42)
                .unwrap();

        let result = Runtime::new()
            .unwrap()
            .block_on(get_cypher().exec(statement))
            .unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "n");
    }

    #[test]
    fn multiple_queries() {
        let cypher = get_cypher();
        let statement1 = Statement::new("MATCH (n:TEST_CYPHER) RETURN n");
        let statement2 = Statement::new("MATCH (n:TEST_CYPHER) RETURN n");

        let query = cypher
            .query()
            .with_statement(statement1)
            .with_statement(statement2);

        let results = Runtime::new().unwrap().block_on(query.send()).unwrap();
        assert_eq!(results.len(), 2);
    }
}
