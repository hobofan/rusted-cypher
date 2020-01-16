//! Transaction management through neo4j's transaction endpoint
//!
//! The recommended way to start a transaction is through the `GraphClient`
//!
//! # Examples
//!
//! ## Starting a transaction
//! ```
//! # use rusted_cypher::{GraphClient, GraphError};
//! # const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data";
//! # fn main() { doctest().unwrap(); }
//! # #[allow(unused_variables)]
//! # fn doctest() -> Result<(), GraphError> {
//! # let graph = GraphClient::connect(URL)?;
//! let mut transaction = graph.transaction();
//! transaction.add_statement("MATCH (n:TRANSACTION) RETURN n");
//!
//! let (transaction, results) = transaction.begin()?;
//! # transaction.rollback()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Statement is optional when beggining a transaction
//! ```
//! # use rusted_cypher::{GraphClient, GraphError};
//! # const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data";
//! # fn main() { doctest().unwrap(); }
//! # #[allow(unused_variables)]
//! # fn doctest() -> Result<(), GraphError> {
//! # let graph = GraphClient::connect(URL)?;
//! let (transaction, _) = graph.transaction().begin()?;
//! # transaction.rollback()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Send queries in a started transaction
//! ```
//! # use rusted_cypher::{GraphClient, GraphError};
//! # const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data";
//! # fn main() { doctest().unwrap(); }
//! # fn doctest() -> Result<(), GraphError> {
//! # let graph = GraphClient::connect(URL)?;
//! # let (mut transaction, _) = graph.transaction().begin()?;
//! // Send a single query
//! let result = transaction.exec("MATCH (n:TRANSACTION) RETURN n")?;
//!
//! // Send multiple queries
//! let results = transaction
//!     .with_statement("MATCH (n:TRANSACTION) RETURN n")
//!     .with_statement("MATCH (n:OTHER_TRANSACTION) RETURN n")
//!     .send()?;
//! # assert_eq!(results.len(), 2);
//! # transaction.rollback()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Commit a transaction
//! ```
//! # use rusted_cypher::{GraphClient, GraphError};
//! # const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data";
//! # fn main() { doctest().unwrap(); }
//! # fn doctest() -> Result<(), GraphError> {
//! # let graph = GraphClient::connect(URL)?;
//! # let (mut transaction, _) = graph.transaction().begin()?;
//! transaction.exec("CREATE (n:TRANSACTION)")?;
//! transaction.commit()?;
//!
//! // Send more statements when commiting
//! # let (mut transaction, _) = graph.transaction().begin()?;
//! let results = transaction.with_statement(
//!     "MATCH (n:TRANSACTION) RETURN n")
//!     .send()?;
//! # assert_eq!(results[0].data.len(), 1);
//! # transaction.rollback()?;
//! # graph.exec("MATCH (n:TRANSACTION) DELETE n")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Rollback a transaction
//! ```
//! # use rusted_cypher::{GraphClient, GraphError};
//! # const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data";
//! # fn main() { doctest().unwrap(); }
//! # fn doctest() -> Result<(), GraphError> {
//! # let graph = GraphClient::connect(URL)?;
//! # let (mut transaction, _) = graph.transaction().begin()?;
//! transaction.exec("CREATE (n:TRANSACTION)")?;
//! transaction.rollback()?;
//! # let result = graph.exec("MATCH (n:TRANSACTION) RETURN n")?;
//! # assert_eq!(result.data.len(), 0);
//! # Ok(())
//! # }
//! ```

use futures::prelude::*;
use hyper::{
    client::HttpConnector,
    client::ResponseFuture,
    header::{HeaderMap, LOCATION},
    Client as HyperClient, Request,
};
use hyper_tls::HttpsConnector;
use std::any::Any;
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex};
use time::{self, Tm};

use super::result::{CypherResult, ResultTrait};
use super::statement::Statement;
use crate::error::{GraphError, Neo4jError};

const DATETIME_RFC822: &'static str = "%a, %d %b %Y %T %Z";

pub struct Created;
pub struct Started;

#[derive(Debug, Deserialize)]
struct TransactionInfo {
    expires: String,
}

#[derive(Debug, Deserialize)]
struct TransactionResult {
    commit: String,
    transaction: TransactionInfo,
    results: Vec<CypherResult>,
    errors: Vec<Neo4jError>,
}

impl ResultTrait for TransactionResult {
    fn results(&self) -> &Vec<CypherResult> {
        &self.results
    }

    fn errors(&self) -> &Vec<Neo4jError> {
        &self.errors
    }
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CommitResult {
    results: Vec<CypherResult>,
    errors: Vec<Neo4jError>,
}

impl ResultTrait for CommitResult {
    fn results(&self) -> &Vec<CypherResult> {
        &self.results
    }

    fn errors(&self) -> &Vec<Neo4jError> {
        &self.errors
    }
}

pub enum Client<B = hyper::Body> {
    HttpClient(HyperClient<HttpConnector, B>),
    HttpsClient(HyperClient<HttpsConnector<HttpConnector>, B>),
}

impl Client<hyper::Body> {
    pub fn new(scheme: &str) -> Client {
        match scheme {
            "https" => Client::HttpsClient(HyperClient::builder().build(HttpsConnector::new())),
            "http" => Client::HttpClient(HyperClient::new()),
            _ => panic!("Unknown scheme \"{}\" for client uri.", scheme),
        }
    }

    pub fn request(&self, req: Request<hyper::Body>) -> ResponseFuture {
        match self {
            Client::HttpClient(client) => client.request(req),
            Client::HttpsClient(client) => client.request(req),
        }
    }
}

/// Provides methods to interact with a transaction
///
/// This struct is used to begin a transaction, send queries, commit an rollback a transaction.
/// Some methods are provided depending on the state of the transaction, for example,
/// `Transaction::begin` is provided on a `Created` transaction and `Transaction::commit` is provided
/// on `Started` transaction
pub struct Transaction<State: Any = Created> {
    transaction: String,
    commit: String,
    expires: Arc<Mutex<Tm>>,
    client: Client,
    headers: HeaderMap,
    statements: Vec<Statement>,
    _state: PhantomData<State>,
}

impl<State: Any> Transaction<State> {
    /// Adds a statement to the transaction
    pub fn add_statement<S: Into<Statement>>(&mut self, statement: S) {
        self.statements.push(statement.into());
    }

    /// Gets the expiration time of the transaction
    pub fn get_expires(&self) -> std::sync::MutexGuard<Tm> {
        self.expires.lock().unwrap()
    }
}

impl Transaction<Created> {
    pub fn new(endpoint: &str, headers: &HeaderMap) -> Transaction<Created> {
        Transaction {
            transaction: endpoint.to_owned(),
            commit: endpoint.to_owned(),
            expires: Arc::new(Mutex::new(time::now_utc())),
            client: Client::new(url::Url::parse(endpoint).unwrap().scheme()),
            headers: headers.to_owned(),
            statements: vec![],
            _state: PhantomData,
        }
    }

    /// Adds a statement to the transaction in builder style
    pub fn with_statement<S: Into<Statement>>(mut self, statement: S) -> Self {
        self.add_statement(statement);
        self
    }

    /// Begins the transaction
    ///
    /// Consumes the `Transaction<Created>` and returns the a `Transaction<Started>` alongside with
    /// the results of any `Statement` sent.
    pub async fn begin(self) -> Result<(Transaction<Started>, Vec<CypherResult>), GraphError> {
        debug!("Beginning transaction");

        let statements = self.statements.clone();
        let mut res = super::send_query(
            &self.client,
            &self.transaction,
            self.headers.clone(),
            statements,
        )
        .await?;

        let mut result: TransactionResult = super::parse_response(&mut res).await?;

        let transaction = res
            .headers()
            .get(LOCATION)
            .map(|location| location.to_str().unwrap().to_owned())
            .ok_or_else(|| {
                error!("No transaction URI returned from server");
                GraphError::Transaction("No transaction URI returned from server".to_owned())
            })?;

        let expires = time::strptime(&mut result.transaction.expires, DATETIME_RFC822)?;

        debug!(
            "Transaction started at {}, expires in {}",
            transaction,
            expires.rfc822z()
        );

        let transaction = Transaction {
            transaction: transaction,
            commit: result.commit,
            expires: Arc::new(Mutex::new(expires)),
            client: self.client,
            headers: self.headers,
            statements: Vec::new(),
            _state: PhantomData,
        };

        Ok((transaction, result.results))
    }
}

impl Transaction<Started> {
    /// Adds a statement to the transaction in builder style
    pub fn with_statement<S: Into<Statement>>(&mut self, statement: S) -> &mut Self {
        self.add_statement(statement);
        self
    }

    /// Executes the given statement
    ///
    /// Any statements added via `add_statement` or `with_statement` will be discarded
    pub async fn exec<S: Into<Statement>>(
        &mut self,
        statement: S,
    ) -> Result<CypherResult, GraphError> {
        self.statements.clear();
        self.add_statement(statement);

        let mut results = self.send().await?;

        let result = results.pop().ok_or(GraphError::Statement(
            "Server returned no results".to_owned(),
        ))?;

        Ok(result)
    }

    /// Executes the statements added via `add_statement` or `with_statement`
    pub async fn send(&mut self) -> Result<Vec<CypherResult>, GraphError> {
        let mut statements = vec![];
        mem::swap(&mut statements, &mut self.statements);
        let expires = self.expires.clone();

        let mut res = super::send_query(
            &self.client,
            &self.transaction,
            self.headers.clone(),
            statements,
        )
        .await?;

        let mut result: TransactionResult = super::parse_response(&mut res).await?;
        *expires.lock().unwrap() =
            time::strptime(&mut result.transaction.expires, DATETIME_RFC822)?;

        Ok(result.results.clone())
    }

    /// Commits the transaction, returning the results
    pub async fn commit(self) -> Result<Vec<CypherResult>, GraphError> {
        debug!("Commiting transaction {}", self.transaction);
        let statements = self.statements.clone();

        let mut res =
            super::send_query(&self.client, &self.commit, self.headers.clone(), statements).await?;

        let result: CommitResult = super::parse_response(&mut res).await?;
        debug!("Transaction commited {}", self.transaction);

        Ok(result.results)
    }

    /// Rollback the transaction
    pub async fn rollback(self) -> Result<(), GraphError> {
        debug!("Rolling back transaction {}", self.transaction);
        let mut req = Request::delete(&self.transaction);
        for (k, v) in self.headers.clone() {
            req.headers_mut().unwrap().insert(k.unwrap(), v);
        }
        let transaction = self.transaction;

        let mut res = self
            .client
            .request(req.body(hyper::Body::empty()).unwrap())
            .err_into::<GraphError>()
            .await?;

        super::parse_response::<CommitResult>(&mut res).await?;
        debug!("Transaction rolled back {}", transaction);
        Ok(())
    }

    /// Sends a query to just reset the transaction timeout
    ///
    /// All transactions have a timeout. Use this method to keep a transaction alive.
    pub async fn reset_timeout(&mut self) -> Result<(), GraphError> {
        super::send_query(
            &self.client,
            &self.transaction,
            self.headers.clone(),
            vec![],
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use headers::{Authorization, ContentType, HeaderMapExt};
    use hyper::header::HeaderMap;

    const URL: &'static str = "http://neo4j:neo4j@localhost:7474/db/data/transaction";

    fn get_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();

        headers.typed_insert(Authorization::basic("neo4j", "neo4j"));
        headers.typed_insert(ContentType::json());

        headers
    }

    #[test]
    fn begin_transaction() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let headers = get_headers();
        let transaction = Transaction::new(URL, &headers);
        let result = rt.block_on(transaction.begin()).unwrap();
        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn create_node_and_commit() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let headers = get_headers();

        let (transaction, _) = rt
            .block_on(
                Transaction::new(URL, &headers)
                    .with_statement(
                        "CREATE (n:TEST_TRANSACTION_CREATE_COMMIT { name: 'Rust', safe: true })",
                    )
                    .begin(),
            )
            .unwrap();
        rt.block_on(transaction.commit()).unwrap();

        let (transaction, results) = rt
            .block_on(
                Transaction::new(URL, &headers)
                    .with_statement("MATCH (n:TEST_TRANSACTION_CREATE_COMMIT) RETURN n")
                    .begin(),
            )
            .unwrap();

        assert_eq!(results[0].data.len(), 1);

        rt.block_on(transaction.rollback()).unwrap();

        let started_transaction = rt
            .block_on(
                Transaction::new(URL, &headers)
                    .with_statement("MATCH (n:TEST_TRANSACTION_CREATE_COMMIT) DELETE n")
                    .begin(),
            )
            .unwrap();
        rt.block_on(started_transaction.0.commit()).unwrap();
    }

    #[test]
    fn create_node_and_rollback() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let headers = get_headers();

        let (mut transaction, _) = rt
            .block_on(
                Transaction::new(URL, &headers)
                    .with_statement(
                        "CREATE (n:TEST_TRANSACTION_CREATE_ROLLBACK { name: 'Rust', safe: true })",
                    )
                    .begin(),
            )
            .unwrap();

        let result = rt
            .block_on(transaction.exec("MATCH (n:TEST_TRANSACTION_CREATE_ROLLBACK) RETURN n"))
            .unwrap();

        assert_eq!(result.data.len(), 1);

        rt.block_on(transaction.rollback()).unwrap();

        let (transaction, results) = rt
            .block_on(
                Transaction::new(URL, &headers)
                    .with_statement("MATCH (n:TEST_TRANSACTION_CREATE_ROLLBACK) RETURN n")
                    .begin(),
            )
            .unwrap();

        assert_eq!(results[0].data.len(), 0);

        rt.block_on(transaction.rollback()).unwrap();
    }

    #[test]
    fn query_open_transaction() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let headers = get_headers();

        let (mut transaction, _) = rt
            .block_on(Transaction::new(URL, &headers).begin())
            .unwrap();

        let result = rt
            .block_on(transaction.exec(
                "CREATE (n:TEST_TRANSACTION_QUERY_OPEN { name: 'Rust', safe: true }) RETURN n",
            ))
            .unwrap();

        assert_eq!(result.data.len(), 1);

        rt.block_on(transaction.rollback()).unwrap();
    }
}
