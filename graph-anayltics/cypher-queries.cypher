// All cypher commands are executed in the neo4j browser. 
// load ../data/neo4j/Collaborations.csv and ../data/neo4j/unique_authors.csv in import folder of neo4j database

// create researchers nodes
CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE;
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
"file:///unique_authors.csv" AS line
WITH line
match (n{name:line.`Unique Author` })
set n.position_num = line.`Job_title_num`

CALL apoc.create.node(['Person',line.`Job Title`],{name: line.`Unique Author`}) YIELD node
RETURN node
;

// create edges in graph 
:auto USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
"file:///collaborations.csv" AS line
WITH line

MATCH (name1:Person {name:line.Name1}), (name2:Person {name:line.Name2})
CREATE (name1)-[collab:COLLABORATED]->(name2)

SET collab.total_paper_count = CASE line.Total_Paper_Count WHEN "Unkown" THEN NULL ELSE toInteger(line.Total_Paper_Count) END
SET collab.total_citations_achieved = CASE line.Total_Citations_Achieved WHEN "Unkown" THEN NULL ELSE toInteger(line.Total_Citations_Achieved) END
;


// Betweenes centraility all of the graph
CALL gds.graph.create.cypher(
  'dcu_external_researchers',
  'MATCH (n:Person) RETURN id(n) AS id, labels(n) AS labels',
  'MATCH (n:Person)-[]-(m:Person) RETURN id(n) AS source, id(m) AS target')
YIELD
  graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels

// call algorithim 
CALL gds.betweenness.stream(
  "dcu_researchers"
)

// get a dcu researcher only graph 
match (n)
where n:Unkown
DETACH DELETE n

// Betweenes centraility dcu researchers only
CALL gds.graph.create.cypher(
  'dcu_researchers',
  'MATCH (n:Person) RETURN id(n) AS id, labels(n) AS labels',
  'MATCH (n:Person)-[r]-(m:Person) RETURN id(n) AS source, id(m) AS target, type(r) AS type, (r.total_paper_count + ( 0.5 * r.total_citations_achieved)) AS weight')
YIELD
  graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels

// call algorithim 
CALL gds.betweenness.stream(
  "dcu_researchers"
)

YIELD
  nodeId,
  score

// the neuler graph app is used to visualise and calculate lovian modulaity
// only interested in nodes that do NOT form islands so delete those
MATCH (a) WHERE NOT (a)-[:COLLABORATED]-()
detach delete a

// For weighted louvian, create weight as defined in report
MATCH (n)-[r]-(m) SET r.weight =  (r.total_paper_count + ( 0.5 * r.total_citations_achieved))