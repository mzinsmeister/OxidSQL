use std::collections::HashMap;

use crate::{execution::plan::PhysicalQueryPlanOperator, planner::{BoundTable, BoundAttributeRef, BoundTableRef}};

#[derive(Clone)]
pub struct QueryNode {
    pub input: BoundTable,
    pub result_cardinality: u64,
    pub source_plan: PhysicalQueryPlanOperator
}

#[derive(Debug, Clone)]
pub struct QueryEdge {
    pub selectivity: f64,
    pub predicate: (BoundAttributeRef, BoundAttributeRef)
}

/// The Query Graph is a helper data structure for all kinds of optimizers. 
/// This one internally uses Petgraph for now. Not sure the petgraph graph representation
/// we are currently using (normal petgraph::Graph) is actually the ideal one. MatrixGraph
/// might be better for our use case. But we can change that later since the QueryGraph doesn't
/// expose any petgraph types.
pub struct QueryGraph {
    graph: petgraph::Graph<QueryNode, QueryEdge>,
    input_node_map: HashMap<BoundTableRef, petgraph::graph::NodeIndex>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            graph: petgraph::Graph::new(),
            input_node_map: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, input: BoundTable, cardinality: u64, source_plan: PhysicalQueryPlanOperator) {
        let table_ref = input.to_ref();
        let index = self.graph.add_node(QueryNode {
            input,
            result_cardinality: cardinality,
            source_plan
        });
        self.input_node_map.insert(table_ref, index);
    }

    pub fn add_edge(
        &mut self,
        from_table_ref: &BoundTableRef,
        to_table_ref: &BoundTableRef,
        selectivity: f64,
        predicate: (BoundAttributeRef, BoundAttributeRef)
    ) {
        let from = self.input_node_map[from_table_ref];
        let to = self.input_node_map[to_table_ref];
        self.graph.add_edge(from, to, QueryEdge { selectivity, predicate });
    }

    pub fn get_node(&self, relation_id: &BoundTableRef) -> &QueryNode {
        &self.graph[self.input_node_map[relation_id]]
    }

    pub fn node_iterator(&self) -> RelationIterator<'_, petgraph::graph::NodeIndices> {
        RelationIterator{ iter: self.graph.node_indices(), petgraph: &self.graph }
    }

    pub fn neighbors(&self, relation_id: &BoundTableRef) -> RelationIterator<'_, petgraph::graph::Neighbors<QueryEdge>> {
        RelationIterator{ iter: self.graph.neighbors(self.input_node_map[relation_id]), petgraph: &self.graph }
    }

    pub fn get_edge(&self, relation_id1: &BoundTableRef, relation_id2: &BoundTableRef) -> Option<QueryEdge> {
        self.graph.find_edge(self.input_node_map[relation_id1], self.input_node_map[relation_id2]).map(|e| self.graph[e].clone())
    }
}

pub struct RelationIterator<'a, I: Iterator<Item=petgraph::graph::NodeIndex>>{
    iter: I,
    petgraph: &'a petgraph::Graph<QueryNode, QueryEdge>,
}

impl<'a, I: Iterator<Item=petgraph::graph::NodeIndex>> Iterator for RelationIterator<'a, I> {
    type Item = &'a QueryNode;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| &self.petgraph[i])
    }
}
