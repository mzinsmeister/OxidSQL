use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub(super) struct QueryNode {
    pub relation_id: usize,
    pub result_cardinality: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct QueryEdge {
    pub selectivity: f64,
    pub predicate: ()
}

/// The Query Graph is a helper data structure for all kinds of optimizers. 
/// This one internally uses Petgraph for now. Not sure the petgraph graph representation
/// we are currently using (normal petgraph::Graph) is actually the ideal one. MatrixGraph
/// might be better for our use case. But we can change that later since the QueryGraph doesn't
/// expose any petgraph types.
pub(super) struct QueryGraph {
    graph: petgraph::Graph<QueryNode, QueryEdge>,
    relation_node_map: HashMap<usize, petgraph::graph::NodeIndex>,
}

impl QueryGraph {
    pub(super) fn new() -> Self {
        Self {
            graph: petgraph::Graph::new(),
            relation_node_map: HashMap::new(),
        }
    }

    pub(super) fn add_node(&mut self, relation_id: usize, cardinality: usize) {
        let index = self.graph.add_node(QueryNode {
            relation_id,
            result_cardinality: cardinality,
        });
        self.relation_node_map.insert(relation_id, index);
    }

    pub(super) fn add_edge(
        &mut self,
        from_relation_id: usize,
        to_relation_id: usize,
        selectivity: f64,
        predicate: ()
    ) {
        let from = self.relation_node_map[&from_relation_id];
        let to = self.relation_node_map[&to_relation_id];
        self.graph.add_edge(from, to, QueryEdge { selectivity, predicate });
    }

    pub(super) fn get_node(&self, relation_id: usize) -> &QueryNode {
        &self.graph[self.relation_node_map[&relation_id]]
    }

    pub(super) fn node_iterator(&self) -> RelationIterator<'_, petgraph::graph::NodeIndices> {
        RelationIterator{ iter: self.graph.node_indices(), petgraph: &self.graph }
    }

    pub(super) fn neighbors(&self, relation_id: usize) -> RelationIterator<'_, petgraph::graph::Neighbors<QueryEdge>> {
        RelationIterator{ iter: self.graph.neighbors(self.relation_node_map[&relation_id]), petgraph: &self.graph }
    }

    pub(super) fn get_edge(&self, relation_id1: usize, relation_id2: usize) -> Option<QueryEdge> {
        self.graph.find_edge(self.relation_node_map[&relation_id1], self.relation_node_map[&relation_id2]).map(|e| self.graph[e])
    }
}

pub(super) struct RelationIterator<'a, I: Iterator<Item=petgraph::graph::NodeIndex>>{
    iter: I,
    petgraph: &'a petgraph::Graph<QueryNode, QueryEdge>,
}

impl<'a, I: Iterator<Item=petgraph::graph::NodeIndex>> Iterator for RelationIterator<'a, I> {
    type Item = &'a QueryNode;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|i| &self.petgraph[i])
    }
}
