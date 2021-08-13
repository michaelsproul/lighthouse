use bron_kerbosch::{
    explore as run_bron_kerbosch,
    graph::{NewableUndirectedGraph, UndirectedGraph, Vertex, VertexMap, VertexSetLike},
    reporter::SimpleReporter,
    slimgraph::SlimUndirectedGraph,
    NUM_FUNCS,
};
use std::collections::HashSet;
use types::{Attestation, EthSpec};

const FUNC_ID: usize = NUM_FUNCS;

enum CliqueError {
    EmptyClique,
    IndexOutOfBounds { i: usize, len: usize },
}

pub struct Graph<E: EthSpec> {
    graph: SlimUndirectedGraph<HashSet<Vertex>>,
    attestations: Vec<Attestation<E>>,
}

impl<E: EthSpec> Graph<E> {
    pub fn new() -> Self {
        let adjacencies = VertexMap::sneak_in(vec![]);
        let graph = SlimUndirectedGraph::new(adjacencies);
        let attestations = vec![];
        Graph {
            graph,
            attestations,
        }
    }

    pub fn insert(&mut self, attestation: Attestation<E>) {
        let new_vertex = Vertex::new(self.graph.order());
        let mut adjacent = HashSet::new();

        for ((existing_vertex, existing_adj), existing_att) in self
            .graph
            .adjacencies
            .iter_mut()
            .zip(self.attestations.iter())
        {
            // If new attestation can be aggregated with this attestation, add an edge to the graph.
            if attestation.signers_disjoint_from(&existing_att) {
                existing_adj.insert(new_vertex);
                adjacent.insert(existing_vertex);
            }
        }

        // Add new adjacencies to graph, and new attestation to storage.
        self.graph.adjacencies.push(adjacent);
        self.attestations.push(attestation);
    }

    pub fn max_clique_attestations(&self) -> Result<Vec<Attestation<E>>, CliqueError> {
        // Find maximum cliques on the aggregation graph using the Bron-Kerbosch algorithm.
        let mut reporter = SimpleReporter::default();
        run_bron_kerbosch(FUNC_ID, &self.graph, &mut reporter);
        let cliques = reporter.cliques;

        // Eliminate cliques that are redundant: "dominated" by another clique by virtue of
        // being a strict subset of aggregation bits.
        // TODO

        // Aggregate the attestations in each clique.
        cliques
            .into_iter()
            .map(|mut clique| {
                if let Some(last_vertex) = clique.pop() {
                    let init = self.get_attestation(last_vertex)?.clone();
                    clique.into_iter().try_fold(init, |mut agg, vertex| {
                        self.get_attestation(vertex).map(|att| {
                            agg.aggregate(att);
                            agg
                        })
                    })
                } else {
                    Err(CliqueError::EmptyClique)
                }
            })
            .collect()
    }

    fn get_attestation(&self, vertex: Vertex) -> Result<&Attestation<E>, CliqueError> {
        let i = vertex.as_usize();
        let len = self.attestations.len();
        self.attestations
            .get(i)
            .ok_or(CliqueError::IndexOutOfBounds { i, len })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn overlapping_aggregates() {}
}
