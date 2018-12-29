import random

import numpy as np
from dask.distributed import Client

from .. import Pregel, Vertex


class PageRankVertex(Vertex):

    def compute(self, in_messages):

        if self.n_superstep < 50:
            rank_sum = sum((rank for _, rank in in_messages))
            self.value = 0.15 / self.n_vertices + 0.85 * rank_sum

            self.send_message_to_all(self.value / len(self.out_edges))
        else:
            self.vote_to_halt()


def generate_graph(n_vertices):
    vertex_ids = list(range(n_vertices))
    out_edges = [[{'target_id': t_id} for t_id in random.sample(vertex_ids, 4)]
                 for _ in vertex_ids]

    graph = [{'id': vid, 'value': 1. / n_vertices, 'out_edges': edges}
             for vid, edges in zip(vertex_ids, out_edges)]

    return graph


def pagerank_numpy(graph):
    """PageRank Matrix form."""
    n_vertices = len(graph)

    I = np.eye(n_vertices)
    M = np.zeros((n_vertices, n_vertices))

    for vertex in graph:
        n_out_vertices = len(vertex['out_edges'])

        for out_vertex in vertex['out_edges']:
            M[out_vertex['target_id'], vertex['id']] = 1. / n_out_vertices

    P = (1. / n_vertices) * np.ones((n_vertices, 1))

    return (0.15 * np.linalg.inv(I - 0.85 * M) @ P).ravel()


def pagerank_pregel(graph):
    client = Client(processes=False)

    pr = Pregel(PageRankVertex, graph, client)
    pr.run()

    result =  np.array([v.value for v in pr.vertices])

    client.close()

    return result


def test_pagerank():
    graph = generate_graph(10)

    expected = pagerank_numpy(graph)
    actual = pagerank_pregel(graph)

    assert np.allclose(actual, expected, atol=1e-4)
