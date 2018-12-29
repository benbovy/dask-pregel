"""
Find maximum value example in Pregel's paper.

"""
from dask.distributed import Client

from .. import Pregel, Vertex


class FindMaxVertex(Vertex):

    def compute(self, in_messages):

        max_value = max([self.value] + [val for _, val in in_messages])

        if self.n_superstep == 0:
            self.send_message_to_all(self.value)
        elif max_value > self.value:
            self.send_message_to_all(max_value)
            self.value = max_value
        else:
            self.vote_to_halt()


graph = [
    {
        'id': 1,
        'value': 3,
        'out_edges': [
             {'target_id': 2}
        ]
    },
    {
        'id': 2,
        'value': 6,
        'out_edges': [
             {'target_id': 1},
             {'target_id': 4}
        ]
    },
    {
        'id': 3,
        'value': 2,
        'out_edges': [
             {'target_id': 2},
             {'target_id': 4}
        ]
    },
    {
        'id': 4,
        'value': 1,
        'out_edges': [
             {'target_id': 3}
        ]
    },
]


def test_findmax():
    client = Client(processes=False)
    pr = Pregel(FindMaxVertex, graph, client)

    pr.run()

    actual = [v.value for v in pr.vertices]
    expected = [6, 6, 6, 6]

    assert actual == expected

    client.close()
