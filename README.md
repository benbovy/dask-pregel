# dask-pregel

A Python implementation of the Pregel distributed programming model
using [Dask](https://github.com/dask/dask).

**This is experimental.**

This implementation might be helpful for prototyping or testing
*Pregel-compatible* algorithms, but not for running it at scale. There
are a lot of caveats.

## Example: PageRank

```python
from dask_pregel import Vertex


class PageRankVertex(Vertex):

    def compute(self, in_messages):

        if self.n_superstep < 30:
            rank_sum = sum((rank for _, rank in in_messages))
            self.value = 0.15 / self.n_vertices + 0.85 * rank_sum

            self.send_message_to_all(self.value / len(self.out_edges))
        else:
            self.vote_to_halt()
```

```python-console
>>> from dask.distributed import Client
>>> from dask_pregel import Pregel
>>> from dask_pregel.tests.test_pagerank import generate_graph
>>> client = Client()
>>> graph = generate_graph(10)   # 10-vertices graph with random connections
>>> pr = Pregel(PageRankVertex, graph, client)
>>> pr.run()
>>> [v.value for v in pr.vertices]
array([ 0.13107403,  0.14083991,  0.07424558,  0.12016776,  0.07447663,
        0.10808172,  0.13848769,  0.03079937,  0.1071777 ,  0.07435384])
```

## Caveats

- This implementation is not feature complete. The following is not
  supported:
  - Combiner and Aggregator classes
  - Fault tolerance
  - Modifying graph topology

- This relies on an experimental feature introduced in
  Dask/Distributed 1.23.0 (stateful actors).

- It is inefficient. The graph is partitioned among Dask workers
  automatically, in a non-optimized way. It would be better to create
  a class for Pregel workers that each handles a bunch of vertices
  rather than let Dask manage each vertex separately.

- I don't have much experience with Pregel and advanced Dask usage,
  there may be things that I don't know or misunderstand.


## See also

- Another Python implementation of Pregel: https://github.com/mnielsen/Pregel
- [Some slides](http://people.apache.org/~edwardyoon/documents/pregel.pdf)
  explaining Pregel.
