from collections import defaultdict


N_SUPERSTEP_LIMIT = 50000


class Pregel:
    """Pregel master."""

    def __init__(self, vertex_cls, graph, dask_client, callbacks=None):
        self.vertex_cls = vertex_cls
        self.dask_client = dask_client
        self.callbacks = callbacks or []

        # not very nice: graph vertices auto-partitioned
        # (round-robin?) between dask workers
        n_vertices = len(graph)

        futures = [
            self.dask_client.submit(self.vertex_cls,
                                    v['id'], v['value'], True, n_vertices,
                                    actor=True)
            for v in graph
        ]
        self.vertices = self.dask_client.gather(futures)

        # set outgoing edges
        vertices_dict = {v['id']: v_obj
                         for v, v_obj in zip(graph, self.vertices)}

        futures = []
        for v, v_obj in zip(graph, self.vertices):
            out_edges = v.get('out_edges', [])
            out_edges_data = [e.get('value', None) for e in out_edges]
            out_targets = [vertices_dict[e['target_id']] for e in out_edges]

            f = v_obj._set_out_edges(out_targets, out_edges_data)
            futures.append(f)

        self.dask_client.gather(futures)

    def _check_active(self):
        return any((v._active for v in self.vertices))

    def _redistribute_messages(self, out_messages):
        in_messages_dict = defaultdict(list)

        for v_source, msg_list in zip(self.vertices, out_messages):
            for v_target, value in msg_list:
                in_messages_dict[v_target.key].append((v_source, value))

        return in_messages_dict

    def superstep(self):
        # get outgoing messages
        futures = [v_obj._get_out_messages_and_clear() for v_obj in self.vertices]
        out_messages = [f.result() for f in futures]

        # get incoming messages
        in_messages_dict = self._redistribute_messages(out_messages)

        # run compute method for active vertices
        futures = [v_obj._maybe_run_compute(in_messages_dict.get(v_obj.key, []))
                   for v_obj in self.vertices]
        self.dask_client.gather(futures)   # distributed.wait(futures)

        # increment superstep
        futures = [v_obj._increment_superstep() for v_obj in self.vertices]
        self.dask_client.gather(futures)   # distributed.wait(futures)

        for func in self.callbacks:
            func(self.vertices, out_messages)

    def run(self):
        n_superstep = 0

        while self._check_active():
            if n_superstep > N_SUPERSTEP_LIMIT:
                raise RuntimeError("Superstep limit reached.")

            self.superstep()
            n_superstep += 1
