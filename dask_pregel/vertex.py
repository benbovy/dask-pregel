"""
Vertex base class (Pregel API).
"""


class Vertex:
    _out_vertices = None
    _out_edges_data = None
    _out_messages = None
    _active = True
    vertex_id = None
    value = None
    _n_superstep = 0
    _n_vertices = 0

    def __init__(self, vertex_id, value, active, n_vertices):
        self.vertex_id = vertex_id
        self._out_vertices = []
        self._out_edges_data = []
        self._out_messages = []
        self._active = active
        self.value = value
        self._n_superstep = 0
        self._n_vertices = n_vertices

    def _set_out_edges(self, out_vertices, out_edges_data):
        self._out_vertices = out_vertices
        self._out_edges_data = out_edges_data

    @property
    def n_vertices(self):
        return self._n_vertices

    @property
    def out_edges(self):
        return self._out_edges_data

    @property
    def n_superstep(self):
        return self._n_superstep

    def _increment_superstep(self):
        self._n_superstep += 1

    def _get_out_messages_and_clear(self):
        out_messages = self._out_messages.copy()
        self._out_messages.clear()
        return out_messages

    def _maybe_run_compute(self, in_messages):
        # maybe reactivate vertex
        if len(in_messages):
            self._active = True

        if self._active:
            self.compute(in_messages)

        # maybe clear outgoing messages
        if not self._active:
            self._out_messages.clear()

    def compute(self, in_messages):
        raise NotImplementedError()

    def send_message_to(self, target, value):
        """Send a message to a given target vertex."""
        self._out_messages.append((target, value))

    def send_message_to_all(self, value):
        """Send a message to the target vertex of each outgoing edge."""
        for target in self._out_vertices:
            self._out_messages.append((target, value))

    def vote_to_halt(self):
        self._active = False
