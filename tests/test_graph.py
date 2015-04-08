class TestGraphEdge(object):
    def test_collection_name(self):
        from arangodb import db, graph

        class MyEdge(db.Edge):
            pass

        class MyGraphEdge(graph.GraphEdge, MyEdge):
            pass

        assert MyGraphEdge.__collection_name__ == "MyEdge"


def test_graph():
    from arangodb import db, graph

    class D1(db.Document):
        pass

    class D2(db.Document):
        pass

    class D3(db.Document):
        pass

    class e2(db.Edge):
        pass

    class G1(graph.Graph):

        """
        api url: /_db/default/_api/gharial/G1/
        """
        class d3(graph.GraphVertex, D3):
            pass

        class edge(graph.GraphEdge, e2):
            pass

        @edge.from_vertex
        class v1(graph.GraphVertex, D1):
            pass

        @edge.to_vertex
        class v2(graph.GraphVertex, D2):
            pass

    class G11(G1):
        pass

    class G111(G11):
        pass

    G1.create_graph()

    assert G1.__graph_name__ == 'G1'
    assert G11.__graph_name__ == 'G1'
    assert G111.__graph_name__ == 'G1'

    assert G1.edge.__definition__ == {
        "collection": "e2",
        "from": ["D1"],
        "to": ["D2"]
    }

    assert G1.__definition__ == {
        "name": "G1",
        "edgeDefinitions": [G1.edge.__definition__],
        "orphanCollections": ["D3"]
    }
