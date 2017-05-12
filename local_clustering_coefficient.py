'''Local Clustering Coefficient algorithm using GraphFrames package
'''
from itertools import chain
import pyspark.sql.functions as func
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import Row


def my_chain(*args):
    '''Transform None to [] before chaining them
    '''
    n_args = []
    for arg in args:
        if arg is None:
            n_args.append([])
        else:
            n_args.append(arg)
    return chain(*tuple(n_args))


def concat(defined_type):
    '''UDF func to concat arrays
    '''
    def concat_(*args):
        '''Concat arrays and eliminates duplicates
        '''
        return list(set(my_chain(*args)))
    return func.udf(concat_, ArrayType(defined_type))


def clustering_coef(n_of_neighbors, n_of_edges_in_neighborhood):
    '''Local clustering coefficient formula
    '''
    if n_of_neighbors < 2:
        return 0
    return (n_of_edges_in_neighborhood * 2) / float(n_of_neighbors * (n_of_neighbors - 1))


class LocalClusteringCoefficient(object):
    '''Method for calculating the Local Clustering Coefficient of the graph.
    The graph should be a GraphFrames graph.
    '''
    def __init__(self, graph):
        self.graph = graph


    def all_vertices(self):
        '''Calculates the Local Clustering Coefficient for every vertice in the graph.
        ATENTION: the algorithm considers the graph as undirected.
        Return: a DataFrame with two columns:
        - "id": the ID of the vertex.
        - "clustering_coefficient": the clustering coefficient of that vertice.
        '''
        print 'Calculating Local Clustering Coefficient'

        vertices = self.graph.vertices
        edges = self.graph.edges.where('src != dst')

        print ' - Finding out neighbors'
        out_neighbors_ids = edges.select(edges.src.alias('id'), 'dst').distinct().groupBy(
            'id').agg(func.expr('collect_list(dst) AS out_neighbors'))

        out_neighbors_ids = vertices.join(
            out_neighbors_ids, out_neighbors_ids.id == vertices.id, 'left_outer'
        ).select(
            vertices.id,
            out_neighbors_ids.out_neighbors.alias('out_neighbors')
        )

        print ' - Finding in neighbors'
        in_neighbors_ids = edges.select(edges.dst.alias('id'), 'src').distinct().groupBy(
            'id').agg(func.expr('collect_list(src) AS in_neighbors'))

        in_neighbors_ids = vertices.join(
            in_neighbors_ids, in_neighbors_ids.id == vertices.id, 'left_outer'
        ).select(
            vertices.id,
            in_neighbors_ids.in_neighbors.alias('in_neighbors')
        )

        print ' - Joining them'
        concat_string_arrays = concat(StringType())
        neighbors_ids = out_neighbors_ids.join(
            in_neighbors_ids, out_neighbors_ids.id == in_neighbors_ids.id
        ).select(
            in_neighbors_ids.id,
            concat_string_arrays(
                'in_neighbors', 'out_neighbors').alias('neighbors')
        )

        print ' - Counting edges between neighbors'
        query = func.expr("array_contains(neighbors, src)") &\
            func.expr("array_contains(neighbors, dst)")

        edges_in_neighborhood = neighbors_ids.join(edges, query, "left_outer").select(
            neighbors_ids.id, edges.dst, edges.src
        ).groupBy(neighbors_ids.id).agg(
            func.countDistinct(edges.dst, edges.src).alias(
                'n_of_edges_in_neighborhood')
        )

        print ' - Counting neighbors'
        n_of_neighbors = neighbors_ids.select(
            'id', func.size('neighbors').alias('n_of_neighbors'))

        print ' - Calculating clustering coefficient'
        return edges_in_neighborhood.join(
            n_of_neighbors, edges_in_neighborhood.id == n_of_neighbors.id
        ).rdd.map(
            lambda row: Row(
                id=row['id'],
                clustering_coefficient=clustering_coef(
                    row['n_of_neighbors'], row['n_of_edges_in_neighborhood'])
            )
        ).toDF()

    def calculate_average(self):
        '''Calculates the average Local Clustering Coefficient of the graph.
        '''
        clustering_coefficients = self.all_vertices()
        return clustering_coefficients.agg(
            func.avg('clustering_coefficient').alias('avg')
        ).take(1)[0]['avg']
