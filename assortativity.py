'''Assortativity algorithm using GraphFrames package.
Article: Newman, M. E. J. (2003). Mixing patterns in networks.\
Phys. Rev. E, 67, 026126. doi: 10.1103/PhysRevE.67.026126
'''
from itertools import chain
from math import sqrt
from pyspark.sql.types import StringType, ArrayType
import pyspark.sql.functions as func
from graphframes import *


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
        return sorted(list(set(my_chain(*args))))
    return func.udf(concat_, ArrayType(defined_type))


# def transform_to_undirected(graph):
#     original_edges = graph.edges.where('src != dst')
#     concat_string_arrays = concat(StringType())
#     edges = original_edges.groupBy('num_recibo').agg(
#         func.expr('collect_list(dst) AS dsts'),
#         func.expr('collect_list(src) AS srcs')
#     ).select(
#         'num_recibo',
#         concat_string_arrays('dsts', 'srcs').alias('vertices')
#     )

#     edges = edges.groupBy('vertices').agg(
#         func.first(edges.num_recibo).alias('num_recibo')
#     ).select('num_recibo')

#     edges = edges.join(
#         original_edges, edges.num_recibo == original_edges.num_recibo
#     ).drop(edges.num_recibo)

#     return GraphFrame(graph.vertices, edges)


class Assortativity(object):
    '''Method for calculating the assortativity of the graph.
    The graph should be a GraphFrames graph.
    '''
    def __init__(self, graph):
        self.graph = graph

    def calculate(self):
        '''Calculates the Assortativity of the graph.
        The method used is based on Newman, M. E. J. (2003). Mixing patterns in networks.
        equation 26.
        '''
        print 'Calculating graph assortativity'

        print ' - Calculating every node degree'
        graph = self.graph
        edges = graph.edges.where('src != dst')

        out_neighbors_ids = edges.select(edges.src.alias('id'), 'dst').distinct().groupBy(
            'id').agg(func.expr('collect_list(dst) AS out_neighbors'))

        in_neighbors_ids = edges.select(edges.dst.alias('id'), 'src').distinct().groupBy(
            'id').agg(func.expr('collect_list(src) AS in_neighbors'))

        nodes_degree = out_neighbors_ids.join(
            in_neighbors_ids, out_neighbors_ids.id == in_neighbors_ids.id
        ).drop(out_neighbors_ids.id)

        nodes_degree = nodes_degree.select(
            'id',
            func.size('out_neighbors').alias('out_degree'),
            func.size('in_neighbors').alias('in_degree')
        )

        nodes_degree = graph.vertices.join(
            nodes_degree, nodes_degree.id == graph.vertices.id, 'left_outer'
        ).select(
            graph.vertices.id,
            nodes_degree.out_degree,
            nodes_degree.in_degree
        ).fillna({'out_degree': 0, 'in_degree': 0})

        print ' - Calculating edges degrees'

        edges = edges.select('src', 'dst').distinct()

        edges_degress = edges.join(
            nodes_degree, edges.src == nodes_degree.id
        ).select(
            'src', 'dst',
            nodes_degree.in_degree.alias('src_in_degree'),
            nodes_degree.out_degree.alias('src_out_degree')
        ).join(
            nodes_degree, edges.dst == nodes_degree.id
        ).select(
            'src', 'dst', 'src_in_degree', 'src_out_degree',
            nodes_degree.in_degree.alias('dst_in_degree'),
            nodes_degree.out_degree.alias('dst_out_degree')
        ).rdd

        print ' - Calculating the assortativity'

        n_of_edges = 1 / float(edges_degress.count())

        sum1 = edges_degress.map(
            lambda row: (row['src_in_degree'] + row['dst_in_degree'] - 2) *\
                        (row['src_out_degree'] + row['dst_out_degree'] - 2)
        ).sum()

        sum2 = edges_degress.map(
            lambda row: row['src_in_degree'] + row['dst_in_degree'] - 2
        ).sum()

        sum3 = edges_degress.map(
            lambda row: row['src_out_degree'] + row['dst_out_degree'] - 2
        ).sum()

        numerator = sum1 - (n_of_edges * sum2 * sum3)

        sum4 = edges_degress.map(
            lambda row: (row['src_in_degree'] + row['dst_in_degree'] - 2) ** 2
        ).sum()

        denominator_part1 = sum4 - (n_of_edges * (sum2 ** 2))

        sum5 = edges_degress.map(
            lambda row: (row['src_out_degree'] + row['dst_out_degree'] - 2) ** 2
        ).sum()

        denominator_part2 = sum5 - (n_of_edges * (sum3 ** 2))

        denominator = sqrt(denominator_part1 * denominator_part2)

        return numerator / denominator
