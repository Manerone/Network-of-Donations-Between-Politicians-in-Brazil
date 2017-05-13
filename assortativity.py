'''Assortativity algorithm using GraphFrames package.
Article: Newman, M. E. J. (2003). Mixing patterns in networks.\
Phys. Rev. E, 67, 026126. doi: 10.1103/PhysRevE.67.026126
'''
import pyspark.sql.functions as func
from graphframes import *


def e_d(value):
    '''Excess degree: Return the given value - 1 or 0 if value equals 0.
    '''
    if float(value) <= 0:
        return 0
    return float(value) - 1


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
        vertices = graph.vertices

        out_neighbors_ids = edges.select(edges.src.alias('id'), 'dst').distinct().groupBy(
            'id').agg(func.expr('collect_list(dst) AS out_neighbors'))

        out_neighbors_ids = vertices.join(
            out_neighbors_ids, out_neighbors_ids.id == vertices.id, 'left_outer'
        ).select(
            vertices.id,
            out_neighbors_ids.out_neighbors.alias('out_neighbors')
        )

        in_neighbors_ids = edges.select(edges.dst.alias('id'), 'src').distinct().groupBy(
            'id').agg(func.expr('collect_list(src) AS in_neighbors'))

        in_neighbors_ids = vertices.join(
            in_neighbors_ids, in_neighbors_ids.id == vertices.id, 'left_outer'
        ).select(
            vertices.id,
            in_neighbors_ids.in_neighbors.alias('in_neighbors')
        )

        nodes_degree = out_neighbors_ids.join(
            in_neighbors_ids, out_neighbors_ids.id == in_neighbors_ids.id
        ).drop(out_neighbors_ids.id)

        fix_degree = func.udf(lambda value: value if value >= 0 else 0)
        nodes_degree = nodes_degree.select(
            'id',
            func.size('out_neighbors').alias('pre_out_degree'),
            func.size('in_neighbors').alias('pre_in_degree')
        )
        nodes_degree = nodes_degree.withColumn(
            'out_degree', fix_degree(nodes_degree.pre_out_degree)
        ).drop('pre_out_degree')

        nodes_degree = nodes_degree.withColumn(
            'in_degree', fix_degree(nodes_degree.pre_in_degree)
        ).drop('pre_in_degree')


        nodes_degree = vertices.join(
            nodes_degree, nodes_degree.id == vertices.id, 'left_outer'
        ).select(
            vertices.id,
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
            lambda row: (e_d(row['src_in_degree']) + e_d(row['dst_in_degree'])) *\
                        (e_d(row['src_out_degree']) + e_d(row['dst_out_degree']))
        ).sum()

        sum2 = edges_degress.map(
            lambda row: e_d(row['src_in_degree']) + e_d(row['dst_in_degree'])
        ).sum()

        sum3 = edges_degress.map(
            lambda row: e_d(row['src_out_degree']) + e_d(row['dst_out_degree'])
        ).sum()

        numerator = sum1 - (n_of_edges * sum2 * sum3)

        sum4 = edges_degress.map(
            lambda row: (e_d(row['src_in_degree']) + e_d(row['dst_in_degree'])) ** 2
        ).sum()

        denominator_part1 = sum4 - (n_of_edges * (sum2 ** 2))

        sum5 = edges_degress.map(
            lambda row: (e_d(row['src_out_degree']) + e_d(row['dst_out_degree'])) ** 2
        ).sum()

        denominator_part2 = sum5 - (n_of_edges * (sum3 ** 2))

        denominator = (denominator_part1 * denominator_part2) ** (1/2.0)

        return numerator / denominator
