'''This script creates a social network using candidates as vertices and donations as edges
'''
import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from graphframes import *
from local_clustering_coefficient import LocalClusteringCoefficient
from pyspark.sql.types import StringType, ArrayType
from itertools import chain

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
CANDIDATES_PATH = CURRENT_PATH + '/databases/consulta_cand/consulta_cand_2016_PR.txt'
DONATIONS_PATH = CURRENT_PATH + \
    '/databases/receitas_despesas/receitas_candidatos_prestacao_contas_final_2016_PR.txt'


def read_candidates_file(context, file_path):
    '''Read file with candidates info.
    Parameters:
    +context+ - spark context.
    +file_path+ - path to the candidates file.
    '''
    candidates_file = context.textFile(file_path)
    candidates_splitted_in_parts = candidates_file.map(
        lambda line: line.encode('unicode-escape').replace('"', '').split(';')
    )
    return candidates_splitted_in_parts.map(
        lambda candidate: Row(
            id=candidate[13], # CPF
            cod_cidade=int(candidate[6]), cargo=candidate[9],
            nome=candidate[10], num_cand=int(candidate[12]),
            cidade=candidate[7], cod_status=int(candidate[43]),
            status=candidate[44], partido=int(candidate[17]),
            nasc=candidate[26], genero=candidate[30]
        )
    ).toDF().where("cidade = 'CURITIBA'")

def read_donations_file(context, file_path):
    '''Read file with donations to candidates
    Parameters:
    +context+ - spark context.
    +file_path+ - path to the donations file.
    '''
    donations_file = context.textFile(file_path)
    donations_splitted_in_parts = donations_file.map(
        lambda line: line.encode('unicode-escape').replace('"', '').replace(',', '.').split(';')
    )
    return donations_splitted_in_parts.map(
        lambda donation: Row(
            dst=donation[12], # CPF
            src=donation[16], # CPF
            nome_doador=donation[17],
            cod_cidade=int(donation[6]),
            valor=float(donation[25]),
            descricao=donation[29],
            data=donation[2],
            num_recibo=donation[14]
        )
    ).toDF()


def average_shortest_path(graph):
    '''Calculates the average shortest path of the graph.
    OBS: Only uses 1000 vertices.
    '''
    print 'Calculating average shortest path'
    s_size = 1000 / float(graph.vertices.count())
    vertices_sample = graph.vertices.sample(False, s_size).rdd.map(
        lambda r: r['id']).collect()
    results = graph.shortestPaths(landmarks=vertices_sample)
    return results.select('id', func.explode('distances').alias('key', 'value'))\
                  .groupBy().agg(func.avg('value').alias('average')).collect()[0]['average']


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


def assortativity(graph):
    '''Calculates the graph assortativity.
    '''
    print 'Calculating graph assortativity'

    print ' - Calculating every node degree'
    edges = graph.edges

    out_neighbors_ids = edges.select(edges.src.alias('id'), 'dst').distinct().groupBy(
        'id').agg(func.expr('collect_list(dst) AS out_neighbors'))

    in_neighbors_ids = edges.select(edges.dst.alias('id'), 'src').distinct().groupBy(
        'id').agg(func.expr('collect_list(src) AS in_neighbors'))

    concat_string_arrays = concat(StringType())
    nodes_degree = out_neighbors_ids.join(
        in_neighbors_ids, out_neighbors_ids.id == in_neighbors_ids.id
    ).select(
        in_neighbors_ids.id,
        concat_string_arrays(
            'in_neighbors', 'out_neighbors').alias('neighbors')
    )

    nodes_degree = nodes_degree.select(
        'id',
        func.size('neighbors').alias('degree')
    )


    nodes_degree = graph.vertices.join(
        nodes_degree, nodes_degree.id == graph.vertices.id, 'left_outer'
    ).select(
        graph.vertices.id,
        nodes_degree.degree
    ).fillna({'degree': 0})

    print ' - Calculating edges degrees'

    edges = edges.select('src', 'dst').distinct()

    edges_degress = edges.join(
        nodes_degree, edges.src == nodes_degree.id
    ).select(
        'src', 'dst', nodes_degree.degree.alias('src_degree')
    ).join(
        nodes_degree, edges.dst == nodes_degree.id
    ).select(
        'src', 'dst', 'src_degree', nodes_degree.degree.alias('dst_degree')
    ).rdd

    print ' - Calculating the assortativity'

    m = 1 / float(edges_degress.count())

    sum1 = edges_degress.map(
        lambda row: row['src_degree'] * row['dst_degree']
    ).sum()

    sum1 = m * sum1

    sum2 = edges_degress.map(
        lambda row: (row['src_degree'] + row['dst_degree']) / 2.0
    ).sum()

    sum2 = m * sum2
    sum2 = sum2 * sum2

    sum3 = edges_degress.map(
        lambda row: (row['src_degree'] ** 2) + (row['dst_degree'] ** 2) / 2.0
    ).sum()

    sum3 = m * sum3

    return (sum1 - sum2)/(sum3 - sum2)


def transform_to_undirected(graph):
    original_edges = graph.edges.where('src != dst')
    concat_string_arrays = concat(StringType())
    edges = original_edges.groupBy('num_recibo').agg(
        func.expr('collect_list(dst) AS dsts'),
        func.expr('collect_list(src) AS srcs')
    ).select(
        'num_recibo',
        concat_string_arrays('dsts', 'srcs').alias('vertices')
    )

    edges = edges.groupBy('vertices').agg(
        func.first(edges.num_recibo).alias('num_recibo')
    ).select('num_recibo')

    edges = edges.join(
        original_edges, edges.num_recibo == original_edges.num_recibo
    ).drop(edges.num_recibo)

    return GraphFrame(graph.vertices, edges)



def main():
    '''Main function of the script
    '''
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    print 'Reading candidates file'
    candidates = read_candidates_file(spark_context, CANDIDATES_PATH)

    print 'Reading donations file'
    donations = read_donations_file(spark_context, DONATIONS_PATH)

    print 'Build graph'
    graph = GraphFrame(candidates, donations)
    graph = transform_to_undirected(graph)

    print assortativity(graph)

    # print LocalClusteringCoefficient(graph).calculate_average()

    # print average_shortest_path(graph)


if __name__ == '__main__':
    main()
