'''This script creates a social network using candidates as vertices and donations as edges
'''
import os
from itertools import chain
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql.types import StringType, ArrayType
from graphframes import *
import graphviz as gv

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


def concat(type):
    def concat_(*args):
        return list(set(chain(*args)))
    return func.udf(concat_, ArrayType(type))


def clustering_coef(n_of_neighbors, n_of_edges_in_neighborhood):
    if n_of_neighbors < 2:
        return 0
    return n_of_edges_in_neighborhood/float(n_of_neighbors * (n_of_neighbors - 1))


def clustering_coefficient(graph):
    vertices = graph.vertices
    edges = graph.edges.where('src != dst')

    out_neighbors = vertices.join(edges, vertices.id == edges.src)
    in_neighbors = vertices.join(edges, vertices.id == edges.dst)

    print 'Finding out neighbors'
    out_neighbors_ids = out_neighbors.select('id', 'dst').distinct().groupBy(
        'id').agg(func.expr('collect_list(dst) AS out_neighbors'))

    print 'Finding in neighbors'
    in_neighbors_ids = in_neighbors.select('id', 'src').distinct().groupBy(
        'id').agg(func.expr('collect_list(src) AS in_neighbors'))

    print 'Joining them'
    concat_string_arrays = concat(StringType())
    neighbors_ids = out_neighbors_ids.join(
        in_neighbors_ids, out_neighbors_ids.id == in_neighbors_ids.id
    ).select(
        in_neighbors_ids.id,
        concat_string_arrays('in_neighbors', 'out_neighbors').alias('neighbors')
    )

    print 'Counting edges between neighbors'
    query = func.expr("array_contains(neighbors, src)") &\
        func.expr("array_contains(neighbors, dst)")

    edges_in_neighborhood = neighbors_ids.join(edges, query, "left_outer").select(
        neighbors_ids.id, edges.dst, edges.src
        ).groupBy(neighbors_ids.id).agg(
            func.countDistinct(edges.dst, edges.src).alias('n_of_edges_in_neighborhood')
        )

    print 'Counting neighbors'
    n_of_neighbors = neighbors_ids.select('id', func.size('neighbors').alias('n_of_neighbors'))

    print 'Calculating clustering coefficient'
    return edges_in_neighborhood.join(n_of_neighbors, edges_in_neighborhood.id == n_of_neighbors.id)\
    .rdd.map(
        lambda row: clustering_coef(row['n_of_neighbors'], row['n_of_edges_in_neighborhood'])
    ).mean()


def average_shortest_path(graph):
    print 'Calculating average shortest path'
    s_size = 1000 / float(graph.vertices.count())
    lm = graph.vertices.sample(False, s_size).rdd.map(
        lambda r: r['id']).collect()
    results = graph.shortestPaths(landmarks=lm)
    return results.select('id', func.explode('distances').alias('key', 'value'))\
                  .groupBy().agg(func.avg('value').alias('average')).collect()[0]['average']


def main():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    print 'Reading candidates file'
    candidates = read_candidates_file(spark_context, CANDIDATES_PATH)

    print 'Reading donations file'
    donations = read_donations_file(spark_context, DONATIONS_PATH)

    print 'Build graph'
    graph = GraphFrame(candidates, donations)

    cc = clustering_coefficient(graph)

    print cc

    sp = average_shortest_path(graph)

    print sp


if __name__ == '__main__':
    main()
