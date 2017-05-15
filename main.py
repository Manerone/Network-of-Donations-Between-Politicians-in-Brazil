'''This script creates a social network using candidates as vertices and donations as edges
'''
import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from graphframes import *
from local_clustering_coefficient import LocalClusteringCoefficient
from assortativity import Assortativity

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
    ).toDF()

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


def clean_graph(graph):
    '''Clean the graph by:
    - Removing self reference edges.
    - Removing multiple edges from the same src to the same dst.
    - Removing duplication of ID's in the vertices.
    - Adding srcs and dsts not in vertices to vertices.

    '''
    vertices = graph.vertices.select('id').distinct()
    edges = graph.edges.where('src != dst').select('src', 'dst').distinct()
    ids = vertices.rdd.map(lambda r: r['id']).collect()

    srcs_not_in = edges.where((edges.src.isin(ids) == False) & (edges.dst.isin(ids)))
    srcs_not_in = srcs_not_in.select(srcs_not_in.src.alias('id')).distinct()
    dsts_not_in = edges.where((edges.dst.isin(ids) == False) & (edges.src.isin(ids)))
    dsts_not_in = dsts_not_in.select(dsts_not_in.dst.alias('id')).distinct()

    vertices = vertices.unionAll(srcs_not_in)
    vertices = vertices.unionAll(dsts_not_in)
    return GraphFrame(vertices, edges)


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


def print_result(result):
    '''Prints the result with the word 'RESULT: ' before
    '''
    print 'RESULT: ', result


def main():
    '''Main function of the script
    '''
    spark_context = SparkContext("local", "Donations Network")
    sql_context = SQLContext(spark_context)

    print '*********************************************************'
    print '******************** STARTING SCRIPT ********************'

    print 'Reading candidates file'
    candidates = read_candidates_file(spark_context, CANDIDATES_PATH)

    print 'Reading donations file'
    donations = read_donations_file(spark_context, DONATIONS_PATH)

    print 'Build graph'

    graph = clean_graph(GraphFrame(candidates, donations))

    print_result(Assortativity(graph).calculate())

    print_result(LocalClusteringCoefficient(graph).calculate_average())

    print_result(average_shortest_path(graph))

    print '******************** FINISHING SCRIPT *******************'
    print '*********************************************************'

if __name__ == '__main__':
    main()
