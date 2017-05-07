'''This script creates a social network using candidates as vertices and donations as edges
'''
import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
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


def out_neighbors(vertice, graph):
    print 'Finding out neighbors'
    edges = graph.edges
    v_id = str(vertice.id)
    query = 'src == ' + v_id + ' AND dst != ' + v_id
    
    return edges.where(query).select(edges.dst.alias('id')).distinct()


def in_neighbors(vertice, graph):
    print 'Finding in neighbors'
    edges = graph.edges
    v_id = str(vertice.id)
    query = 'dst == ' + v_id + ' AND src != ' + v_id

    return edges.where(query).select(edges.src.alias('id')).distinct()


def neighbors(vertice, graph):
    print vertice
    o_neighbors = out_neighbors(vertice, graph)
    i_neighbors = in_neighbors(vertice, graph)
    print 'Joining them'
    return o_neighbors.union(i_neighbors).distinct()


def vertice_clustering_coefficient(vertice, graph):
    vertice_neighbors = neighbors(vertice, graph)
    n_of_neighbors = vertice_neighbors.count()

    # vertice_neighbors_id = vertice_neighbors.rdd.map(lambda x: x['id']).collect()

    print vertice_neighbors.show()

    if n_of_neighbors < 2:
        return 0

    edges = graph.edges

    query = (edges.src.isin(vertice_neighbors.id))\
            & (edges.dst.isin(vertice_neighbors.id))\
            & (edges.src != edges.dst)

    vertices = edges.join(
        vertice_neighbors, query
    ).select('dst', 'src').distinct()

    vertices.show()

    return vertices.count() / float(n_of_neighbors * (n_of_neighbors - 1))


def clustering_coefficient(graph):
    vertices = graph.vertices.collect()
    ccs = [vertice_clustering_coefficient(v, graph) for v in vertices]

    return sum(ccs)/float(len(ccs))


def main():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    print 'Reading candidates file'
    candidates = read_candidates_file(spark_context, CANDIDATES_PATH)

    print 'Reading donations file'
    donations = read_donations_file(spark_context, DONATIONS_PATH)

    print 'Build graph'
    graph = GraphFrame(candidates, donations)

    # print clustering_coefficient(graph)


    vertices = graph.vertices
    edges = graph.edges.where('src != dst')

    j = vertices.join(edges, vertices.id == edges.src)
    number_of_neighbors = j.groupBy('id').agg(
        func.countDistinct('dst').alias('n_of_neighbors')
    )

    neighbors_ids = j.select('id', 'dst').distinct().groupBy('id').agg(func.expr('collect_list(dst) AS neighbors'))

    number_of_neighbors.join(neighbors_ids, neighbors_ids.id == number_of_neighbors.id)\
    .select(neighbors_ids.id, neighbors_ids.neighbors, number_of_neighbors.n_of_neighbors).show()

    # query = (edges.src.isin(n.neighbors)) &\
    #         (edges.dst.isin(n.neighbors))

    # n.join(edges, query).select(n.id, edges.dst, edges.src)\
    #          .groupBy(n.id).agg(
    #              func.countDistinct(edges.dst, edges.src)
    #          ).show()


if __name__ == '__main__':
    main()
