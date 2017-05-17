'''This script creates a social network using candidates as vertices and donations as edges
'''
import os
from itertools import chain
from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as func
from pyspark.sql.types import StringType, ArrayType
from graphframes import *
from local_clustering_coefficient import LocalClusteringCoefficient
from assortativity import Assortativity
from graphviz import Digraph

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
    ).toDF().where("cidade= 'CURITIBA'")

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
    - Adding srcs and dsts not in vertices into vertices.
    - Removing edges where both dst and src are not in the vertices.

    '''
    print 'Graph cleaning'
    print ' - Removing ID duplication'
    vertices = graph.vertices.groupBy('id').agg(
        func.first('nome').alias('nome')
    )
    print ' - Removing multiple connections'
    edges = graph.edges.where('src != dst').groupBy('src', 'dst').agg(
        func.sum('valor').alias('valor')
    )

    print ' - Adding srcs and dsts that are not in vertices into vertices'
    ids = vertices.rdd.map(lambda r: r['id']).collect()
    srcs_not_in = edges.where((edges.src.isin(ids) == False) & (edges.dst.isin(ids)))
    srcs_not_in = srcs_not_in.select(srcs_not_in.src.alias('id')).distinct()
    dsts_not_in = edges.where((edges.dst.isin(ids) == False) & (edges.src.isin(ids)))
    dsts_not_in = dsts_not_in.select(dsts_not_in.dst.alias('id')).distinct()

    vertices = vertices.unionAll(srcs_not_in.withColumn('nome', func.lit(None).cast(StringType())))
    vertices = vertices.unionAll(dsts_not_in.withColumn('nome', func.lit(None).cast(StringType())))

    print ' - Removing edges where both dst and src are not in the vertices'
    ids = vertices.rdd.map(lambda r: r['id']).collect()
    edges = edges.where((edges.src.isin(ids)) & (edges.dst.isin(ids)))
    return GraphFrame(vertices, edges)


def write_graph_to_file(graph, file_name):
    '''Writes the graph to a .gv file
    '''
    dot = Digraph(name=file_name)

    srcs = graph.edges.rdd.map(lambda r: r['src']).collect()
    dsts = graph.edges.rdd.map(lambda r: r['dst']).collect()
    values = graph.edges.rdd.map(lambda r: str(r['valor'])).collect()
    vertices = graph.vertices
    vertices = vertices.where(
        vertices.id.isin(srcs) | vertices.id.isin(dsts)
    ).rdd.map(lambda r: {'id': r['id'], 'nome': r['nome']}).collect()

    for vertice in vertices:
        dot.node(vertice['id'], label=vertice['nome'])

    for src, dst, value in zip(srcs, dsts, values):
        dot.edge(src, dst, label=value)

    dot.save(directory='graphs')


def average_shortest_path(graph):
    '''Calculates the average shortest path of the graph.
    OBS: Only uses 100 vertices.
    '''
    print 'Calculating average shortest path'
    s_size = 100 / float(graph.vertices.count())
    vertices_sample = graph.vertices.sample(False, s_size).rdd.map(
        lambda r: r['id']).collect()
    results = graph.shortestPaths(landmarks=vertices_sample)
    return results.select('id', func.explode('distances').alias('key', 'value'))\
                  .groupBy().agg(func.avg('value').alias('average')).collect()[0]['average']


def biggest_connected_component(graph):
    '''Return the biggest connected component
    '''
    return graph.connectedComponents(algorithm='graphx').groupBy('component').agg(
        func.countDistinct('id').alias('size')
    ).sort(func.desc('size')).rdd.take(1)[0]


def biggest_strong_connected_component(graph):
    '''Return the biggest strong connected component
    '''
    return graph.stronglyConnectedComponents(maxIter=10).groupBy('component').agg(
        func.countDistinct('id').alias('size')
    ).sort(func.desc('size')).rdd.take(1)[0]


def largest_wcc(graph):
    '''Returns the percentage that the biggest connect component covers
    '''
    print 'Calculating largest connect component coverage'
    print ' - Calculating largest connect component size'
    size = biggest_connected_component(graph)['size']
    print ' - Calculating number of vertices in graph'
    return (size / float(n_of_vertices(graph))) * 100


def largest_scc(graph):
    '''Returns the percentage that the biggest strong connect component covers
    '''
    print 'Calculating largest strong connect component coverage'
    print ' - Calculating largest strong connect component size'
    size = biggest_strong_connected_component(graph)['size']
    print ' - Calculating number of vertices in graph'
    return (size / float(n_of_vertices(graph))) * 100


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


def average_degree(graph):
    '''Return the average degree of the graph
    '''
    print 'Calculating Average Degree'
    vertices = graph.vertices
    edges = graph.edges.where('src != dst')

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

    print ' - Counting neighbors'
    n_of_neighbors = neighbors_ids.select(
        'id', func.size('neighbors').alias('n_of_neighbors'))
    return n_of_neighbors.agg(
        func.avg('n_of_neighbors').alias('avg')
    ).take(1)[0]['avg']


def n_of_vertices(graph):
    '''Return the number of vertices in graph
    '''
    return graph.vertices.count()


def n_of_edges(graph):
    '''Return the number of edges in graph
    '''
    return graph.edges.count()


def print_result(result):
    '''Prints the result with the word 'RESULT: ' before
    '''
    print 'RESULT: ', result


def calculate_architecture(graph):
    number_of_vertices = n_of_vertices(graph)
    print 'Number of vertices: ', number_of_vertices

    number_of_edges = n_of_edges(graph)
    print 'Number of edges: ', number_of_edges

    avg_degree = average_degree(graph)
    print_result(avg_degree)

    assortativity = Assortativity(graph).calculate()
    print_result(assortativity)

    lcc = LocalClusteringCoefficient(graph).calculate_average()
    print_result(lcc)

    avg_shortest_path = average_shortest_path(graph)
    print_result(avg_shortest_path)

    wcc = largest_wcc(graph)
    print_result(wcc)

    scc = largest_scc(graph)
    print_result(scc)

    print '@@@@@@@@@@@@@@@@@@@@ ARCHITECTURE RESULT @@@@@@@@@@@@@@@@'
    schema = ["N of nodes", "N of edges", "Avg. Degree", "Clustering Coef.",
              "Avg. Path Length", "Assorativity Coef.", "Largest SCC", "Largest WCC"]
    data = (number_of_vertices, number_of_edges, avg_degree, lcc,
            avg_shortest_path, assortativity, scc, wcc)
    result = sql_context.createDataFrame([data], schema)
    result.show()
    print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@'


def community_info(graph):
    graph.labelPropagation(maxIter=10)


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

    print 'Building graph'

    graph = clean_graph(GraphFrame(candidates, donations))

    # calculate_architecture(graph)

    write_graph_to_file(graph, 'curitiba_full')

    print '******************** FINISHING SCRIPT *******************'
    print '*********************************************************'

if __name__ == '__main__':
    main()
