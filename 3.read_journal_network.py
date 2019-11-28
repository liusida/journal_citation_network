import json
import pymysql
import pymysql.cursors
import sys
import networkx as nx

G = nx.DiGraph()
connection = pymysql.connect(host='localhost',
                             user='citation',
                             password='password',
                             db='citation',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()

try:
    sql = """
    select (j_from.journal_name) as edge_from, (j_to.journal_name) as edge_to, weight from 
    (SELECT * FROM `journal_edges` where edge_from<>edge_to order by weight desc limit 1000) t
    left join journals j_from on j_from.journal_id=t.edge_from
    left join journals j_to on j_to.journal_id=t.edge_to
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    edges = []
    for row in rows:
        edges.append([row['edge_from'], row['edge_to'], row['weight']])
    G.add_weighted_edges_from(edges)
    print(G.number_of_nodes())

except pymysql.err.MySQLError as e:
    print("When processing select")
    print('Got error {!r}, errno is {}'.format(e, e.args[0]))    

# try:
#     sql = "SELECT * FROM journals"
#     cursor.execute(sql)
#     rows = cursor.fetchall()
#     for row in rows:
#         if G.has_node(row['journal_id']):
#             G.nodes[row['journal_id']]['title'] = row['journal_name']
# except pymysql.err.MySQLError as e:
#     print("When processing select")
#     print('Got error {!r}, errno is {}'.format(e, e.args[0]))    

nx.readwrite.graphml.write_graphml(G,"journal_citation_network.graphml")