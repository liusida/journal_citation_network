import json
import pymysql
import pymysql.cursors
import sys

# Careful, Don't need to run again unless we mess up with the data in mysql.
exit()

sql_drop_target_table = "DROP TABLE IF EXISTS %s "
sql_create_journal_edges = "CREATE TABLE `citation`.`journal_edges` ( `edge_from` BIGINT UNSIGNED NOT NULL,  `edge_to` BIGINT UNSIGNED NOT NULL,  `weight` BIGINT UNSIGNED NULL DEFAULT 1, PRIMARY KEY (`edge_from`, `edge_to`))"

connection = pymysql.connect(host='localhost',
                             user='citation',
                             password='password',
                             db='citation',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()

try:
    cursor.execute(sql_drop_target_table%"journal_edges")

    
    cursor.execute(sql_create_journal_edges)
    connection.commit()
except:
    print("database error 1")
    exit()

print("Drop and Create...done", flush=True)

try:
    sql = "SELECT count(*) as c FROM `paper_edges`"
    cursor.execute(sql)
    ans = cursor.fetchone()
    total_edges = ans['c']
except pymysql.err.MySQLError as e:
    print("When processing select")
    print('Got error {!r}, errno is {}'.format(e, e.args[0]))    

for i in range(100000):
    limitation = 1000000
    start_point = 0 + limitation * i
    if start_point>=total_edges:
        break
    print(start_point, ",", limitation, ". total_edges:", total_edges)
    try:
        sql = "INSERT INTO journal_edges (edge_from, edge_to) Select papers_from.journal_id, papers_to.journal_id From (SELECT * FROM paper_edges LIMIT "+str(start_point)+","+str(limitation)+") paper_edges Left Join papers papers_from on paper_edges.edge_from=papers_from.paper_id Left Join papers papers_to on paper_edges.edge_to=papers_to.paper_id Where papers_from.journal_id is not null and papers_to.journal_id is not null ON Duplicate Key update journal_edges.weight = journal_edges.weight+1"
        cursor.execute(sql)
        connection.commit()
    except pymysql.err.MySQLError as e:
        print("When processing commit")
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))

try:
    sql = "Select sum(weight) as s from journal_edges"
    cursor.execute(sql)
    weight_sum = cursor.fetchone()
    print("Total weights in journal_edges is ", weight_sum['s'])
except pymysql.err.MySQLError as e:
    print("When processing select sum")
    print('Got error {!r}, errno is {}'.format(e, e.args[0]))    