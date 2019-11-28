import json
import pymysql
import pymysql.cursors
import sys

# Careful, Don't need to run again unless we mess up with the data in mysql.
exit()

db_filename = "dblp_papers_v11.txt"

sql_drop_target_table = "DROP TABLE IF EXISTS %s "
sql_create_edges_table = "CREATE TABLE `paper_edges` ( `edge_from` BIGINT UNSIGNED NOT NULL, `edge_to` BIGINT UNSIGNED NOT NULL, PRIMARY KEY (`edge_from`, `edge_to`))"
sql_create_papers_table = "CREATE TABLE `papers` (  `paper_id` BIGINT unsigned NOT NULL,  `paper_title` varchar(255) DEFAULT NULL,  `journal_id` BIGINT unsigned NOT NULL,  PRIMARY KEY (`paper_id`,`journal_id`),  UNIQUE KEY `paper_id_UNIQUE` (`paper_id`))"
sql_create_journals_table = "CREATE TABLE `journals` (  `journal_id` BIGINT unsigned NOT NULL,  `journal_name` varchar(255) DEFAULT NULL,  PRIMARY KEY (`journal_id`),  UNIQUE KEY `journal_id_UNIQUE` (`journal_id`))"

connection = pymysql.connect(host='localhost',
                             user='citation',
                             password='password',
                             db='citation',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()
try:
    cursor.execute(sql_drop_target_table%"paper_edges")
    cursor.execute(sql_drop_target_table%"papers")
    cursor.execute(sql_drop_target_table%"journals")
    
    cursor.execute(sql_create_edges_table)
    cursor.execute(sql_create_papers_table)
    cursor.execute(sql_create_journals_table)
    connection.commit()
except:
    print("database error 1")
    exit()

print("Drop and Create...done", flush=True)

edges = []
papers = []

with open(db_filename, "r") as f:
    line = f.readline()
    for i in range(100000000):

        obj = json.loads(line)
        error = False
        try:
            if obj["doc_type"]!="Journal":
                error = True
            else:
                paper_id = obj["id"]
                paper_title = obj["title"]
                paper_title = (paper_title[:250] + '..') if len(paper_title) > 250 else paper_title
                journal_name = obj["venue"]["raw"]
                journal_id = obj["venue"]["id"]
                references = obj["references"]
        except:
            error = True
        if not error:
            #print(i,")",[paper_id])
            #targets = split(references)
            try:
                for target in references:
                    sql = "INSERT INTO `paper_edges` (`edge_from`, `edge_to`) VALUES (%s, %s)"
                    cursor.execute(sql, (int(paper_id), int(target)))

                    sql = "INSERT INTO `papers` (`paper_id`, `paper_title`, `journal_id`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `paper_id`=`paper_id`"
                    cursor.execute(sql, (int(paper_id), paper_title, int(journal_id)))

                    sql = "INSERT INTO `journals` (`journal_id`, `journal_name`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `journal_id`=`journal_id`"
                    cursor.execute(sql, (int(journal_id), journal_name))
            except pymysql.err.MySQLError as e:
                print("When processing  ", journal_id, journal_name)
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except:
                print("database error 2",  sys.exc_info()[0], "\n\n", journal_id, journal_name)

        if i%10000==0:
            print(i,")", line[:60], flush=True)
            try:
                connection.commit()
            except:
                print("database commit error")

        line = f.readline()
        if line=="":
            break

try:
    connection.commit()
except:
    print("database commit error")

print(len(papers))
print("ok")
