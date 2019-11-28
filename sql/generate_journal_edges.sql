use citation;

INSERT INTO journal_edges (edge_from, edge_to)
Select papers_from.journal_id, papers_to.journal_id From paper_edges
Left Join papers papers_from on paper_edges.edge_from=papers_from.paper_id
Left Join papers papers_to on paper_edges.edge_to=papers_to.paper_id
Where papers_from.journal_id is not null and papers_to.journal_id is not null
Limit 100
ON Duplicate Key update journal_edges.weight = journal_edges.weight+1;
