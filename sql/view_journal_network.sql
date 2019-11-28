SELECT 
(journals_from.journal_name) as from_name,
(journals_to.journal_name) as to_name,
weight FROM citation.journal_edges 
Left Join journals journals_from on journals_from.journal_id=journal_edges.edge_from 
Left Join journals journals_to on journals_to.journal_id=journal_edges.edge_to
order by weight desc;