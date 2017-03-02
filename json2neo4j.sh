projId=syn4921369

python json2csv.py ${projId}.json --edge_csv ${projId}_edges.csv --node_csv ${projId}_nodes.csv

head -n 1 ${projId}_nodes.csv | sed "s/_id/_id:ID/" > ${projId}_nodes_UsedURL.csv
grep org.sagebionetworks.repo.model.provenance.UsedURL ${projId}_nodes.csv >> ${projId}_nodes_UsedURL.csv
grep org.sagebionetworks.repo.model.provenance.Activity ${projId}_nodes.csv >> ${projId}_nodes_Activity.csv
grep org.sagebionetworks.repo.model.FileEntity ${projId}_nodes.csv >> ${projId}_nodes_Entity.csv

sed -i -e "s/_inV/_inV:END_ID/" -e "s/_outV/_outV:START_ID/" ${projId}_edges.csv

for x in generatedBy used executed ; do
    head -n 1 ${projId}_edges.csv > ${projId}_edges_${x}.csv ;
    grep ${x} ${projId}_edges.csv >> ${projId}_edges_${x}.csv ;
done

~/neo4j-community-3.1.0/bin/neo4j-import --into ~/neo4j-community-3.1.0/data/databases/graph.db --nodes:Entity ${projId}_nodes_Entity.csv --nodes:Entity ${projId}_nodes_UsedURL.csv --nodes:Activity ${projId}_nodes_Activity.csv --relationships:GENERATED_BY ${projId}_edges_generatedBy.csv --relationships:USED ${projId}_edges_used.csv --relationships:EXECUTED ${projId}_edges_executed.csv
826  history
