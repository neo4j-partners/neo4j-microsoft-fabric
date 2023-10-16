echo 'Getting Access Token from Azure Active Directory...'

export ACCESS_TOKEN=$(az account get-access-token --resource https://storage.azure.com/ --query "accessToken" --output tsv)

git reset --hard

git pull https://github.com/neo4j-partners/neo4j-microsoft-fabrlc.git

envsubst '$ACCESS_TOKEN' < load_from_one_lake.cypher > new_load_from_one_lake.cypher

echo 'Now loading One Lake Data into Neo4j database...'

cat new_load_from_one_lake.cypher | cypher-shell -u neo4j -p P@55w0rd@2023  -a neo4j://node000000:7687

echo 'Successfully loaded data from One Lake into Neo4j Database!'
