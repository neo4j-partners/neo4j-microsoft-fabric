echo 'Getting Access Token from Azure Active Directory...'

export ACCESS_TOKEN=$(az account get-access-token --resource https://storage.azure.com/ --query "accessToken" --output tsv)

git reset --hard

git pull https://github.com/neo4j-partners/neo4j-microsoft-fabrlc.git

envsubst '$ACCESS_TOKEN,$NEO4J_DB_USER_NAME,$NEO4J_DB_PASSWORD,$NEO4J_DB_SERVER_URL' < load_from_lakehouse.cypher > new_load_from_lakehouse.cypher

clear

cat new_load_from_lakehouse.cypher
