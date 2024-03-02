echo 'Getting Access Token from Azure Active Directory...'

export ACCESS_TOKEN=$(az account get-access-token --resource https://storage.azure.com/ --query "accessToken" --output tsv)

git reset --hard

git pull https://github.com/neo4j-partners/neo4j-microsoft-fabrlc.git

envsubst '$ACCESS_TOKEN,$FABRIC_LH_PATH' < load_from_lakehouse.cypher > new_load_from_lakehouse.cypher

clear

cat new_load_from_lakehouse.cypher
