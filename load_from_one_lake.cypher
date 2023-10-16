:params {accessToken:'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSIsImtpZCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzU0ZTg1NzI1LWVkMmEtNDlhNC1hMTllLTExYzhkMjlmOWEwZi8iLCJpYXQiOjE2OTc0MzE0NjksIm5iZiI6MTY5NzQzMTQ2OSwiZXhwIjoxNjk3NDM2OTM0LCJhY3IiOiIxIiwiYWlvIjoiQVRRQXkvOFVBQUFBc1p5eHBRbUQzL1lheWk1Y2hSSFBidzBGcTNjREx6UXZqSHJPTWxaclFCOWpqdXA2cUxpUVFzSnFUZFRUZFpTcCIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiJiNjc3YzI5MC1jZjRiLTRhOGUtYTYwZS05MWJhNjUwYTRhYmUiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6IlNpdmFqaSIsImdpdmVuX25hbWUiOiJHdWhhbiIsImdyb3VwcyI6WyJiMzJhMTEwYS1mY2JmLTQwZjQtYWU1Ny1iZmY5NWUwOGFjNDUiLCIzN2Q0YTEzNy1iOTU3LTQzYjgtODM4Mi1mNjYyMjZiODBmYWIiXSwiaXBhZGRyIjoiOTYuMjQwLjE3LjE0NyIsIm5hbWUiOiJHdWhhbiBTaXZhamkiLCJvaWQiOiJhNDU5MDRiMC1jNzZmLTQ3N2MtODM3MC1kNDczMDUzNTZkMWEiLCJwdWlkIjoiMTAwMzIwMDI1MjM2RDA5NiIsInJoIjoiMC5BWFlBSlZmb1ZDcnRwRW1obmhISTBwLWFENEdtQnVUVTg2aENrTGJDc0NsSmV2RjJBUFkuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUDB1d3UyN09xdWJVTVYzTlZqc1pBZ01pWlp5S3J4bTE3R3Y5RDVtV2x3YyIsInRpZCI6IjU0ZTg1NzI1LWVkMmEtNDlhNC1hMTllLTExYzhkMjlmOWEwZiIsInVuaXF1ZV9uYW1lIjoiZ3VoYW4uc2l2YWppQG5lbzRqLmNvbSIsInVwbiI6Imd1aGFuLnNpdmFqaUBuZW80ai5jb20iLCJ1dGkiOiJOR3dmQTN4OFRrbWxFVk52a3AwRkFRIiwidmVyIjoiMS4wIn0.HqS2Hhfh_0S2pmeG3447c1QayLHDB6-fkSIKiosniribIOXnieH3FytwxFmHG5ILWwTZkSABDfwKJ6qhht6qUH76cOnR-Kmegm_M2BqYevfIgOBE1OAwQh64W27izKo3HgPss5XX1GaOXPM1eHd8Blogz_rK7xDQabXh0QBNnllZqWEygw1c72Qx3bzIvKSuzRfm4QnMokIyQ3OR-aRhcaTDPOalcLOg7xotEvs1zKa6ZFuJsE8wdztqSUa-mMRpKMCdpWxt2z22DVy36d0XALjEEWT5Q2tFNY0vwYV5vF7uK5N719kmlIMqiWO8ICilFbygPhPM3EvER9c5W8GuPQ',
productFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/products.json',
customerFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/customers.json',
orderFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/orders.json',
supplierFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/suppliers.json'};
CALL apoc.load.jsonParams($productFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (product:Product{productID:value.productID})
SET product.name = value.name
with product, value 
UNWIND value.categoryID  AS category
MERGE (cat:Category{categoryID: value.categoryID})
SET cat.categoryName = value.categoryName, cat.description=value.description
MERGE (product)-[:BELONGS_TO]->(cat);
CALL apoc.load.jsonParams($customerFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (c:Customer{customerID:value.customerID})
SET c.companyName = value.companyName, c.Bloom_Link=value.Bloom_Link
WITH c, value
UNWIND value.customerID AS address
MERGE (a:Address{addressID: value.customerID})
SET a.address = value.address, a.city=value.city,a.country=value.country
MERGE (a)-[:LOCATED_AT]->(c)
WITH c, value
UNWIND value.customerID AS contact
MERGE (cnt:Contact{contactID: value.customerID})
SET cnt.contactID = value.customerID, cnt.contactName=value.contactName,cnt.contactTitle=value.contactTitle
MERGE (cnt)-[:COMPANY_CONTACT]->(c);
CALL apoc.load.jsonParams($supplierFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (c:Supplier{supplierID:value.supplierID})
SET c.companyName = value.companyName
WITH c, value
UNWIND value.supplierID AS address
MERGE (a:Address{addressID: value.supplierID})
SET a.address = value.address, a.city=value.city,a.country=value.country
MERGE (a)-[:SUPPLIER_LOC]->(c)
WITH c, value
UNWIND value.supplierID AS contact
MERGE (cnt:Contact{contactID: value.supplierID})
SET cnt.contactID = value.supplierID, cnt.contactName=value.contactName,cnt.contactTitle=value.contactTitle
MERGE (cnt)-[:SUPPLIER_CONTACT]->(c);
CALL apoc.load.jsonParams($orderFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (o:Order{orderID:value.orderID})
SET o.orderDate = value.orderDate, o.shippedDate=value.shippedDate,o.shipVia=value.shipVia,o.freight=value.freight
WITH o, value
UNWIND value.orderID AS address
MERGE (saddr:ShippingAddress{addressID: value.orderID})
SET saddr.shipAddress = value.shipAddress, saddr.shipCity=value.shipCity,saddr.shipRegion=value.shipRegion,saddr.shipPostalCode=value.shipPostalCode,saddr.shipCountry=value.shipCountry
MERGE (o)-[:SHIPPED_TO]->(saddr)
