:params {accessToken:'Bearer $ACCESS_TOKEN',
productFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/products.json',
categoriesFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/categories.json',
customerFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/customers.json',
orderFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/orders.json',
supplierFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/suppliers.json',
orderDetailFileURL:'https://onelake.dfs.fabric.microsoft.com/Neo4j_Workspace/myLakehouse.Lakehouse/Files/Northwind/order-details.json'};
CALL apoc.load.jsonParams($productFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (product:Product{productID:value.productID})
SET product.productName = value.productName, product.quantityPerUnit=value.quantityPerUnit, product.unitPrice=value.unitPrice, product.unitsInStock=value.unitsInStock, product.reorderLevel=value.reorderLevel, product.discontinued=value.discontinued
with product, value
UNWIND value.categoryID  AS category
MERGE (cat:Category{categoryID: value.categoryID})
SET cat.categoryName = value.categoryName, cat.description=value.description
MERGE (product)-[:BELONGS_TO]->(cat)
with product, value
UNWIND value.supplierID AS supplier
MERGE (supp:Supplier{supplierID: value.supplierID})
MERGE (product)-[:SUPPLIED_BY]->(supp);
CALL apoc.load.jsonParams($categoriesFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (category:Category{categoryID:value.categoryID})
SET category.categoryName = value.categoryName, category.description=value.description, category.picture=value.picture;
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
MERGE (a)-[:LOCATED_AT]->(c)
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
MERGE (saddr:Address{addressID: value.orderID})
SET saddr.shipAddress = value.shipAddress, saddr.shipCity=value.shipCity,saddr.shipRegion=value.shipRegion,saddr.shipPostalCode=value.shipPostalCode,saddr.shipCountry=value.shipCountry
MERGE (o)-[:SHIPPED_TO]->(saddr)
WITH o, value
UNWIND value.customerID AS customer
MERGE (customerOrd:Customer{customerID:value.customerID})
MERGE (customerOrd)-[:ORDERED]->(o)
WITH o, value
UNWIND value.employeeID AS emp
MERGE (empOrd:Employee{employeeID:value.employeeID})
MERGE (o)-[:ORDER_PROCESSED_BY]->(empOrd);
CALL apoc.load.jsonParams($orderDetailFileURL,{Authorization:$accessToken},null)
YIELD value MERGE (o:Order{orderID:value.orderID})
WITH o, value
UNWIND value.productID AS prdO
MERGE (prdOrd:Product{productID: value.productID})
MERGE (o)-[:ORDER_CONTAINS]->(prdOrd);
MATCH (c:Customer)-[:ORDERED]->(:Order)-[:ORDER_CONTAINS]->(p:Product)
WITH DISTINCT c, p
MERGE (c)-[:PURCHASED]->(p);
