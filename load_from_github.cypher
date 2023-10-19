CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/products.json',null,null)
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
CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/categories.json',null,null)
YIELD value MERGE (category:Category{categoryID:value.categoryID})
SET category.categoryName = value.categoryName, category.description=value.description, category.picture=value.picture;
CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/customers.json',null,null)
YIELD value MERGE (c:Customer{customerID:value.customerID})
SET c.customerName = value.companyName, c.Bloom_Link=value.Bloom_Link
WITH c, value
UNWIND value.customerID AS address
MERGE (a:Address{addressID: value.customerID})
SET a.address = value.address, a.city=value.city,a.region = value.region,a.postalCode = value.postalCode,a.country=value.country
MERGE (c)-[:LOCATED_AT]->(a)
WITH c, value
UNWIND value.customerID AS contact
MERGE (cnt:Contact{contactID: value.customerID})
SET cnt.contactID = value.customerID, cnt.contactName=value.contactName,cnt.contactTitle=value.contactTitle,cnt.phone=value.phone, cnt.fax=value.fax
MERGE (c)-[:HAS_CONTACT]->(cnt);
CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/suppliers.json',null,null)
YIELD value MERGE (s:Supplier{supplierID:value.supplierID})
SET s.supplierName = value.companyName
WITH s, value
UNWIND value.supplierID AS address
MERGE (sa:Address{addressID: value.supplierID})
SET sa.address = value.address,sa.city = value.city,sa.region = value.region,sa.postalCode = value.postalCode,sa.country = value.country
MERGE (s)-[:LOCATED_AT]->(sa)
WITH s, value
UNWIND value.supplierID AS contact
MERGE (scnt:Contact{contactID: value.supplierID})
SET scnt.contactID = value.supplierID, scnt.contactName=value.contactName,scnt.contactTitle=value.contactTitle
MERGE (s)-[:HAS_CONTACT]->(scnt);
CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/orders.json',null,null)
YIELD value MERGE (o:Order{orderID:value.orderID})
SET o.orderDate = value.orderDate, o.shippedDate=value.shippedDate,o.shipVia=value.shipVia,o.freight=value.freight
WITH o, value
UNWIND value.orderID AS address
MERGE (saddr:Address{addressID: value.orderID})
SET saddr.address = value.shipAddress, saddr.city=value.shipCity,saddr.region=value.shipRegion,saddr.postalCode=value.shipPostalCode,saddr.country=value.shipCountry
MERGE (o)-[:SHIPPED_TO]->(saddr)
WITH o, value
UNWIND value.customerID AS customer
MERGE (customerOrd:Customer{customerID:value.customerID})
MERGE (customerOrd)-[:ORDERED]->(o)
WITH o, value
UNWIND value.employeeID AS emp
MERGE (empOrd:Employee{employeeID:value.employeeID})
MERGE (o)-[:PROCESSED_BY]->(empOrd);
CALL apoc.load.jsonParams('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/order-details.json',null,null)
YIELD value MERGE (o:Order{orderID:value.orderID})
WITH o, value
UNWIND value.productID AS prdO
MERGE (prdOrd:Product{productID: value.productID})
MERGE (o)-[:ORDER_CONTAINS]->(prdOrd);
MATCH (c:Customer)-[:ORDERED]->(:Order)-[:ORDER_CONTAINS]->(p:Product)
WITH DISTINCT c, p
MERGE (c)-[:PURCHASED]->(p);
