/////////////////////
// 1) Load product data to create product,category and suppliers graph
/////////////////////
//load product data
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/products.json')
YIELD value
//create product nodes
MERGE (product:Product{productID:value.productID})
SET product.productName = value.productName,
    product.quantityPerUnit=value.quantityPerUnit,
    product.unitPrice=value.unitPrice,
    product.unitsInStock=value.unitsInStock,
    product.reorderLevel=value.reorderLevel,
    product.discontinued=value.discontinued
//create category nodes and belongs-to relationships between product and category
MERGE (cat:Category{categoryID: value.categoryID})
MERGE (product)-[:BELONGS_TO]->(cat)
//create supplier nodes and supplied-by relationship between product and supplier
MERGE (supp:Supplier{supplierID: value.supplierID})
MERGE (product)-[:SUPPLIED_BY]->(supp);

/////////////////////
// 2) Load category data to set properties on category nodes
/////////////////////
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/categories.json')
YIELD value
MERGE (category:Category{categoryID:value.categoryID})
SET category.categoryName = value.categoryName,
    category.description=value.description,
    category.picture=value.picture;

/////////////////////
// 3) Load supplier data to set properties on supplier nodes and create supplier, address, contact subgraph
/////////////////////
//load supplier data
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/suppliers.json')
YIELD value
//set properties for supplier nodes
MERGE (s:Supplier{supplierID:value.supplierID})
SET s.supplierName = value.companyName
//merge address nodes and located-at relationship between supplier and address
MERGE (sa:Address{addressID: value.supplierID})
SET sa.address = value.address,
    sa.city = value.city,
    sa.region = value.region,
    sa.postalCode = value.postalCode,
    sa.country = value.country
MERGE (s)-[:LOCATED_AT]->(sa)
//merge contact nodes and has-contact relationship between supplier and contact
MERGE (scnt:Contact{contactID: value.supplierID})
SET scnt.contactID = value.supplierID, scnt.contactName=value.contactName,scnt.contactTitle=value.contactTitle
MERGE (s)-[:HAS_CONTACT]->(scnt);

/////////////////////
// 4) Load customer data to create customer, address, contact subgraph
/////////////////////
//load customer data
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/customers.json')
YIELD value
//create customer nodes
MERGE (c:Customer{customerID:value.customerID})
SET c.customerName = value.companyName,
    c.Bloom_Link=value.Bloom_Link
//merge address nodes and located-at relationship between customer and address
MERGE (a:Address{addressID: value.customerID})
SET a.address = value.address,
    a.city=value.city,
    a.region = value.region,
    a.postalCode = value.postalCode,
    a.country=value.country
MERGE (c)-[:LOCATED_AT]->(a)
//merge contact nodes and has-contact relationship between customer and contact
MERGE (cnt:Contact{contactID: value.customerID})
SET cnt.contactID = value.customerID,
    cnt.contactName=value.contactName,
    cnt.contactTitle=value.contactTitle,
    cnt.phone=value.phone,
    cnt.fax=value.fax
MERGE (c)-[:HAS_CONTACT]->(cnt);

/////////////////////
// 5) Load order data to create order, shipping address, customer, processing employee subgraph
/////////////////////
//load order data
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/orders.json')
YIELD value
MERGE (o:Order{orderID:value.orderID})
SET o.orderDate = value.orderDate,
    o.shippedDate=value.shippedDate,
    o.shipVia=value.shipVia,
    o.freight=value.freight
//merge address nodes to represent shipping address and shipped-to relationships between order and address
MERGE (saddr:Address{addressID: value.orderID})
SET saddr.address = value.shipAddress,
    saddr.city=value.shipCity,
    saddr.region=value.shipRegion,
    saddr.postalCode=value.shipPostalCode,
    saddr.country=value.shipCountry
MERGE (o)-[:SHIPPED_TO]->(saddr)
//merge customer nodes and ordered relationships between order and customer
MERGE (customerOrd:Customer{customerID:value.customerID})
MERGE (customerOrd)-[:ORDERED]->(o)
//create employee nodes and processed-by relationships between order and employee
MERGE (empOrd:Employee{employeeID:value.employeeID})
MERGE (o)-[:PROCESSED_BY]->(empOrd);

/////////////////////
// 6) load order details data to create order contains product relationships
/////////////////////
//load order-details data
CALL apoc.load.json('https://raw.githubusercontent.com/neo4j-partners/neo4j-microsoft-fabrlc/main/data/json/order-details.json')
YIELD value
MERGE (o:Order{orderID:value.orderID})
MERGE (prdOrd:Product{productID: value.productID})
MERGE (o)-[:ORDER_CONTAINS]->(prdOrd);

/////////////////////
// 7) create purchased relationship between customer and everything in their order
/////////////////////
MATCH (c:Customer)-[:ORDERED]->(:Order)-[:ORDER_CONTAINS]->(p:Product)
WITH DISTINCT c, p
MERGE (c)-[:PURCHASED]->(p);
