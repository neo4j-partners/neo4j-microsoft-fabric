CREATE INDEX customer_id_index IF NOT EXISTS FOR (c:Customer) on c.customerID;
CREATE INDEX order_id_index IF NOT EXISTS FOR (o:Order) on o.orderID;
CREATE INDEX product_id_index IF NOT EXISTS FOR (p:Product) on p.productID;
CREATE INDEX supplier_id_index IF NOT EXISTS FOR (s:Supplier) on s.supplierID;
CREATE CONSTRAINT cust_id_cnstrnt IF NOT EXISTS FOR (c:Customer) REQUIRE (c.customerID) IS NODE KEY;
CREATE CONSTRAINT ord_id_cnstrnt IF NOT EXISTS FOR (o:Order) REQUIRE (o.orderID) IS NODE KEY;
CREATE CONSTRAINT prd_id_cnstrnt IF NOT EXISTS FOR (p:Product) REQUIRE (p.productID) IS NODE KEY;
CREATE CONSTRAINT sup_id_cnstrnt IF NOT EXISTS FOR (s:Supplier) REQUIRE (s.supplierID) IS NODE KEY;


//Customers
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/customer/customers.csv' AS row
MERGE (c:Customer {customerID: row.customerID})
SET c.companyName = row.companyName,
    c.contactName = row.contactName,
    c.contactTitle = row.contactTitle,
    c.address = row.address,
    c.city = row.city,
    c.region = row.region,
    c.postalCode = row.postalCode,
    c.country = row.country,
    c.phone = row.phone,
    c.fax = row.fax;

//Suppliers
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/supplier/suppliers.csv' AS row
MERGE (s:Supplier {supplierID: row.supplierID})
SET s.companyName = row.companyName,
    s.contactName = row.contactName,
    s.contactTitle = row.contactTitle,
    s.address = row.address,
    s.city = row.city,
    s.region = row.region,
    s.postalCode = row.postalCode,
    s.country = row.country,
    s.phone = row.phone;

//Categories
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/products/categories.csv' AS row
MERGE (c:Category {categoryID: row.categoryID})
SET c.categoryName = row.categoryName,
    c.description = row.description;

// Load Nodes with Relationships
// Load nodes that connect back to the independent nodes we just created.

//Products
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/products/products.csv' AS row
MERGE (p:Product {productID: row.productID})
SET p.productName = row.productName,
    p.quantityPerUnit = row.quantityPerUnit,
    p.unitPrice = toFloat(row.unitPrice),
    p.unitsInStock = toInteger(row.unitsInStock),
    p.unitsOnOrder = toInteger(row.unitsOnOrder),
    p.reorderLevel = toInteger(row.reorderLevel),
    p.discontinued = (row.discontinued <> '0')
WITH p, row
MATCH (s:Supplier {supplierID: row.supplierID})
MERGE (s)-[:SUPPLIES]->(p)
WITH p, row
MATCH (c:Category {categoryID: row.categoryID})
MERGE (p)-[:PART_OF]->(c);

//Orders
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/order/orders.csv' AS row
MERGE (o:Order {orderID: row.orderID})
SET o.orderDate = row.orderDate,
    o.requiredDate = row.requiredDate,
    o.shippedDate = row.shippedDate,
    o.freight = toFloat(row.freight),
    o.shipName = row.shipName,
    o.shipAddress = row.shipAddress,
    o.shipCity = row.shipCity,
    o.shipCountry = row.shipCountry

WITH o, row
MATCH (c:Customer {customerID: row.customerID})
MERGE (c)-[:PURCHASED]->(o);


//Load the Join Table (Relationships)
//Connect Orders to Products using the order-details CSV.

//Order Details
LOAD CSV WITH HEADERS FROM '<YOUR_FILE_URL_BASE>/order/order-details.csv' AS row
MATCH (o:Order {orderID: row.orderID})
MATCH (p:Product {productID: row.productID})
MERGE (o)-[rel:ORDERS]->(p)
SET rel.unitPrice = toFloat(row.unitPrice),
    rel.quantity = toInteger(row.quantity),
    rel.discount = toFloat(row.discount);
