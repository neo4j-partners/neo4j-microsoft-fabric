CREATE INDEX customer_id_index IF NOT EXISTS FOR (c:Customer) on c.customerID;
CREATE INDEX order_id_index IF NOT EXISTS FOR (o:Order) on o.orderID;
CREATE INDEX product_id_index IF NOT EXISTS FOR (p:Product) on p.productID;
CREATE INDEX supplier_id_index IF NOT EXISTS FOR (s:Supplier) on s.supplierID;
CREATE CONSTRAINT cust_id_cnstrnt IF NOT EXISTS FOR (c:Customer) REQUIRE (c.customerID) IS NODE KEY;
CREATE CONSTRAINT ord_id_cnstrnt IF NOT EXISTS FOR (o:Order) REQUIRE (o.orderID) IS NODE KEY;
CREATE CONSTRAINT prd_id_cnstrnt IF NOT EXISTS FOR (p:Product) REQUIRE (p.productID) IS NODE KEY;
CREATE CONSTRAINT sup_id_cnstrnt IF NOT EXISTS FOR (s:Supplier) REQUIRE (s.supplierID) IS NODE KEY;
