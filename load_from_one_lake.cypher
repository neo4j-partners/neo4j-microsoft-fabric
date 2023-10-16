:params {accessToken:'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSIsImtpZCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzU0ZTg1NzI1LWVkMmEtNDlhNC1hMTllLTExYzhkMjlmOWEwZi8iLCJpYXQiOjE2OTc0MzcwNjksIm5iZiI6MTY5NzQzNzA2OSwiZXhwIjoxNjk3NDQyMzI5LCJhY3IiOiIxIiwiYWlvIjoiQVRRQXkvOFVBQUFBeWtQUUJaOUpJVE90RjhoTmpRckp4VDBEczFBVnB0ZThuUEhYNGhXMzJsaldOODBkOENXeTBSR3IzK2xXTE5MQiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiJiNjc3YzI5MC1jZjRiLTRhOGUtYTYwZS05MWJhNjUwYTRhYmUiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6IlNpdmFqaSIsImdpdmVuX25hbWUiOiJHdWhhbiIsImdyb3VwcyI6WyJiMzJhMTEwYS1mY2JmLTQwZjQtYWU1Ny1iZmY5NWUwOGFjNDUiLCIzN2Q0YTEzNy1iOTU3LTQzYjgtODM4Mi1mNjYyMjZiODBmYWIiXSwiaXBhZGRyIjoiOTYuMjQwLjE3LjE0NyIsIm5hbWUiOiJHdWhhbiBTaXZhamkiLCJvaWQiOiJhNDU5MDRiMC1jNzZmLTQ3N2MtODM3MC1kNDczMDUzNTZkMWEiLCJwdWlkIjoiMTAwMzIwMDI1MjM2RDA5NiIsInJoIjoiMC5BWFlBSlZmb1ZDcnRwRW1obmhISTBwLWFENEdtQnVUVTg2aENrTGJDc0NsSmV2RjJBUFkuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUDB1d3UyN09xdWJVTVYzTlZqc1pBZ01pWlp5S3J4bTE3R3Y5RDVtV2x3YyIsInRpZCI6IjU0ZTg1NzI1LWVkMmEtNDlhNC1hMTllLTExYzhkMjlmOWEwZiIsInVuaXF1ZV9uYW1lIjoiZ3VoYW4uc2l2YWppQG5lbzRqLmNvbSIsInVwbiI6Imd1aGFuLnNpdmFqaUBuZW80ai5jb20iLCJ1dGkiOiJSSjFZMGkxc0hrNlFjMUp3UEl5MEFBIiwidmVyIjoiMS4wIn0.ZQj9jSARdPLi7hbzZVMhcsTda6tTlqrUN6zj08Ic-4nMp-NDLREsBguNqmYKdTLsI5LAuJNyTKWaYbM8VO4rxHdTCBXaMrf6vc73WV79Mk2Zgzxh7MpC3nNDT2lcgl3W72dZsY4D0YRPVgkjeeRpy1sbUplDjaTanBYzws-Xx4-9k2L5l1d-sLgUQbSA-SVyiRbehty6rXyTpQndC8bSGMTE1VEKx__0xTTSjRn0Us8fMJB5CJpwZzUtJQBXaECBIQwDNyDJOGTavQ1RJ_f78DGATWvF3gXSAnMgxddB9ZMDm-gIyA5xInVWcqh9WvbEbqAW_EFWM2KOBNg92iSD4g',
productFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/products.json',
customerFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/customers.json',
orderFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/orders.json',
supplierFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/suppliers.json',
orderDetailFileURL:'https://onelake.dfs.fabric.microsoft.com/guhanworkspace/myLakehouse.Lakehouse/Files/Northwind/order-details.json'};
#Load Product 
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
