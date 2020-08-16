import sqlite3

path_to_grocy_db = '/containers/grocy/data/grocy.db'
conn = sqlite3.connect(path_to_grocy_db)
c = conn.cursor()

c.execute("UPDATE products SET default_best_before_days = (SELECT ROUND(AVG(JulianDay(best_before_date) - JulianDay(purchased_date))) FROM stock_log WHERE best_before_date != '2999-12-31' And transaction_type = 'purchase' AND product_id = products.id GROUP by product_id) WHERE id IN (SELECT products.id FROM stock_log, products WHERE best_before_date != '2999-12-31' And transaction_type = 'purchase' AND product_id = products.id)")
conn.commit()

c.execute("SELECT id, barcode FROM products where length(barcode) != 0")
rows = c.fetchall()

total_updated = 0
for id, barcodes in rows:
	temp_barcodes = barcodes + ","
	for barcode in barcodes.split(","):
		if len(barcode) == 12 and "0" + barcode not in barcodes:
			temp_barcodes = temp_barcodes + "0" + barcode + ","
		elif len(barcode) == 13 and barcode[0] == '0' and (','+ barcode[-12:] not in barcodes or barcode[-12:]+',' not in barcodes):
			temp_barcodes = temp_barcodes + barcode[-12:] + ","
			
	if temp_barcodes[:-1] != barcodes:
		total_updated+=1
		c.execute("UPDATE products  SET barcode = '%s' where id = '%s'" % (temp_barcodes[:-1], id))
if total_updated > 0:  conn.commit()

print("%s Products Updated" % total_updated)
