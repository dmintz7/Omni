import sqlite3

path_to_grocy_db = '/containers/grocy/data/grocy.db'
conn = sqlite3.connect(path_to_grocy_db)
c = conn.cursor()
c.execute("SELECT id, barcode FROM products where length(barcode) != 0")
rows = c.fetchall()

total_updated = 0
for id, barcodes in rows:
	temp_barcodes = barcodes + ","
	for barcode in barcodes.split(","):
		if barcode not in barcodes:
			if len(barcode) == 12 and "0" + barcode not in barcodes and:
				temp_barcodes = temp_barcodes + "0" + barcode + ","
			elif len(barcode) == 13 and barcode[:12] not in barcodes and barcode[0] == 0:
				temp_barcodes = temp_barcodes + barcode[:12] + ","
				
	if temp_barcodes[:-1] != barcodes:
		total_updated+=1
		c.execute("UPDATE products  SET barcode = '%s' where id = '%s'" % (temp_barcodes[:-1], id))
if total_updated > 0:  conn.commit()

print("%s Products Updated" % total_updated)
