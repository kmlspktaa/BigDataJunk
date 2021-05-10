def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')
	
schema = avro.schema.parse(open("bq_table_avro.avsc","rb").read())
SOURCE_FILE_NAME = os.environ['BQ_SOURCE_FILE_NAME']
TARGET_FILE_NAME = os.environ['BQ_TARGET_FILE_NAME']

with codecs.open(SOURCE_FILE_NAME, 'r') as csvfile:
	reader = unicode_csv_reader(csvfile, delimiter=',')
	writer = DataFileWriter(open(TARGET_FILE_NAME, "wb"), DatumWriter(), schema, codec='deflate')
	for count, row in enumerate(reader):
		print (count)
		try:
			writer.append(
				{"ts": row[0],
    			"device": row[1],
    			"co": row[2],
    			"humidity": row[3],
    			"light": row[4],
    			"lpg": row[5],
    			"motion": row[6],
    			"smoke": row[7],
    			"temp": row[8]
				}
			)
		except IndexError:
			print ("Bad record, skip.")
	writer.close()