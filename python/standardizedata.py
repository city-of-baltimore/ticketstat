""" This standardizes the pre-july-2019 data to the post-july-2019 data"""

"""

# This validates data
import csv

first = True
with open('outfile1.csv', 'r') as f:
	csv_reader = csv.reader(f)
	for in_row in csv_reader:
		if first:
			first = False
			continue
		try:
			assert(len(in_row) == 19)
			float(in_row[10])
			float(in_row[17])
			float(in_row[18])
			#assert(39.14 < float(in_row[17]) < 39.5)
			#assert(-76.85 < float(in_row[18]) < -76.45)
		except:
			print(len(in_row))
			print(in_row)
			float(in_row[10])
			float(in_row[17])
			float(in_row[18])
			#assert(39.14 < float(in_row[17]) < 39.5)
			#assert(-76.85 < float(in_row[18]) < -76.45)
		prev = in_row
"""
from datetime import datetime

OUTFILE = 'outfile.csv'
csv_header = ['Ticket #', 'Status', 'Plate', 'State', 'Officer Badge No', 'Officer Name', 'Squad', 'Post',
              'violation Code', 'Infraction Text', 'Fine', 'ClientId', 'Server', 'Software', 'Export Date',
              'Infraction Datetime', 'Street Address', 'Latitude', 'Longitude']


# This starndardizes dates
def get_date(datestr):
	d = 0
	for fmt in ('%Y-%m-%d 00:00:00 %H:%M', '%Y-%m-%d 00:00:00', '%m/%d/%Y', '%m/%d/%Y %I:%M %p'):
		try:
			d = datetime.strptime(datestr, fmt)
		except:
			pass

	assert d != 0

	return d.strftime('%m/%d/%Y %I:%M %p') #11/13/2019 8:26 AM

first = True
with open(OUTFILE, 'w') as out_file:
	with open('outfile1.csv', 'r') as csv_file:
		csv_reader = csv.reader(csv_file)
		for in_row in csv_reader:
			if first:
				csv_header = in_row
				writer = csv.DictWriter(out_file, fieldnames=csv_header)
				writer.writeheader()
				first = False
				continue
			# Export Date
			if in_row[14]:
				in_row[14] = get_date(in_row[14])

			# Infraction Datetime
			if in_row[15]:
				in_row[15] = get_date(in_row[15])
			else:
				print("Error in {}".format(in_row))

			out_row = {}
			for i in range(len(csv_header)):
				out_row[csv_header[i]] = in_row[i]
			writer.writerow(out_row)
