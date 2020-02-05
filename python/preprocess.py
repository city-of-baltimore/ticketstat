import csv
import glob
import os
import requests

cached_geo = {}
GEO_LOOKUP = ("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
              "singleLine={}&"
              "f=json&"
              "outFields=Match_addr,Addr_type")

def get_geo(address):
	if not cached_geo.get(address):
		try:
			r = requests.get(GEO_LOOKUP.format(address))
			data = r.json()
		except:
			print("ERROR!")
			print(r)
			return ('', '')
		lat = data['candidates'][0]['location']['y']
		lng = data['candidates'][0]['location']['x']
		cached_geo[address] = (lat, lng)
		return lat, lng
	return cached_geo.get(address)


URL = ("https://maps.googleapis.com/maps/api/geocode/json")
OUTFILE = 'outfile.csv'
INFILE = 'infile.csv'

with open(OUTFILE, 'w') as out_file:
	for in_file in glob.glob(os.path.join(os.pardir, 'data', '*.csv')):
		print("Opening {}".format(in_file))
		with open(in_file, 'r') as csv_file:
			csv_reader = csv.reader(csv_file)
			first = True
			for in_row in csv_reader:
				if first:
					# Read the other header and then add the extra fields
					csv_header = in_row
					csv_header.remove('Civic #')
					csv_header.remove('Direction')
					csv_header.remove('Street')
					csv_header.remove('Infraction Date')
					csv_header.remove('Creation Time')
					csv_header.append('Infraction Datetime')
					csv_header.append('Street Address')
					csv_header.append('Latitude')
					csv_header.append('Longitude')
					writer = csv.DictWriter(out_file, fieldnames=csv_header)
					writer.writeheader()
					first = False
					continue

				street_addr = "{num} {direction}{street}".format(num=in_row[13] if in_row[13] != '0' else 1,
					                                             direction="{} ".format(in_row[14]) if in_row[14] else "",
					                                             street=in_row[15])
				address = "{},baltimore,md".format(street_addr)
				lat, lng = get_geo(address)

				in_row.append("{} {}".format(in_row[4], in_row[5])) # Combine date and time fields
				in_row.append(street_addr) # Combined Civic #, Direction, Street
				in_row.append(lat) # Latitude
				in_row.append(lng) # Longitude
				in_row.pop(15) # Street
				in_row.pop(14) # Direction
				in_row.pop(13) # Civic #
				in_row.pop(5) # Creation time
				in_row.pop(4) # Infraction date

				out_row = {}
				for i in range(len(csv_header)):
					out_row[csv_header[i]] = in_row[i]
				writer.writerow(out_row)
