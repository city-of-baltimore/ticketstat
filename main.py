import argparse
import datetime

from parkingcitations.citationdata import CitationData
from parkingcitations.creds import GTECHNA_USERNAME, GTECHNA_PASSWORD

yesterday = datetime.date.today() - datetime.timedelta(days=1)
parser = argparse.ArgumentParser(description='Pulls parking citation data from GTechna, geocodes, and puts it in '
                                             'our internal database')
parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                    help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to all '
                          'days if not specified'))
parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                    help=('Optional: Day of date we should start searching on (IE: 5). Defaults to all days if '
                          'not specified'))
parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                    help=('Optional: Year of date we should start searching on (IE: 2020). Defaults to all days '
                          'if not specified'))
parser.add_argument('-n', '--numofdays', default=1, type=int,
                    help='Optional: Number of days to search, including the start date.')
parser.add_argument('-c', '--create_table', action='store_true',
                    help='Creates the database table. Only needed on first run')

args = parser.parse_args()

citations = CitationData(GTECHNA_USERNAME, GTECHNA_PASSWORD)
with citations:
    for i in range(args.numofdays):
        insert_date = datetime.date(args.year, args.month, args.day) + datetime.timedelta(days=i)
        print("Processing {}".format(insert_date))
        citations.insert_data(insert_date, args.create_table)
