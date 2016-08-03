"""
 Modified Version of https://github.com/surya-shodan/mongoexportcsv allows exports irrespective of structural differences within documents of a collection
 Added:
 - xls export
 - added utf-8 encoding support, improved formatting
 - allow specifing queries to export subsets of collection
"""


import sys
from pymongo import MongoClient
import csv
import pprint
import json

from openpyxl import Workbook

class generic_converter:

    def __init__(self):
        self.header_dict = {}

    def retrieve_headers(self, test_dict, name_var):
        for element in test_dict.keys():
            if isinstance(test_dict[element], dict):
                self.retrieve_headers(test_dict[element], name_var +
                                      '||' + element)
            else:
                self.header_dict[name_var + '||' + element] = test_dict[element]

    def converter_main(self, csv_writer):
        client = MongoClient(sys.argv[1])
        db = client[sys.argv[2]]
        collection_obj = db[sys.argv[3]]
        cursor_records = collection_obj.find(json.loads(sys.argv[4]))  
        header_list = []

        for cursor in cursor_records:

            self.retrieve_headers(cursor, '')
            for item_label in self.header_dict:
                if item_label not in header_list:
                    header_list.append(item_label)
            self.header_dict = {}

        header_list.sort()
        out_header_list = [field.replace('||', '.') for field in header_list]
        out_header_list = [(field[1:] if field.startswith('.') else field) for field in out_header_list]

        csv_writer.writerow(out_header_list)

        cursor_records = collection_obj.find(json.loads(sys.argv[4]))  


        
        for cursor in cursor_records:
            row_to_push = []
            self.header_dict = {}
            self.retrieve_headers(cursor, '')
            for item_label in header_list:
                if item_label in self.header_dict:
                    row_to_push.append(self.header_dict[item_label])
                else:
                    row_to_push.append('')
            csv_writer.writerow(row_to_push)


def read_one_row(csvreader):
    for row in csvreader:
        return row
    return None

# Converts csv file saved by tool to xls file
def csv2xls(infile,xlspath):

    reader = csv.reader(infile, dialect='excel')
    wb = Workbook(encoding="utf-8")
    ws = wb.active

    for row in reader:
        if len(row) == 0:
            continue

        ws.append(row)

    wb.save(xlspath)

def print_usage():
    print ('python3 mongoexportcsv_custom.py "mongodb://username:password@domain:port "db-name" "table-name" "filter (e.g. {\\"_id\":\\"q05nK_EwqiQ\\"} )"')

def main():
    if len(sys.argv) != 5:
        print_usage()
        return
    collection_name = sys.argv[3]
    csvpath = collection_name + '.csv'
    xlspath = collection_name + '.xls'
    with open(csvpath, 'w', encoding='utf-8') as f_write:
        csv_writer = csv.writer(f_write, dialect='excel')
        converter_object = generic_converter()
        converter_object.converter_main(csv_writer)



    with open(csvpath, 'r', encoding='utf-8') as csvfile:
        # with open('out.xls', 'w', encoding='utf-8') as xlsfile:
        csv2xls(csvfile, xlspath=xlspath)




if __name__ == '__main__':
    main()
