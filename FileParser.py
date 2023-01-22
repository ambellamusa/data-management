import os
import csv
import json
import time
import datetime
import striprtf
from DatabaseWrapper import MySqlDatabase
from Configurations import Data
import xml.etree.ElementTree as ET
from Configurations import Configurations

mySql_db = MySqlDatabase()


def get_all_files():
    files_fun = {}
    # r = root, d = directory, f = files
    for r, d, f in os.walk(Configurations.folder_path):
        for file_itr in f:
            if any(file_type_itr in file_itr for file_type_itr in Configurations.allowed_files):
                files_fun[file_itr] = os.path.join(r, file_itr)
    return files_fun


def csv_file_parser(file_name_arg, file_path_arg):
    data_fun = Data()
    data_fun.UploadDate = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path_arg))
                                                     , "%a %b %d %H:%M:%S %Y")
    data_fun.ClientID = str(file_name_arg).split("_")[-1:][0].replace(".csv", "")
    data_fun.FileName = file_name_arg
    try:
        with open(file_path_arg, "r") as csv_file:
            csv_file_reader = csv.DictReader(csv_file)
            data_fun.Columns = ", ".join(csv_file_reader.fieldnames)
            first = True
            for line in csv_file_reader:
                if first:
                    first = False
                    data_fun.Min = float(line[Configurations.low_column])
                    data_fun.Max = float(line[Configurations.high_column])

                if data_fun.Max < float(line[Configurations.high_column]):
                    data_fun.Max = float(line[Configurations.high_column])
                if data_fun.Min > float(line[Configurations.low_column]):
                    data_fun.Min = float(line[Configurations.low_column])
    except:
        print(file_name_arg, ": High and low can't be extracted.")
    return data_fun


def rtf_txt_file_parser(file_name_arg, file_path_arg):
    data_fun = Data()
    data_fun.UploadDate = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path_arg))
                                                     , "%a %b %d %H:%M:%S %Y")
    data_fun.ClientID = str(file_name_arg).split("_")[-1:][0].replace(".csv", "")
    data_fun.FileName = file_name_arg
    try:
        with open(file_path_arg, "r", encoding="utf-8") as read_file:
            lines = read_file.read()
            if file_name_arg.__contains__(".rtf"):
                lines = striprtf.striprtf(lines)
                lines = lines.replace("\n", "")
                lines = lines[:-3]
                lines += "}}"
            file_dict_fun = json.loads(lines)
            data_fun.Columns = ", ".join(file_dict_fun["dataset"]["column_names"])
            high_index = file_dict_fun["dataset"]["column_names"].index(Configurations.high_column)
            low_index = file_dict_fun["dataset"]["column_names"].index(Configurations.low_column)

            rows = file_dict_fun["dataset"]["data"]
            first = True
            for line in rows:
                if first:
                    first = False
                    data_fun.Min = float(line[low_index])
                    data_fun.Max = float(line[high_index])

                if data_fun.Max < float(line[high_index]):
                    data_fun.Max = float(line[high_index])
                if data_fun.Min > float(line[low_index]):
                    data_fun.Min = float(line[low_index])
    except:
        print(file_name_arg, ": High and low can't be extracted.")
    return data_fun


def xml_file_parser(file_name_arg, file_path_arg):
    data_fun = Data()
    data_fun.UploadDate = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path_arg))
                                                     , "%a %b %d %H:%M:%S %Y")
    data_fun.ClientID = str(file_name_arg).split("_")[-1:][0].replace(".csv", "")
    data_fun.FileName = file_name_arg
    DOMTree = ET.parse(file_path_arg)
    root = DOMTree.getroot()

    initial = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"
    initial_1 = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
    initial_2 = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"
    first_row = True
    first_record = True
    columns = []
    try:

        for child in root.findall('%sbody/%sspreadsheet/%stable/%stable-row' %
                                  (initial, initial, initial_1, initial_1)):
            index = 0
            for sub_child in child.findall('%stable-cell/%sp' % (initial_1, initial_2)):

                if first_row:
                    columns.append(sub_child.text)
                elif first_record:
                    high_index = columns.index(Configurations.high_column)
                    low_index = columns.index(Configurations.low_column)
                    if index == high_index:
                        data_fun.Max = float(sub_child.text.strip())
                    if index == low_index:
                        data_fun.Min = float(sub_child.text.strip())
                else:
                    if index == high_index:
                        if data_fun.Max < float(sub_child.text.strip()):
                            data_fun.Max = float(sub_child.text.strip())
                    if index == low_index:
                        if data_fun.Min > float(sub_child.text.strip()):
                            data_fun.Min = float(sub_child.text.strip())
                index += 1

            if not first_row:
                first_record = False
            first_row = False
    except:
        print(file_name_arg, ": High and low can't be extracted.")
    data_fun.Columns = ", ".join(columns)
    return data_fun


if __name__ == "__main__":
    all_files = get_all_files()
    for key in all_files:
        if key.__contains__(".csv"):
            if not mySql_db.find_record(key):
                file_record = csv_file_parser(key, all_files[key])
                mySql_db.save_to_database(file_record)
            else:
                print(key, ": already saved!")

        if any(ext in key for ext in [".txt", ".rtf"]):
            if not mySql_db.find_record(key):
                file_record = rtf_txt_file_parser(key, all_files[key])
                mySql_db.save_to_database(file_record)
            else:
                print(key, ": already saved!")

        if key.__contains__(".xml"):
            if not mySql_db.find_record(key):
                file_record = xml_file_parser(key, all_files[key])
                mySql_db.save_to_database(file_record)
            else:
                print(key, ": already saved!")
    mySql_db.close_connection()

