import sqlite3
import csv

class format_converter:
    def sql_to_csv(self,database,csv_file,table_name):
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM '+ table_name) 
        header = [row[0] for row in cursor.description]
        with open(csv_file,'w') as csv_file: 
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)
            csv_writer.writerows(cursor)
        connection.commit()    
        cursor.close()    
        connection.close()
        return csv_file

    def csv_to_sql(self,csv_file,database,table_name):
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS " + table_name)
        cursor.execute('CREATE TABLE ' + table_name + '(Volcano_Name,Country,Type,Latitude_dd,Longitude_dd,Elevation_m)')
        with open(csv_file,'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            list_to_insert = [(i['Volcano Name'], i['Country'], i['Type'], i['Latitude (dd)'], i['Longitude (dd)'], i['Elevation (m)']) for i in csv_reader]    
        cursor.executemany('INSERT INTO ' + table_name + '(Volcano_Name,Country,Type,Latitude_dd,Longitude_dd,Elevation_m) VALUES (?, ?, ?, ?, ?, ?);', list_to_insert)
        cursor.execute("UPDATE " + table_name + " SET Type=NULL WHERE Type IS ''")
        connection.commit()
        cursor.close()
        connection.close()

convert = format_converter()
convert.sql_to_csv('sourse_all_fault_line.db','all_fault_lines.csv','fault_lines')
convert.csv_to_sql('sourse_list_volcano.csv','list_volcanos.db','volcanos')