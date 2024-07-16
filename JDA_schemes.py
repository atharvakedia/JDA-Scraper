
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 23 12:35:22 2024

@author: atharvakedia
"""

import requests
import sqlite3
from datetime import datetime
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def initialize_db():
    conn = sqlite3.connect('JDA_schemes.db')
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS login_hist (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      login DATETIME)''')
    
    cursor.execute('INSERT INTO login_hist(login) VALUES(?)', (datetime.now(),))
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS zones (
                      id INTEGER PRIMARY KEY,
                      name VARCHAR(50),
                      schemes_cnt INTEGER)''') 
    conn.commit()
    
    for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 20, 27, 28]:
        cursor.execute('INSERT OR IGNORE INTO zones(id) VALUES(?)', (i,))
    conn.commit()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS schemes (
                      sch_id INTEGER,
                      sector_id INTEGER,
                      name VARCHAR(100),
                      zone VARCHAR(50),
                      zone_id INTEGER,
                      developer VARCHAR(100),
                      dev_type VARCHAR(100),
                      status VARCHAR(50),
                      plots INTEGER,
                      last_updated DATE,
                      PRIMARY KEY(sch_id,sector_id)
                      FOREIGN KEY (zone_id) REFERENCES zones(id))''')
    conn.commit()
    
    conn.close()

def update_db():
    conn = sqlite3.connect('JDA_schemes.db')
    cursor = conn.cursor()
    
    for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18, 20, 27, 28]:
        zone_url = f'https://api.jaipurjda.org/APICPRMS/api/CPRMSAPI/GetCPRMSSchemeDetails?SchemeName=&DeveloperTypeId=&DeveloperId=&ZoneId={i}'
        try:
            response = requests.get(zone_url)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to retrieve the web page for zone ID: {i}. Error: {e}")
            continue
        
        all_schemes = response.json().get('Data', [])
        if not all_schemes:
            print(f"No schemes found for zone ID: {i}")
            continue
        
        new_count = len(all_schemes)
        
        zone_name = all_schemes[0]['ZName']
        cursor.execute('UPDATE zones SET name = ? WHERE id = ? AND name IS NULL', (zone_name, i))
        
        cursor.execute('SELECT sch_id,sector_id FROM schemes WHERE zone_id = ?', (i,))
        existing_schemes = cursor.fetchall()
        existing_schemes_ids = set()
        for scheme in existing_schemes:
            existing_schemes_ids.add(scheme)
        
        
        new_schemes_ids = set()
        for scheme in all_schemes:
            new_schemes_ids.add((int(scheme['SchId']), int(scheme['SectorId'])))

        
        to_add = new_schemes_ids - existing_schemes_ids
 #       to_delete = existing_schemes_ids - new_schemes_ids
        to_update = new_schemes_ids & existing_schemes_ids
        
        
        # Handle additions
        for scheme in all_schemes:
            if (int(scheme['SchId']), int(scheme['SectorId'])) in to_add:
                scheme_id = int(scheme['SchId'])
                sector_id = int(scheme['SectorId'])
                scheme_name = scheme['SchName']
                zone_name = scheme['ZName']
                zone_id = int(scheme['ZoneId'])
                developer = scheme['DeveloperName']
                dev_type = scheme['DevTypeDesc']
                status = scheme['SchemeStatus']
                plots = int(scheme['TotalPlot'])
                cursor.execute('INSERT INTO schemes (sch_id, sector_id, name, zone, zone_id, developer, dev_type, status, plots, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                   (scheme_id, sector_id, scheme_name, zone_name, zone_id, developer, dev_type, status, plots, datetime.now()))
        
        # Handle deletions
        #for sch_id in to_delete:
            #cursor.execute('UPDATE schemes SET status = ? WHERE sch_id = ?', ('DELETED', sch_id))
        
        # Handle updates
        updated = []
        for scheme in all_schemes:
            if (int(scheme['SchId']), int(scheme['SectorId'])) in to_update:
                cursor.execute('SELECT status FROM schemes WHERE sch_id = ? AND sector_id = ?', (int(scheme['SchId']), int(scheme['SectorId'])))
                old_status = cursor.fetchone()[0]
                new_status = scheme['SchemeStatus']
                if new_status != old_status:
                    cursor.execute('''UPDATE schemes SET status = ?, last_updated = ? WHERE sch_id = ? AND sector_id = ?''',
                                   (scheme['SchemeStatus'], datetime.now(), int(scheme['SchId']), int(scheme['SectorId'])))
                    updated.append((int(scheme['SchId']), int(scheme['SectorId'])))
        
        conn.commit()
        cnt_add = len(to_add)
        cnt_update = len(updated)
        cnt = len(to_add) + len(updated)
        if cnt != 0:
            #cursor.execute('SELECT login FROM login_hist ORDER BY id DESC LIMIT 2')
            #login = cursor.fetchall()
            #last_login = login[1][0]
            
            #cursor.execute('SELECT * FROM schemes WHERE last_updated > ?', (last_login,))
            #rows = cursor.fetchall()
            
            print(f"{cnt_add} new records were found in {zone_name}")
            print(f"{cnt_update} records were updated in {zone_name}")
            print("New records are listed below:")
            #for item in rows:
                #print(item)
            #to_add.update(updated)
            for item in to_add:
                cursor.execute('SELECT * FROM schemes WHERE sch_id = ? AND sector_id = ?', item)
                x = cursor.fetchone()
                print(x)
            print("New records are listed below:")
            for item in to_update:
                cursor.execute('SELECT * FROM schemes WHERE sch_id = ? AND sector_id = ?', item)
                x = cursor.fetchone()
                print(x)
            
            cursor.execute('UPDATE zones SET schemes_cnt = ? WHERE id = ?', (new_count, i))
        else:
            print(f"No new records or updates in {zone_name}")
        
        conn.commit()
    
    cursor.close()
    conn.close()
    
    menu()
    
def menu():
    print("Please choose from the following options:")
    print("""
          - Enter 1 to update database
          - Enter 2 + {zone id} to fetch schemes in a particular zone
          - Enter 3 to fetch count of schemes in all zones
          - Enter 4 to fetch data based on time
          - Enter 5 to output login data
          - Enter 6 to input a custom query
          - Enter 7 to end the program
          """)
    user_input = input(":")
    if user_input == '1':
        update_db()
    elif user_input == '6':
        run_query()
    elif user_input == '5':
        login_data()
    elif user_input == '7':
        return
    elif user_input == '21':
        print_zone(1)
    elif user_input == '22':
        print_zone(2)
    elif user_input == '23':
        print_zone(3)
    elif user_input == '24':
        print_zone(4)
    elif user_input == '25':
        print_zone(5)
    elif user_input == '26':
        print_zone(6)
    elif user_input == '27':
        print_zone(7)
    elif user_input == '28':
        print_zone(8)
    elif user_input == '29':
        print_zone(9)
    elif user_input == '210':
        print_zone(10)
    elif user_input == '211':
        print_zone(11)
    elif user_input == '212':
        print_zone(12)
    elif user_input == '213':
        print_zone(13)
    elif user_input == '214':
        print_zone(14)
    elif user_input == '218':
        print_zone(18)
    elif user_input == '220':
        print_zone(20)
    elif user_input == '227':
        print_zone(27)
    elif user_input == '3':
        print_zones()
    elif user_input == '4':
        timely()
    else:
        print("Invalid input please try again")
        menu()
        
def timely():
    conn = sqlite3.connect('JDA_schemes.db')
    time_str = input("Kindly input the time you would like to see new data from (in format YYYY-MM-DD HH:MM:SS): ")
    time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    query = 'SELECT * FROM schemes WHERE last_updated > ?'
    df = pd.read_sql_query(query, conn, params=(time,))
    print(df)
    conn.close()
    menu()
    
def print_zone(x):
    conn = sqlite3.connect('JDA_schemes.db')
    query = 'SELECT * from schemes WHERE zone_id = ' + str(x)
    df = pd.read_sql_query(query, conn)
    print(df)
    conn.close()
    menu()
    
def print_zones():
    conn = sqlite3.connect('JDA_schemes.db')
    query = 'SELECT * from zones'
    df = pd.read_sql_query(query, conn)
    print(df)
    conn.close()
    menu()

def login_data():
    conn = sqlite3.connect('JDA_schemes.db')
    query = 'SELECT * from login_hist'
    df = pd.read_sql_query(query, conn)
    print(df)
    conn.close()
    menu()
        
def run_query():
    conn = sqlite3.connect('JDA_schemes.db')
    cursor = conn.cursor()
    
    query = input("Kindly type in the query to display data:")
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()
    #df = pd.read_sql_query(query, conn)
    #print(df)
    
    conn.close()
    menu()
    

def start():
    password = "123"
    user_input = input("Kindly enter the password to proceed: ")
    
    if user_input == password:
        print("Correct password")
        initialize_db()
        menu()
    else:
        print("The password entered was incorrect, please try again")
        start()

print("Welcome to Kedia Group's JDA schemes Database")
start()