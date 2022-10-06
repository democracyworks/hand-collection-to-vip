#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 16:41:44 2022

@author: liamjones
"""

from sys import argv
import argparse
import os

# DATABASE libraries
import mysql.connector
from mysql.connector import errorcode

# DATA MUNGING & FORMATTING libraries
import pandas as pd
import json
import datetime
import re # regex
import numpy as np
import time
import hashlib
import pytz

STATES_USING_TOWNSHIPS = ["CT", "ME", "MA", "NH", "RI", "VT"] # SET states that use townships instead of counties for elections

OUTPUT_DIR = os.path.expanduser("~/Democracy Works VIP Dropbox/Democracy Works VIP's shared workspace/hand_collection/2022-11-08/valid_precincts/")

dbname = {"AL":"alabama", "AK":"alaska", "AZ":"arizona", "AR":"arkansas", "CA":"california", "CO":"colorado", "CT":"connecticut", "DE":"delaware", 
          "DC":"district_of_columbia", "FL":"florida", "GA":"georgia", "HI":"hawaii", "ID":"idaho", "IL":"illinois", "IN":"indiana", "IA":"iowa", 
          "KS":"kansas", "KY":"kentucky", "LA":"louisiana", "ME":"maine", "MD":"maryland", "MA":"massachusetts", "MI":"michigan", "MN":"minnesota", 
          "MS":"mississippi", "MO":"missouri", "MT":"montana", "NE":"nebraska", "NV":"nevada", "NH":"new_hampshire", "NJ":"new_jersey", "NM":"new_mexico", 
          "NY":"new_york", "NC":"north_carolina", "ND":"north_dakota", "OH":"ohio", "OK":"oklahoma", "OR":"oregon", "PA":"pennsylvania", "RI":"rhode_island", 
          "SC":"south_carolina", "SD":"south_dakota", "TN":"tennessee", "TX":"texas", "UT":"utah", "VT":"vermont", "VA":"virginia", "WA":"washington", 
          "WV":"west_virginia", "WI":"wisconsin", "WY":"wyoming", }

# SET UP command line inputs
parser = argparse.ArgumentParser()
parser.add_argument('-states', nargs='+')

try:
    # CONNECT to AWS RDS MySQL database
    with open('../credentials/targetsmart_credentials.json', 'r') as f: conn_string = json.load(f)
    conn = mysql.connector.connect(**conn_string)
except:
    print('ERROR | There is a database connection error.')
    
os.chdir(OUTPUT_DIR)

input_states = parser.parse_args()._get_kwargs()[0][1]
 
input_states = [state.upper() for state in input_states]

for state_abbrv in input_states:
    
    sql_table_name = "hand_collection." + dbname[state_abbrv]
    
    if state_abbrv in STATES_USING_TOWNSHIPS:
        loc_str = "vf_township"
    else:
        loc_str = "vf_county_name"

    # SET list of vars selected in MySQL query
    targetsmart_sql_col_string = ', '.join([loc_str+" AS vf_locality", 'vf_precinct_name'])

    # SET list of fields to filter out if they are empty in TargetSmart
    targetsmart_sql_filter_string = ''.join([" WHERE vf_reg_city <> ''",
                                             " AND vf_precinct_name <> ''",
                                             " AND vf_reg_address_1 <> ''",
                                             " AND vf_reg_zip <> ''",
                                             " AND vf_reg_cass_street_name <> 'PO BOX'"])
    
    # SET MySQL query
    query = "SELECT DISTINCT " + targetsmart_sql_col_string + " FROM " + sql_table_name + \
        targetsmart_sql_filter_string + ";"
    
    print("Processing",state_abbrv, ". . .", end = "\r")
    
    try:
        target_smart = pd.read_sql(query, conn)

        conn.rollback() # REFRESH database connection

    except:
        print('ERROR | TargetSmart data for', state_abbrv, 'is either missing from the database or there is data reading error.')
    
    else:
    
        print("Processing",state_abbrv, ". . .", "Done")
            
        target_smart.sort_values(["vf_locality", "vf_precinct_name"], inplace = True)
        
        target_smart.to_csv(state_abbrv+"_valid.txt", encoding = "utf-8", index = False)
            
conn.close()