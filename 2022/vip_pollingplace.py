from __future__ import print_function
from sys import argv
import argparse
import os

# GOOGLE API libraries
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http
from oauth2client import file, client, tools

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

# OTHER libraries
from zipfile import ZipFile
import warnings

from upload_script import upload

file_dict = {}

# _________________________________________________________________________________________________________________________

# GLOBAL VARIABLES | Set global variables and print display formats

warnings.filterwarnings('ignore') # FILTER out warnings that are not critical

pd.set_option('display.max_columns', 100)  # or 1000 or None
pd.set_option('display.max_rows', 1000)  # or 1000 or None
PRINT_OUTPUT_WIDTH = 100 # SET print output length
PRINT_CENTER = int(PRINT_OUTPUT_WIDTH/2)
np.set_printoptions(linewidth=PRINT_OUTPUT_WIDTH)
PRINT_TUPLE_WIDTH = 4
PRINT_ARRAY_WIDTH = 11 

ELECTION_YEAR = 2022

STATES_USING_TOWNSHIPS = ["CT", "ME", "MA", "NH", "RI", "VT"] # SET states that use townships instead of counties for elections

STATES_WITH_WARNINGS = [] # STORE states that trigger warnings

OUTPUT_DIR = os.path.expanduser("~/Democracy Works VIP Dropbox/Democracy Works VIP's shared workspace/hand_collection/")

fips_lookup = pd.read_json("fips_ocdid_zips_dictionary.txt").T["ocdid"]
fips_lookup.dropna(inplace = True)

fips_lookup_r = pd.Series(fips_lookup.index.values, name = "FIPS", index = fips_lookup)
# _________________________________________________________________________________________________________________________

# GOOGLE API | Set scopes & Google Spreadsheet IDs (1 scope, 2 IDs)

# PRO-TIP: if modifying these scopes, delete the file token.json.
# NOTE: Scope url should not change year to year unless Google alters syntax
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# individual states & STATE_FEED tabs in a single Google Sheet (multiple tabs)
# https://docs.google.com/spreadsheets/d/1evSv43fKQ8Qf529IHNIAAS_FoN0okQjPlk__qUNMQrs/edit#gid=2009403147
SPREADSHEET_ID = '1evSv43fKQ8Qf529IHNIAAS_FoN0okQjPlk__qUNMQrs' 

# ELECTION_AUTHORITIES entire Google Sheet (1 tab) 
# https://docs.google.com/spreadsheets/d/12jlzfFM5Fr7LmLaH_1hJ2MLO2-YCcI5eKkRK1b8ayZA/edit#gid=1572182198
SPREADSHEET_EA_ID = '12jlzfFM5Fr7LmLaH_1hJ2MLO2-YCcI5eKkRK1b8ayZA' 


# _________________________________________________________________________________________________________________________



def vip_build(state_abbrv, state_feed, state_data, election_authorities, target_smart):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 11 .txt files
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt, precinct.txt, street_segment.txt)
    INPUT: state_abbrv, state_data, state_feed, election_authorities, target_smart
    RETURN: None
    """
    
    # PREP | Identify data issues, create/format features, and standardize dataframes

    """
    # GENERATE warnings in target_smart (3 types of warnings)
    missing_counties_count, missing_townships_count, \
      missing_state_count = generate_warnings_target_smart(state_abbrv, target_smart)
    """
    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # ENSURE leading zeros
    state_feed['state_id'] = state_abbrv.lower() + state_feed['state_fips']

    # CLEAN/FORMAT state_feed, state_data, election_authorities, and target_smart (4 dataframes)
    state_feed, state_data, election_authorities, \
       target_smart = clean_data(state_abbrv, state_feed, state_data, election_authorities, target_smart)
       
    # GENERATE warnings/fatal errors in state_data (2 warnings, 5 fatal errors)
    try:
        warnings = generate_warnings_state_data(state_data, state_abbrv, state_feed['official_name'][0], target_smart)
    except Exception as e:
        raise Exception("Warnings, "+str(e)) from e
    """
    # GENERATE warnings for matches between counties/places/townships & precincts in state_data & target_smart (2 warnings)
    sd_unmatched_precincts_list, ts_unmatched_precincts_list = warning_unmatched_precincts(state_abbrv, state_data, target_smart)
    """
    # _____________________________________________________________________________________________________________________

    # CREATE IDS | Create IDs on dataframes

    # CREATE 'election_adminstration_id'
    temp = election_authorities[['ocd_division']]
    temp.drop_duplicates(['ocd_division'], inplace=True)
    temp['election_administration_id'] = "ea_"+temp["ocd_division"]
    election_authorities = pd.merge(election_authorities, temp, on =['ocd_division'])
    election_authorities.drop_duplicates(subset=['election_administration_id'], inplace=True) #REMOVE all except first election administration entry for each ocd-id

    # CREATE 'election_official_person_id'
    temp = election_authorities[['ocd_division', 'official_title']]
    temp.drop_duplicates(['ocd_division', 'official_title'], keep='first',inplace=True)
    temp['election_official_person_id'] = "per_"+temp["ocd_division"]
    election_authorities = pd.merge(election_authorities, temp, on =['ocd_division', 'official_title']) 

    # CREATE stable IDs for polling locations
    stable_cols = ['location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_state', 'address_zip']
    
    state_data = stable_id(state_data, ID = 'polling_location_ids', stable_prfx = "pl", stable_cols = stable_cols)
    state_data = stable_id(state_data, ID = 'hours_open_id', stable_prfx = "ho", stable_cols = stable_cols)
    
    # _____________________________________________________________________________________________________________________

    # FILE CREATION | Generate files for dashboard zip

    # GENERATE 11 .txt files
    try:
        election = generate_election(state_feed)
    except Exception as e:
        raise Exception("Election, "+str(e)) from e
    try:
        polling_location = generate_polling_location(state_data)
    except Exception as e:
        raise Exception("Polling locations, "+str(e)) from e
    try:
        schedule = generate_schedule(state_data, state_feed)
    except Exception as e:
        raise Exception("Schedule, "+str(e)) from e
    try:
        source = generate_source(state_feed)
    except Exception as e:
        raise Exception("Source, "+str(e)) from e
    try:
        state = generate_state(state_feed)
    except Exception as e:
        raise Exception("State, "+str(e)) from e
    try:
        locality = generate_locality(state_feed, state_data, election_authorities, target_smart)
    except Exception as e:
        raise Exception("Localities, "+str(e)) from e
    try:
        election_administration = generate_election_administration(election_authorities)
    except Exception as e:
        raise Exception("Administration, "+str(e)) from e
    try:
        department = generate_department(election_authorities)
    except Exception as e:
        raise Exception("Departments, "+str(e)) from e
    try:
        person = generate_person(election_authorities)
    except Exception as e:
        raise Exception("Persons, "+str(e)) from e
    try:
        precinct = generate_precinct(state_data, target_smart)
    except Exception as e:
        raise Exception("Precinct, "+str(e)) from e
    try:
        street_segment = generate_street_segment(state_abbrv, target_smart, state_data, precinct)
    except Exception as e:
        raise Exception("Street Segments, "+str(e)) from e
    '''
    # GENERATE .txt file warnings (3 warnings)
    missing_precinct_ids_count, missing_house_numbers_count, \
      missing_street_suffixes_count = generate_warnings_txt(state_abbrv, street_segment)
    '''
    # GENERATE zip file
    generate_zip(state_abbrv, state_feed, {'election': election,
                                           'polling_location': polling_location,
                                           'schedule': schedule,
                                           'source':source,
                                           'state':state,
                                           'locality':locality,
                                           'election_administration':election_administration,
                                           'department': department, 
                                           'person': person,
                                           'precinct': precinct,
                                           'street_segment': street_segment})
    # _____________________________________________________________________________________________________________________

    # REPORT | Print zip file sizes, dataframe descriptions, and data warnings

    # PRINT state report
    state_report(warnings, # WARNINGS for .txt dataframes
                 state_abbrv, state_feed, state_data, election_authorities, target_smart, # MAIN dataframes
                 {'state':state,                                                          
                  'source':source,
                  'election': election,
                  'election_administration':election_administration,
                  'department': department,
                  'person': person,
                  'locality':locality,
                  'polling_location': polling_location,
                  'precinct': precinct,
                  'schedule': schedule,
                  'street_segment': street_segment})

    return
            
    

def clean_data(state_abbrv, state_feed, state_data, election_authorities, target_smart):
    """
    PURPOSE: cleans & formats state_feed, state_data, election_authorities, sand target_smart to output standards
    INPUT: state_feed, state_data, election_authorities, target_smart
    RETURN: state_feed, state_data dataframe, election_authorities, target_smart dataframes
    """

    # CREATE/FORMAT | Adjust variables to desired standards shared across relevant .txt files

    # REPLACE empty strings with NaNs
    state_data = state_data.replace('^\\s*$', np.nan, regex=True)
    
    # TRIM whitespace
    state_data = state_data.apply(lambda x: x.str.strip())
    
    # FORMAT FIPS & ZIPS (2 formatted)
    state_feed['fips'] = state_feed['state_fips'].str.pad(5, side='right', fillchar='0')
    target_smart['vf_reg_cass_zip'] = target_smart['vf_reg_cass_zip'].astype(str).str.pad(5, side='left', fillchar='0')

    # CREATE/FORMAT county (1 created, 2 formatted)
    state_data['locality'] = state_data['locality'].str.upper().str.strip()
    target_smart['vf_locality'] = target_smart['vf_locality'].str.upper().str.strip()
    election_authorities['ocd_division'] = election_authorities['ocd_division'].str.strip()
    
    # FORMAT precinct (2 formatted)
    state_data['precinct'] = state_data['precinct'].str.strip().str.upper()
    target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].astype(str).str.strip().str.upper()
    

    # _____________________________________________________________________________________________________________________

    # ERROR HANDLING | Interventionist adjustments to account for state eccentricities and common hand collection mistakes, etc

    '''
    # CONDITIONAL formattating for 2 states (NH, AZ) 
    if state_abbrv == 'NH':

        # REMOVE dash and quotes in precinct names
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace(' - ', ' ') \
                                                                           .str.replace('"', '') \
                                                                           .str.replace("'", '')

        state_data['county'] = state_data['county'].str.replace("'", '') # REMOVE apostrophes in state_data `county` because target_smart does not incl. them 
        state_data['precinct'] = state_data['precinct'].str.replace("'", '')
        

    elif state_abbrv == 'AZ':

        # NOTE: TargetSmart lists some of the precincts in Arizona as 'PIMA' + #, but not all
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('PIMA ', '')
    '''
    # OPTIMIZE data types 
    # state_data, election_authorities, target_smart = optimize_data_types(state_abbrv, state_data, election_authorities, target_smart)


    return state_feed, state_data, election_authorities, target_smart


def optimize_data_types(state_abbrv, state_data, election_authorities, target_smart):
    """
    PURPOSE: Optimizes data types 
    INPUT: state_abbrv, state_data, election_authorities, target_smart dataframes
    RETURN: state_abbrv, state_data, election_authorities, target_smart dataframes
    """

    # OPTIMIZE state_data data types
    state_data['location_name'] = state_data['location_name'].astype('category')
    state_data['directions'] = state_data['directions'].astype('category')
    state_data['start_time'] = state_data['start_time'].astype('category')
    state_data['end_time'] = state_data['end_time'].astype('category')

    # OPTIMIZE target_smart data types
    target_smart['vf_reg_cass_city'] = target_smart['vf_reg_cass_city'].astype('category')
    target_smart['vf_reg_cass_state'] = target_smart['vf_reg_cass_state'].astype('category')
    target_smart['vf_reg_cass_zip'] = target_smart['vf_reg_cass_zip'].astype('category')


    # OPTIMIZE election_authorities data types
    election_authorities['state'] = election_authorities['state'].astype('category')



    return state_data, election_authorities, target_smart


def generate_election(state_feed):
    """
    PURPOSE: generates election dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: election dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/election.html
    """
 
    # SELECT feature(s) (3 selected)
    election = state_feed[['election_date','election_name','state_id']]
    
    # CREATE/FORMAT feature(s) (3 created, 2 formatted)
    election['id'] = 'ele01' 
    election['election_type'] = 'General'
    election['is_statewide'] = 'true'
    election.rename(columns={'election_date':'date', 
                             'election_name':'name'}, inplace=True) # RENAME col(s)
    

    return election



def generate_polling_location(state_data):
    """
    PURPOSE: generates polling_location dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: polling_location dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/polling_location.html
    """ 

    # SELECT feature(s) (10 selected)
    polling_location = state_data[['polling_location_ids','location_name', 
                                   'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_state', 'address_zip',
                                   'directions', 'hours_open_id']] 
    
    # FORMAT col(s) (8 formatted)                              
    polling_location.rename(columns={'polling_location_ids':'id', 
                                     'location_name':'name',
                                     'address_line1':'structured_line_1',
                                     'address_line2':'structured_line_2',
                                     'address_line3':'structured_line_3',
                                     'address_city':'structured_city',
                                     'address_state':'structured_state',
                                     'address_zip':'structured_zip'}, inplace=True)
    
    polling_location.drop_duplicates("id",inplace=True)
    
    # ADD features (2 selected)
    polling_location["is_drop_box"] = "false"
    polling_location["is_early_voting"] = "false"
    
    return polling_location



def generate_schedule(state_data, state_feed):
    """
    PURPOSE: generates schedule dataframe for .txt file
    INPUT: state_feed
    RETURN: schedule dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/hours_open.html#schedule
    """ 

    # SELECT feature(s) (3 selected)
    schedule = state_data[['time_zone', 'hours_open_id', 'start_time', 'end_time']]
    schedule.drop_duplicates(inplace=True, ignore_index = True)
    
    # Add start and end times (election day)
    eday = state_feed.loc[0, "election_date"]
    schedule["start_date"] = eday
    schedule["end_date"] = eday
    
    # Add UTC offsets
    for i, row in schedule.iterrows():
        
        tz = pytz.timezone(row["time_zone"])
        dt_start = tz.localize(datetime.datetime.strptime(row["start_date"]+row["start_time"], "%Y-%m-%d%H:%M:%S"))
        dt_end = tz.localize(datetime.datetime.strptime(row["end_date"]+row["end_time"], "%Y-%m-%d%H:%M:%S"))
        
        offset_start = dt_start.strftime("%z")
        offset_end = dt_end.strftime("%z")
        
        offset_start = offset_start[:3]+":"+offset_start[3:]
        offset_end = offset_end[:3]+":"+offset_end[3:]
        
        schedule.iloc[i]["start_time"] = row["start_time"]+offset_start
        schedule.iloc[i]["end_time"] = row["end_time"]+offset_end
    
    schedule.drop(columns = "time_zone", inplace = True)
    
    fill = int(np.ceil(np.log10(len(schedule))))
    
    # CREATE feature(s) (1 created)
    schedule.reset_index(drop=True, inplace=True) # RESET index
    schedule['id'] = 'sch' + (schedule.index + 1).astype(str).str.zfill(fill) # CREATE feature(s)


    return schedule



def generate_source(state_feed):
    """
    PURPOSE: generates source dataframe for .txt file
    INPUT: state_feed
    RETURN: source dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/source.html
    """ 
    
    # SELECT feature(s) (1 selected)
    source = state_feed[['fips']]

    # CREATE/FORMAT feature(s) (4 created, 1 formatted)
    source.reset_index(drop=True, inplace=True)
    source['id'] = 'src' + (source.index + 1).astype(str).str.zfill(4)
    source['date_time'] = datetime.datetime.now().replace(microsecond=0).isoformat() 
    source['name'] = 'Democracy Works Outreach Team'
    source['version'] = '5.2' # REFERENCES VIP SPEC
    source.rename(columns={'fips':'vip_id'}, inplace=True) # RENAME col(s)
    

    return source



def generate_state(state_feed): 
    """
    PURPOSE: generates state dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: state dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/state.html
    """ 

    # SELECT feaure(s) (2 selected)
    state = state_feed[['state_id','official_name']]

    # FORMAT feature(s) (2 formatted)
    state.rename(columns={'state_id':'id', 
                          'official_name':'name'}, inplace=True)


    return state




def generate_locality(state_feed, state_data, election_authorities, target_smart):
    """
    PURPOSE: generates locality dataframe for .txt file
    INPUT: state_feed, state_data, election_authorities
    RETURN: locality dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/locality.html
    """ 

    # SELECT feature(s) (1 selected)
    locality = state_data[['OCD_ID', 'locality']]
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows from merge 

    # MERGE locality with election_authorities
    election_admins = election_authorities[['ocd_division', 'election_administration_id']]
    locality = locality.merge(election_admins, left_on='OCD_ID', right_on='ocd_division', how='left')
    
    if state_feed['state_abbrv'][0] in STATES_USING_TOWNSHIPS:
        loctype = "town"
    else:
        loctype = "county"
    
    # CREATE/FORMAT feature(s) (3 created)
    locality['state_id'] = state_feed['state_id'][0]
    locality['type'] = loctype
    locality['id'] = "loc_"+locality["OCD_ID"]
    
    pl = state_data[['locality', 'polling_location_ids']][state_data["precinct"].isna()]
    grouped = pl.groupby(['locality'], dropna = False)
    grouped = pd.DataFrame(grouped.aggregate(lambda x: ' '.join(x))['polling_location_ids']) # FLATTEN aggregated df
    grouped.reset_index(inplace = True)
    
    locality = locality.merge(grouped, on = "locality", how = "left")
    
    locality.rename(columns={'locality':'name'}, inplace=True)
    locality.drop(columns=["OCD_ID", "ocd_division"], inplace=True)
    
    locality.reset_index(drop=True, inplace=True)

    return locality

def generate_election_administration(election_authorities):
    """
    PURPOSE: generates election_administration dataframe for .txt file
    INPUT: election_authorities
    RETURN: election_administration dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/election_administration.html
    """

    # SELECT feature(s) (2 selected)
    election_administration = election_authorities[['election_administration_id', 'homepage_url']]

    # FORMAT feature(s) (2 formatted)
    election_administration.drop_duplicates(inplace=True)
    election_administration.rename(columns={'election_administration_id':'id',
                                            'homepage_url':'elections_uri'}, inplace=True)


    return election_administration



def generate_department(election_authorities):
    """
    PURPOSE: generates department dataframe for .txt file
    INPUT: election_authorities
    RETURN: department dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/department.html
    """

    # SELECT feature(s) (2 selected)
    department = election_authorities[['election_administration_id', 'election_official_person_id']]

    # CREATE feature(s) (1 created)
    department.drop_duplicates(inplace=True)
    department.reset_index(drop=True, inplace=True)
    department['id'] = 'dep' + (department.index + 1).astype(str).str.zfill(4)

    return department



def generate_person(election_authorities):
    """
    PURPOSE: generates person dataframe for .txt file
    INPUT: election_authorities
    RETURN: person dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/person.html
    """

    # SELECT feature(s) (3 selected)
    person = election_authorities[['ocd_division', 'official_title', 'election_official_person_id']]

    # CREATE/FORMAT feature(s) (2 created, 1 formatted)
    person.drop_duplicates('election_official_person_id', keep='first',inplace=True)
    person['profession'] = 'ELECTION ADMINISTRATOR'
    person['title'] = person['ocd_division'].str.extract('([^\\:]*)$', expand=False).str.upper() + ' ' + person['official_title'].str.upper()
    person.rename(columns={'election_official_person_id':'id'}, inplace=True)

    # REMOVE feature(s) (2 removed)
    person.drop(['ocd_division', 'official_title'], axis=1, inplace=True)

    return person



def generate_precinct(state_data, target_smart):
    """
    PURPOSE: generates precinct dataframe for .txt file
    INPUT: state_data, locality
    RETURN: precinct dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/precinct.html
    """
    
    precinct = target_smart[["vf_locality", "vf_precinct_name"]].rename(columns={"vf_locality":"locality",
                                                                                 "vf_precinct_name":"precinct"})
    precinct.sort_values(["locality", "precinct"], inplace = True, ignore_index = True)
    # SELECT feature(s) (3 selected)
    loc = state_data[['locality', 'OCD_ID']].drop_duplicates(ignore_index = True)

    precinct = precinct.merge(loc, on='locality', how='left')

    # GROUP polling_location_ids
    pl = state_data[['locality', 'precinct', 'polling_location_ids']]
    grouped = pl.groupby(['locality', 'precinct'], dropna = False)
    grouped = pd.DataFrame(grouped.aggregate(lambda x: ' '.join(x))['polling_location_ids']) # FLATTEN aggregated df
    grouped.reset_index(inplace = True)
    
    precinct = precinct.merge(grouped, on = ["locality", "precinct"], how = "left")
    
    precinct.fillna("", inplace = True)
    
    precinct["locality_id"] = "loc_" + precinct["OCD_ID"]
    precinct = stable_id(precinct, stable_prfx = "pre", stable_cols = ["locality", "precinct"])
    
    precinct.drop(columns = ["locality", "OCD_ID"], inplace = True)
    
    # CREATE/FORMAT feature(s) (1 created, 2 formatted)
    precinct.drop_duplicates("id", inplace=True)
    precinct.rename(columns={'precinct':'name'}, inplace=True)
    precinct.reset_index(drop=True, inplace=True)
    
    precinct = precinct[["id", "locality_id", "name", "polling_location_ids"]]

    return precinct

def generate_street_segment(state_abbrv, target_smart, state_data, precinct):
    """
    PURPOSE: generates street_segment dataframe for .txt file
    INPUT: state_data, target_smart, precinct
    RETURN: street_segment dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/street_segment.html
    """    
    
    street_segment = pd.DataFrame()
    
    # SELECT feature(s)
    street_segment["address_direction"] = target_smart["vf_reg_cass_post_directional"]
    street_segment["city"] = target_smart["vf_reg_cass_city"]
    street_segment["includes_all_addresses"] = "false"
    street_segment["includes_all_streets"] = "false"
    street_segment["odd_even_both"] = "both"
    street_segment["precinct_id"] = stable_id(target_smart[["vf_locality", "vf_precinct_name"]],
                                              stable_prfx = "pre",
                                              stable_cols = ["vf_locality", "vf_precinct_name"])["id"]
    streetnum = target_smart["vf_reg_cass_street_num"].str.strip().str.extract(r"^(?P<pre>\D*)\W*(?P<num>\d+)\W*(?P<suf>.*?)$", expand = True)
    street_segment["start_house_number"] = streetnum["num"]
    street_segment["end_house_number"] = streetnum["num"]
    street_segment["house_number_prefix"] = streetnum["pre"]
    street_segment["house_number_suffix"] = streetnum["suf"]
    street_segment["state"] = state_abbrv
    street_segment["street_direction"] = target_smart["vf_reg_cass_pre_directional"]
    street_segment["street_name"] = target_smart["vf_reg_cass_street_name"]
    street_segment["street_suffix"] = target_smart["vf_reg_cass_street_suffix"]
    street_segment["unit_number"] = target_smart["vf_reg_cass_apt_num"]
    street_segment["zip"] = target_smart["vf_reg_cass_zip"]
    
    street_segment.fillna("", inplace = True)
    
    street_segment.drop_duplicates(inplace=True)
    street_segment.reset_index(drop=True, inplace=True) # RESET index
    
    fill = int(np.ceil(np.log10(len(street_segment))))
    
    street_segment['id'] = 'ss_' + (street_segment.index + 1).astype(str).str.zfill(fill) 

    return street_segment

def generate_zip(state_abbrv, state_feed, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt, precinct.txt, street_segment.txt)
    INPUT: state_abbrv, state_feed, files
    RETURN: exports zip of 11 .txt files
    """

    reset_path = os.getcwd()
    
    election = state_feed['election_date'][0]

    save_path = os.path.join(OUTPUT_DIR,election,state_abbrv,"polling_places")

    # CREATE state directory
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        
    os.chdir(save_path)
    
    # WRITE dataframes to txt files
    file_list = []
    for name, df in files.items():

        file = name + '.txt'
        file_list.append(file)
        df.to_csv(file, index=False, encoding='utf-8')
    
    # DEFINE name of zipfile
    zip_filename = 'vipfeed-pp-' + election + '-' + state_abbrv + '.zip'

    # WRITE files to a zipfile
    with ZipFile(zip_filename, 'w') as z:
        for file in file_list:
            z.write(file)
            
    file_dict[state_abbrv] = os.path.join(save_path, zip_filename)

    os.chdir(reset_path)
    
    return 



###########################################################################################################################
# END OF VIP BUILD FUNCTION DEFINITIONS ###################################################################################
###########################################################################################################################

def stable_id(df, ID="id", file_name=None, stable_prfx=None, stable_cols=None):
    """Takes the inputs of a standardized 5.2 VIP CSV and generates an ID that is stable
    Currently only works for source, election, state, locality, precinct, polling_location, street_segment
    Default value is 'id' but if other calculations are needed (such as for hours_open_id), you can 'id' equal whatever to call that column
    You can also use your own stable_prfx and stable_cols values depending on your source data and need to customize the method for making IDs static"""
    if len(df) == 0:
        return

    stable_dict = {
        "source": ["so", "name", "vip_id"],
        "election": ["el", "name", "date"],
        "state": ["st", "name"],
        "locality": ["lc", "name", "type", "is_mail_only"],
        "precinct": [
            "pr",
            "name",
            "number",
            "is_mail_only",
            "precinct_split_name",
            "ward",
        ],
        "polling_location": [
            "pl",
            "name",
            "address_line",
            "structured_line_1",
            "structured_line_2",
            "structured_city",
            "structured_state",
            "structured_zip",
        ],
        "drop_box": [
            "db",
            "name",
            "address_line",
            "structured_line_1",
            "structured_line_2",
            "structured_city",
            "structured_state",
            "structured_zip",
        ],
        "early_vote_site": [
            "ev",
            "name",
            "address_line",
            "structured_line_1",
            "structured_line_2",
            "structured_city",
            "structured_state",
            "structured_zip",
        ],
        "street_segment": [
            "ss",
            "address_direction",
            "city",
            "house_number_prefix",
            "house_number_suffix",
            "includes_all_addresses",
            "includes_all_streets",
            "odd_even_both",
            "start_house_number",
            "end_house_number",
            "state",
            "street_direction",
            "street_name",
            "street_suffix",
            "unit_number",
            "zip",
        ],
    }

    if (
        file_name in stable_dict.keys()
    ):  # if file_name is provided, use the default columns for that file type
        stable_cols = stable_dict[file_name][1:]
        if (
            stable_prfx is None
        ):  # if no prefix is provided, use the default prefix for that file type
            stable_prfx = stable_dict[file_name][0]

    # now, assign the id value based on the stable prefix and a calculation of the stable_cols values
    df[ID] = (
        stable_prfx
        + "_"
        + df[stable_cols].apply(
            lambda x: hashlib.sha1(
                (
                    " ".join(
                        [str(col).upper().strip() for col in x if str(col) != "nan"]
                    ).encode()
                )
            ).hexdigest(),
            axis=1,
        )
    )
    return df

class warn_obj:
    def __init__(self, desc = "", rows = [], fatal = False):
        self.desc = desc
        self.rows = rows
        self.fatal = fatal

def generate_warnings_state_data(state_data, state_abbrv, state_fullname, target_smart):
    """
    PURPOSE: isolate which rows, if any, have warnings or fatal errors
    INPUT: state_data, state_abbrv, state_fullname
    RETURN: list of warning objects
    """
    
    l = [
            warning_multi_directions(state_data), # General warnings
            warning_multi_addresses(state_data),
            warning_cross_street(state_data),
            warning_missing_data(state_data), # Fatal errors
            #warning_date_year(state_data),
            #warning_semi_colon(state_data),
            warning_ocd_id(state_data, state_abbrv),
            warning_bad_zip(state_data),
            warning_bad_timing(state_data),
            warning_unmatched_precinct_hc(state_data, target_smart),
            warning_unmatched_precinct_ts(state_data, target_smart)
        ]

    warnings = [x for x in l if x is not None]

    return warnings


def warning_missing_data(state_data):
    """
    PURPOSE: isolate which rows, if any, are missing data in the state data 
    INPUT: state_data
    RETURN: missing_data_rows
    """
    print("MISSING DATA")
    # SELECT feature(s) (all features from state_data except 3 features)
    missing_data_check = state_data[state_data.columns.difference(['address_line2', 'address_line3', 'directions',"precinct"])].isnull().any(axis=1)
    missing_data_check.index = missing_data_check.index + 1  # INCREASE INDEX to correspond with google sheets index
    missing_data_rows = missing_data_check.loc[lambda x: x==True].index.values.tolist()
    '''
    if missing_data_check.any(): # IF any data is missing
        missing_data_rows = missing_data_check.loc[lambda x: x==True].index.values.tolist()
        if len(missing_data_rows) > 30:  # IF there are more than 30 rows with missing data then simply notify user
            missing_data_rows = ['More than 30 rows with missing data']
    '''
    if missing_data_rows:        
        return warn_obj("Rows with missing data:", missing_data_rows, fatal = True)
    else:
        return None



def warning_cross_street(state_data):
    """
    PURPOSE: isolate which rows, if any, have invalid cross street (e.g. 1st & Main St)
    INPUT: state_data
    RETURN: cross_street_rows
    """
    print("CROSS ST")
    # NOTE: Invalid cross streets sometimes do not map well on Google's end 
    cross_street_addresses = state_data[state_data['address_line1'].str.contains(' & | and ')]
    cross_street_rows = sorted(list(cross_street_addresses.index + 1))

    if cross_street_rows:
        return warn_obj("Rows with cross street addresses:", cross_street_rows, fatal = False)
    else: return None



def warning_multi_addresses(state_data):
    """
    PURPOSE: isolate which polling locations (OCD_ID, location name), if any, have multiple addresses
             (warning: each unique set of addresses is considered a polling location)
    INPUT: state_data
    RETURN: multi_address_rows
    """
    print("MULTI ADDR")
    # SELECT feature(s) (3 selected)
    addresses = state_data[['OCD_ID','location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip']].drop_duplicates()
    multi_addresses = addresses[addresses.duplicated(subset=['OCD_ID','location_name'], keep=False)]
    multi_addresses.index = multi_addresses.index + 1   # INCREASE INDEX to correspond with google sheets index

    multi_address_rows = []
    if not multi_addresses.empty:  # IF the dataframe is not empty
        multi_address_rows = sorted([tuple(x) for x in multi_addresses.groupby(['OCD_ID', 'location_name']).groups.values()])

    if multi_address_rows:
        return warn_obj("Rows with multiple addresses for one polling location:", multi_address_rows, fatal = False)
    else: return None


def warning_multi_directions(state_data):
    """
    PURPOSE: isolate which polling locations, if any, have multiple directions 
             (warning: each unique set of directions is considered a polling location)
    INPUT: state_data
    RETURN: multi_directions_rows
    """
    print("MULTI DIRECTIONS")
    # SELECT feature(s) (4 selected)
    unique_rows = state_data[['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip', 'directions']].drop_duplicates()
    duplicate_locations = unique_rows[unique_rows.duplicated(subset=['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip'],keep=False)]
    duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index

    multi_directions_rows = []
    if not duplicate_locations.empty: # IF there are polling locations with multiple locations
        multi_directions_rows = sorted([tuple(x) for x in \
                                        duplicate_locations.groupby(['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip']).groups.values()])

    if multi_directions_rows:
        return warn_obj("Rows with multiple directions for one polling location:", multi_directions_rows, fatal = False)
    else: return None



def warning_ocd_id(state_data, state_abbrv):
    """
    PURPOSE: isolate which issues with ocd_ids
    INPUT: state_data
    RETURN: ocd_id_rows
    """
    print("OCDID")
    ocd_id_rows = []

    # ISOLATE if ocd-division is incorrect
    ocd_id_issue = state_data[~state_data['OCD_ID'].str.contains('ocd-division')]
    if not ocd_id_issue.empty:
        ocd_id_rows.append(('ocd-id', str(set(ocd_id_issue.index+1)).strip('{}')))

    # ISOLATE if country is incorrect
    country_issue = state_data[~state_data['OCD_ID'].str.contains('country:us')]
    if not country_issue.empty:
        ocd_id_rows.append(('country', str(set(country_issue.index+1)).strip('{}')))

    # ISOLATE if state is incorrect 
    state_string = 'state:' + state_abbrv.lower()
    state_issue = state_data[~state_data['OCD_ID'].str.contains(state_string)]
    if not state_issue.empty:
        ocd_id_rows.append(('state', str(set(state_issue.index+1)).strip('{}')))

    # ISOLATE if country is incorrect
    if state_abbrv != 'AK': # Alaska ocd-ids does not include county/place 
        county_place_issue = state_data[~state_data['OCD_ID'].str.contains(r'county|place|sldl|parish')]
        if not county_place_issue.empty:
            ocd_id_rows.append(('county|place|sldl|parish', str(set(county_place_issue.index+1)).strip('{}')))

    # ISOLATE if the number of slashes is incorrect
    if state_abbrv != 'AK': # Alaska ocd-ids have 2 and 3 slashes, depending
        slash_number = 3
        slash_issue = state_data[state_data['OCD_ID'].str.count('/') != slash_number]
        if not slash_issue.empty:
            ocd_id_rows.append(('slashes', str(set(slash_issue.index+1)).strip('{}')))

    if ocd_id_rows:
        return warn_obj("Rows with bad OCD IDs:", ocd_id_rows, fatal = True)
    else: return None



def warning_date_year(state_data): 
    """
    PURPOSE: isolate which rows, if any, have a start_date or end_date outside of the election year
    INPUT: state_data
    RETURN: date_year_rows
    """
    
    temp = pd.DataFrame()

    # FORMAT features (2 formatted)
    temp['start_date'] = pd.to_datetime(state_data['start_date'])
    temp['end_date'] = pd.to_datetime(state_data['end_date'])
    
    # ISOLATE data errors in 2 features
    incorrect_start_dates = temp[temp['start_date'].dt.year != ELECTION_YEAR]
    incorrect_end_dates = temp[temp['end_date'].dt.year != ELECTION_YEAR]
    incorrect_dates = incorrect_start_dates.append(incorrect_end_dates)

    date_year_rows = sorted(list(set(incorrect_dates.index + 1)))

    if date_year_rows:
        return warn_obj("Rows with start or end dates outside election year:", date_year_rows, fatal = True)
    else: return None



def warning_semi_colon(state_data):
    """
    PURPOSE: isolate which rows, if any, have ;'s instead of :'s in hours
    INPUT: state_data
    RETURN: semi_colon_rows
    """

    # ISOLATE data errors in 2 features
    semi_colon_start_time = state_data[state_data['start_time'].str.contains(';')]
    semi_colon_end_time = state_data[state_data['end_time'].str.contains(';')]
    semi_colon_times = semi_colon_start_time.append(semi_colon_end_time)

    semi_colon_rows = sorted(list(set(semi_colon_times.index + 1)))

    if semi_colon_rows:
        return warn_obj("Rows with semicolons in times:", semi_colon_rows, fatal = True)
    else: return None

def warning_bad_zip(state_data):
    """
    PURPOSE: isolate which rows, if any, have zip codes the wrong number of digits long
            matches zip codes in formats 12345, 123456789, and 12345-6789
    INPUT: state_data
    RETURN: bad_zip_rows
    """
    print("BAD ZIP")
    bad_zip_df = state_data[~state_data["address_zip"].str.fullmatch("(^\d{5}$)|(^\d{9}$)|(^\d{5}-\d{4}$)")]
    bad_zip_rows = sorted(list(set(bad_zip_df.index + 1)))
    
    if bad_zip_rows:
        return warn_obj("Rows with invalid zip codes:", bad_zip_rows, fatal = True)
    else: return None

def warning_bad_timing(state_data):
    """
    PURPOSE: isolate rows where start_time comes after end_time or start_date comes after end_date
    INPUT: state_data
    RETURN: bad_timing_rows
    """
    print("BAD TIMING")
    temp = pd.DataFrame()
    temp["start_time"] = pd.to_datetime(state_data["start_time"])
    temp["end_time"] = pd.to_datetime(state_data["end_time"])
    
    bad_timing_df = state_data[(temp["start_time"] > temp["end_time"])]
    bad_timing_rows = sorted(list(set(bad_timing_df.index + 1)))
    
    if bad_timing_rows:
        return warn_obj("Rows where start comes after end:", bad_timing_rows, fatal = True)
    else: return None

def warning_unmatched_precinct_hc(state_data, target_smart):
    print("UNMATCHED 1")
    hc_df = state_data[["locality", "precinct"]].fillna("").apply(", ".join, axis = 1)
    ts_df = target_smart[["vf_locality", "vf_precinct_name"]].fillna("").apply(", ".join, axis = 1)
    
    hc = set(hc_df.tolist())
    ts = set(ts_df.tolist())
    
    hc_but_not_ts = list(hc.difference(ts))
    if hc_but_not_ts:
        return warn_obj("Precincts found in HC but not TS:", hc_but_not_ts, fatal = False)
    else: return None

def warning_unmatched_precinct_ts(state_data, target_smart):
    print("UNMATCHED 2")
    hc_df = state_data[["locality", "precinct"]].fillna("").apply(", ".join, axis = 1)
    ts_df = target_smart[["vf_locality", "vf_precinct_name"]].fillna("").apply(", ".join, axis = 1)
    
    hc = set(hc_df.tolist())
    ts = set(ts_df.tolist())
    
    ts_but_not_hc = list(ts.difference(hc))
    if ts_but_not_hc:
        return warn_obj("Precincts found in TS but not HC:", ts_but_not_hc, fatal = False)
    else: return None

###########################################################################################################################
# END OF STATE_DATA WARNING FUNCTION DEFINITIONS ##########################################################################
###########################################################################################################################



def generate_warnings_target_smart(state_abbrv, target_smart):
    """
    PURPOSE: isolate which rows, if any, have warnings in target_smart
    INPUT: state_abbrv, target_smart
    RETURN: missing_counties_count, missing_townships_count, missing_state_count
    """

    # GENERATE warnings (3 warnings)
    missing_counties_count = warning_missing_counties(target_smart)
    missing_townships_count = warning_missing_townships(target_smart)
    missing_state_count = warning_missing_state(state_abbrv, target_smart)


    return missing_counties_count, missing_townships_count, missing_state_count



def warning_missing_counties(target_smart):
    """
    PURPOSE: count how many rows are missing county data
    INPUT: target_smart
    RETURN: missing_counties_count
    """

    # COUNT the number of rows missing county data
    target_smart['vf_county_name'] = target_smart['vf_county_name'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    missing_counties_count = len(target_smart[target_smart['vf_county_name'].isnull()])


    return missing_counties_count



def warning_missing_townships(target_smart):
    """
    PURPOSE: count how many rows are missing township data
    INPUT: target_smart
    RETURN: missing_townships_count
    """

    # COUNT the number of rows missing township data (critical for states listed in STATES_USING_TOWNSHIPS)
    target_smart['vf_township'] = target_smart['vf_township'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    missing_townships_count = len(target_smart[target_smart['vf_township'].isnull()])


    return missing_townships_count



def warning_missing_state(state_abbrv, target_smart):
    """
    PURPOSE: count how many rows are missing state abbreviations in target_smart & fill it in
    INPUT: state_abbrv, target_smart
    RETURN: missing_state_count
    """

    # COUNT the number of rows missing target smart data
    target_smart['vf_source_state'] = target_smart['vf_source_state'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    missing_state = target_smart[target_smart['vf_source_state'].isnull()]
    missing_state_count = len(missing_state)

    if not missing_state.empty: # IF there are rows missing state data
        pass
        #street_segment['vf_source_state'].fillna(state_abbrv, inplace=True)
    

    return missing_state_count




###########################################################################################################################
# END OF TARGET_SMART WARNING FUNCTION DEFINITIONS ########################################################################
###########################################################################################################################



def warning_unmatched_precincts(state_abbrv, state_data, target_smart):
    """
    PURPOSE: isolate which county/precinct pairs in state_data, are missing from target_smart, and vice versa
    INPUT: state_abbrv, state_data, target_smart
    RETURN: sd_unmatched_precincts, ts_unmatched_precincts
    """

    # SELECT all state_data County/Precinct pairs
    sd_precincts_df = state_data[['county', 'precinct']].drop_duplicates()
    sd_precincts = set((x[0].upper(), x[1]) for x in sd_precincts_df.values)

    # IDENTIFY Vote Center Locations
    vote_center_locations = list(sd_precincts_df[sd_precincts_df['precinct'] == 'VOTE CENTER']['county'].str.upper())

    # SELECT all target_smart County/Precinct pairs
    if state_abbrv in STATES_USING_TOWNSHIPS:
        ts_precincts_df = target_smart[['vf_township', 'vf_precinct_name']].drop_duplicates()
        ts_precincts_df['vf_precinct_name'][ts_precincts_df['vf_township'].isin(vote_center_locations)] = 'VOTE CENTER'
    else:
        ts_precincts_df = target_smart[['vf_county_name', 'vf_precinct_name']].drop_duplicates()
        ts_precincts_df['vf_precinct_name'][ts_precincts_df['vf_county_name'].isin(vote_center_locations)] = 'VOTE CENTER'

    ts_precincts_df.drop_duplicates(inplace=True)    
    ts_precincts = set(tuple(x) for x in ts_precincts_df.values)

    # UNMATCHED state_data precincts -- County/Precinct pairs only in state_data
    sd_unmatched_precincts_list = sorted(list(sd_precincts - ts_precincts))

    # UNMATCHED target_smart precincts -- County/Precinct pairs only in target_smart
    ts_unmatched_precincts_list = sorted(list(ts_precincts - sd_precincts))


    return sd_unmatched_precincts_list, ts_unmatched_precincts_list



###########################################################################################################################
# END OF MATCHING WARNING FUNCTION DEFINITIONS ############################################################################
###########################################################################################################################



def generate_warnings_txt(state_abbrv, street_segment):
    """
    PURPOSE: isolate matching issues between state_data and target_smart
    INPUT: state_abbrv, state_data, target_smart
    RETURN: sd_unmatched_precincts, ts_unmatched_precincts
    """

    # GENERATE warnings (3 warnings)
    missing_precinct_ids_count = warning_missing_precinct_ids(state_abbrv, street_segment)
    missing_house_numbers_count = warning_missing_house_numbers(state_abbrv, street_segment)
    missing_street_suffixes_count = warning_missing_street_suffixes(state_abbrv, street_segment)


    return missing_precinct_ids_count, missing_house_numbers_count, missing_street_suffixes_count



def warning_missing_precinct_ids(state_abbrv, street_segment):
    """
    PURPOSE: isolate the number of addresses that failed to match to precinct ids
    INPUT: street_segment
    RETURN: missing_precinct_ids_count
    """

    # COUNT the number of rows in street_segment that do not have a precinct_id
    missing_precinct_ids = street_segment[street_segment['precinct_id'].isnull()]
    missing_precinct_ids_count = len(missing_precinct_ids)

    # OPTIONAL: Export a csv of the rows with missing precinct_ids to identify potential state data issues
    # missing_precinct_ids_filename = 'PP_' + state_abbrv + '_missing_precinct_ids.csv'
    # missing_precinct_ids.to_csv(missing_precinct_ids_filename, index=False)


    return missing_precinct_ids_count



def warning_missing_house_numbers(state_abbrv, street_segment):
    """
    PURPOSE: isolate the number of addresses missing a house number
    INPUT: street_segment
    RETURN: missing_house_numbers_count
    """

    # COUNT the number of rows in street_segment that are missing a house number
    missing_house_numbers = street_segment[street_segment['start_house_number'].isnull()]
    missing_house_numbers_count = len(missing_house_numbers)

    # OPTIONAL: Export a csv of the rows with missing house numbers to identify new regex-able patterns
    # missing_house_numbers_filename = 'PP_' + state_abbrv + '_missing_house_numbers.csv'
    # missing_house_numbers.to_csv(missing_house_numbers_filename, index=False)


    return missing_house_numbers_count



def warning_missing_street_suffixes(state_abbrv, street_segment):
    """
    PURPOSE: isolate the number of addresses missing a street suffixes
    INPUT: street_segment
    RETURN: missing_street_suffixes_count
    """
    # COUNT the number of rows in street_segment that are missing a street suffix
    missing_street_suffixes = street_segment[street_segment['street_suffix'].isnull()]
    missing_street_suffixes_count = len(missing_street_suffixes)

    # OPTIONAL: Export a csv of the rows with missing street suffixes to identify new regex-able patterns
    # missing_street_suffixes_filename = 'PP_' + state_abbrv + '_missing_street_suffixes.csv'
    # missing_street_suffixes.to_csv(missing_street_suffixes_filename, index=False)


    return missing_street_suffixes_count




###########################################################################################################################
# END OF WARNING FUNCTION DEFINITIONS #####################################################################################
###########################################################################################################################



def state_report(warnings,
                 state_abbrv, state_feed, state_data, election_authorities, target_smart,
                 files):
    """
    PURPOSE: print state report (general descriptive stats, warnings, and fatal errors)
    INPUT: warnings, # WARNINGS for .txt dataframes
           state_abbrv, state_feed, state_data, election_authorities, target_smart, # MAIN dataframes
           files
    RETURN: None
    """
    
    # _____________________________________________________________________________________________________________________

    # .txt DATAFRAMES | Print dataframe stats 

    # PRINT state name
    print('\n'*1)
    state_name_with_space = ' ' + state_feed['official_name'][0].upper() + ' '
    print(state_name_with_space.center(PRINT_OUTPUT_WIDTH, '-'))
    print('\n')
    print('.txt Size'.center(PRINT_OUTPUT_WIDTH, ' '))
    print()

    for name, df in files.items():

        print(f'{name:>{PRINT_CENTER-2}} | {len(df.index)} row(s)')
        
    # CREATE dataframes of unique OCD IDs for election authorities and state data
    ea = election_authorities[['ocd_division']]
    ea.drop_duplicates(inplace=True)
    sd = state_data[['locality']]
    sd.drop_duplicates(inplace=True)
    ts = target_smart[['vf_locality']]
    ts.drop_duplicates(inplace=True)
    # PRINT count of unique OCD IDs
    print('\n'*2) 
    if state_abbrv in STATES_USING_TOWNSHIPS: # PROCESS states that use townships
        print('# of Unique Townships'.center(PRINT_OUTPUT_WIDTH, ' ')) 
        unique_ocd_id_string = 'townships'
    else: # PROCESS states that use counties/places
        print('# of Unique Counties/Places'.center(PRINT_OUTPUT_WIDTH, ' ')) 
        unique_ocd_id_string = 'counties/places'
    print()
    print(f"{'State Data |':>{PRINT_CENTER}} {len(sd)} {unique_ocd_id_string}")
    print(f"{'TargetSmart |':>{PRINT_CENTER}} {len(ts)} {unique_ocd_id_string}")
    print(f"{'Election Authorities |':>{PRINT_CENTER}} {len(ea)} {unique_ocd_id_string}")


    # _____________________________________________________________________________________________________________________

    
    # WARNINGS | Print warnings


    if warnings:
        print('\n'*2)
        print('----------------------- STATE DATA WARNINGS -----------------------'.center(PRINT_OUTPUT_WIDTH, ' '))
        
    for w in warnings:    
        print("\n")
        print(w.desc.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        for i in range(0, len(w.rows), PRINT_ARRAY_WIDTH):
            print(str(w.rows[i:i+PRINT_ARRAY_WIDTH]).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))


    print('\n'*1)
    print('_'*PRINT_OUTPUT_WIDTH)
    print('\n'*2)


    return 




def summary_report(num_input_states, increment_httperror, increment_processingerror, increment_success,
                   states_failed_to_load, states_failed_to_process, states_successfully_processed):
    """
    PURPOSE: print summary report
    INPUT: increment_httperror, increment_processingerror, increment_success,
           states_failed_to_load, states_failed_to_process, states_successfully_processed
    RETURN: 
    """

    # PRINT final report
    print('\n'*1)
    print('SUMMARY REPORT'.center(PRINT_OUTPUT_WIDTH, ' '))
    print('\n'*1)
    print('Final Status for All Requested States'.center(PRINT_OUTPUT_WIDTH, ' '))
    print()
    print(f"{'Failed to load state data |':>{PRINT_CENTER}} {increment_httperror} state(s) out of {num_input_states}")
    print(f"{'Failed to process |':>{PRINT_CENTER}} {increment_processingerror} state(s) out of {num_input_states}")
    print(f"{'Successfully processed |':>{PRINT_CENTER}} {increment_success} state(s) out of {num_input_states}")

    if states_failed_to_load:
        print('\n'*1)
        print('States that failed to load state data'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        for i in range(0, len(states_failed_to_load),PRINT_ARRAY_WIDTH):
            print(str(states_failed_to_load[i:i+PRINT_ARRAY_WIDTH]).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
        
    if states_failed_to_process:
        print('\n'*1)
        print('States that failed to process & why'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        for i in range(0, len(states_failed_to_process),PRINT_ARRAY_WIDTH):
            print(str(states_failed_to_process[i:i+PRINT_ARRAY_WIDTH]).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))

    if STATES_WITH_WARNINGS:      
        print('\n'*1)
        print('States that processed with warnings'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        for i in range(0, len(STATES_WITH_WARNINGS),PRINT_ARRAY_WIDTH):
            print(str(STATES_WITH_WARNINGS[i:i+PRINT_ARRAY_WIDTH]).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
        
    if states_successfully_processed:
        print('\n'*1)
        print('States that sucessfully processed'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        for i in range(0, len(states_successfully_processed),PRINT_ARRAY_WIDTH):
            print(str(states_successfully_processed[i:i+PRINT_ARRAY_WIDTH]).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
    
    print('\n'*3)


    return


###########################################################################################################################
# END OF REPORT RELATED DEFINITIONS #######################################################################################
###########################################################################################################################

def main():
    # SET UP command line inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('-states', nargs='+')

    # PRINT timestamp to help user guage processing times
    print(f'Timestamp: {datetime.datetime.now().replace(microsecond=0)}')
    start = time.time()

    # _____________________________________________________________________________________________________________________

    # SET UP Google API credentials
    # REQUIRES a local 'token.json' file & 'credentials.json' file
    # https://developers.google.com/sheets/api/quickstart/python
    
    store = file.Storage('../credentials/token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('../credentials/credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))
    
    try: 
        # LOAD state feed data
        state_feed_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, 
                                                                range='STATE_FEED').execute()
        state_feed_values = state_feed_result.get('values', [])
    except:
        print('ERROR | STATE_FEED Google Sheet is either missing from the workbook or there is data reading error.')
        raise

    try: 
        # LOAD election authorities data
        election_authorities_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_EA_ID, 
                                                                          range='ELECTION_AUTHORITIES').execute()
        election_authorities_values = election_authorities_result.get('values', [])
    except:
        print('ERROR | ELECTION_AUTHORITIES Google Sheet is either missing from the workbook or there is data reading error.')
        raise

    # _____________________________________________________________________________________________________________________

    # ESTABLISH connection to AWS MySQL database

    try:
        # CONNECT to AWS RDS MySQL database
        with open('../credentials/targetsmart_credentials.json', 'r') as f: conn_string = json.load(f)
        conn = mysql.connector.connect(**conn_string)
    except:
        print('ERROR | There is a database connection error.')

    # _____________________________________________________________________________________________________________________
      
    # PROCESS all user requested states 
        
    # STORE states with errors
    states_successfully_processed = [] # STORE states that successfully create zip files
    states_failed_to_load = [] # STORE states whose data failed to load
    states_failed_to_process = [] # STORE states that failed to process
    increment_success = 0 # STORE count of states successfully processed
    increment_httperror = 0 # STORE count of states that could not be retrieved or found in Google Sheets
    increment_processingerror = 0 # STORE count of states that could not be processed
    
    
    # PROCESS each state individually (input_states are requested states listed as state abbreviations)
    for _, input_states in parser.parse_args()._get_kwargs(): # ITERATE through input arguments


        input_states = [state.upper() for state in input_states] # FORMAT all inputs as uppercase
        
        # GENERATE state_feed & election_authorities dataframe
        state_feed_all = pd.DataFrame(state_feed_values[1:], columns=state_feed_values[0])
        election_authorities_all = pd.DataFrame(election_authorities_values[0:], columns=election_authorities_values[0])
        election_authorities_all.drop([0], inplace=True)
        election_authorities_all = election_authorities_all[['ocd_division', 'official_title', 'homepage_url', 'state']]


        if 'ALL' in input_states: # IF user requests all states to be processed
            
            input_states = state_feed_all['state_abbrv'].unique().tolist() # FORMAT unique list of 50 state abbreviations

        
        for state_abbrv in input_states:
        
            try:
            
                # LOAD state data
                state_data_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=state_abbrv).execute()
                state_data_values = state_data_result.get('values', [])
                state_data_unfiltered = pd.DataFrame(state_data_values[0:],columns=state_data_values[0])
                state_data_unfiltered.drop([0], inplace=True)
                
                # FILTER rows not marked "complete"
                cols = ["OCD_ID","locality","precinct","location_name",
                        "directions","address_line1","address_line2",
                        "address_line3","address_city","address_state",
                        "address_zip","time_zone","start_time","end_time"]
                state_data = state_data_unfiltered.loc[state_data_unfiltered["status"] == "Complete",cols]
                
                # Check for missing vital data:
                missing_rows = state_data[(state_data[["location_name", "address_line1", "locality", "OCD_ID", "address_city", "address_state", "address_zip", "start_time", "end_time"]]=="").any(axis = 1)].index.tolist()
                if missing_rows:
                    raise Exception("Missing vital data on rows: "+" ".join(str(e+2) for e in missing_rows))
                
                # FILTER dataframes for selected state (2 dataframes)
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state_abbrv] 
                election_authorities = election_authorities_all[election_authorities_all['state'] == state_abbrv] 

                state_feed.reset_index(inplace=True) # RESET index
                
                # CREATE MYSQL table name (format: full name, lowercase, underscores for spaces)
                sql_table_name = "hand_collection." + state_feed.loc[0,'official_name'].lower().replace(' ', '_')


                try: # LOAD TargetSmart data for a single state 
                    
                    # specify county vs township
                    if state_abbrv in STATES_USING_TOWNSHIPS:
                        loc_str = "vf_township"
                    else:
                        loc_str = "vf_county_name"
                
                    # SET list of vars selected in MySQL query
                    targetsmart_sql_col_string = ', '.join([loc_str+" AS vf_locality", 'vf_precinct_name', 'vf_reg_cass_post_directional', 'vf_reg_cass_city',
                                                            'vf_reg_cass_street_num', 'vf_reg_cass_state', 'vf_reg_cass_pre_directional', 
                                                            'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_apt_num', 'vf_reg_cass_zip'])

                    # SET list of fields to filter out if they are empty in TargetSmart
                    targetsmart_sql_filter_string = ''.join([" WHERE vf_reg_city <> ''",
                                                             " AND vf_precinct_name <> ''",
                                                             " AND vf_reg_address_1 <> ''",
                                                             " AND vf_reg_zip <> ''",
                                                             " AND vf_reg_cass_street_name <> 'PO BOX'"])
                    
                    if state_abbrv == "FL":
                        #hc_counties = ["LAKE","PINELLAS","ST LUCIE","MANATEE","MADISON","HARDEE","BAY","MONROE","VOLUSIA","SUMTER",
                        #               "PASCO","PALM BEACH","GULF","CHARLOTTE","BROWARD","ALACHUA","DUVAL","WASHINGTON","NASSAU","LIBERTY"]
                        hc_counties = state_data["locality"].drop_duplicates().tolist()
                        print(hc_counties)
                        hc_counties = ["'"+x+"'" for x in hc_counties]
                        
                        targetsmart_sql_filter_string = targetsmart_sql_filter_string + " AND vf_county_name IN ({})".format(
                            ",".join(hc_counties))
                    
                    # SET MySQL query
                    query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name + \
                                targetsmart_sql_filter_string + ";"
                            
                    target_smart = pd.read_sql(query, conn)

                    conn.rollback() # REFRESH database connection

                except:
                    print('ERROR | TargetSmart data for', state_abbrv, 'is either missing from the database or there is data reading error.')
 
                # GENERATE zip file and print state report
                vip_build(state_abbrv, state_feed, state_data, election_authorities, target_smart)
                

                states_successfully_processed.append(state_abbrv) # STORE states that processed all the way through
                increment_success +=1
                
                
            except HttpError:
                increment_httperror += 1
                states_failed_to_load.append(state_abbrv)
                
            except Exception as e:                
                increment_processingerror += 1
                states_failed_to_process.append((state_abbrv, type(e).__name__, e))


    conn.close() # CLOSE MySQL database connection 


    summary_report(len(input_states), increment_httperror, increment_processingerror, increment_success,
                   states_failed_to_load, states_failed_to_process, states_successfully_processed)


    print(f'Timestamp: {datetime.datetime.now().replace(microsecond=0)}')
    print(f'Run time: {float((time.time()-start)/60):.2f} minute(s)')
    
    if file_dict:
        query = input("Upload? [ALL or state_abbrvs]: ").upper().strip()
        if query == "ALL":
            upload_states = file_dict.keys()
        else:
            query = re.sub(r"\s{2,}", " ", query)
            upload_states = [x for x in query.split(" ") if x in file_dict.keys()]
        for state in upload_states:
            upload(file_dict[state])

if __name__ == '__main__':
    
    main()