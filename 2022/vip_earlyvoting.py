from __future__ import print_function
from sys import argv
import argparse
import os

# GOOGLE API libraries
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http
from oauth2client import file, client, tools

# DATA MUNGING & FORMATTING libraries
import pandas as pd
import json
import datetime
import re # regex
import numpy as np
import time
import pytz
import hashlib

# OTHER libraries
from zipfile import ZipFile
import warnings

from upload_script import upload
# _________________________________________________________________________________________________________________________

# GLOBAL VARIABLES | Set global variables and print display formats

warnings.filterwarnings('ignore') # FILTER out warnings that are not critical

pd.set_option('display.max_columns', 100)  # or 1000 or None
pd.set_option('display.max_rows', 1000)  # or 1000 or None
PRINT_OUTPUT_WIDTH = 100 # SET print output length
PRINT_CENTER = int(PRINT_OUTPUT_WIDTH/2)
np.set_printoptions(linewidth=PRINT_OUTPUT_WIDTH)
PRINT_TUPLE_WIDTH = 2
PRINT_ARRAY_WIDTH = 11 

ELECTION_YEAR = 2022

STATES_WITH_WARNINGS = [] # STORE states that trigger warnings

OUTPUT_DIR = os.path.expanduser("~/Democracy Works VIP Dropbox/Democracy Works VIP's shared workspace/hand_collection/")

fips_lookup = pd.read_json("fips_ocdid_zips_dictionary.txt").T["ocdid"]
fips_lookup.dropna(inplace = True)

fips_lookup_r = pd.Series(fips_lookup.index.values, name = "FIPS", index = fips_lookup)

file_dict = {}

# _________________________________________________________________________________________________________________________

# GOOGLE API | Set scopes & Google Spreadsheet IDs (1 scope, 2 IDs)

# PRO-TIP: if modifying these scopes, delete the file token.json
# NOTE: Scope url should not change year to year unless Google alters syntax
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# individual states & STATE_FEED tabs in a single Google Sheet (multiple tabs)
# https://docs.google.com/spreadsheets/u/1/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=366784608
SPREADSHEET_ID = '1ulDgIatmEe4IG-Wtu0ZcqDG-2RRAESCNeUwHj4JGOeg' 

# ELECTION_AUTHORITIES entire Google Sheet (1 tab) 
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '12jlzfFM5Fr7LmLaH_1hJ2MLO2-YCcI5eKkRK1b8ayZA' 


# _________________________________________________________________________________________________________________________


def vip_build(state_abbrv, state_feed, state_data, election_authorities):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 9 .txt files
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt)
    INPUT: state_abbrv, state_data, state_feed, election_authorities
    RETURN: None
    """
    # PREP | Identify data issues, create/format features, and standardize dataframes

    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # first make sure there are leading zeros
    state_feed['state_id'] = state_abbrv.lower() + state_feed['state_fips']
    state_feed['external_identifier_type'] = 'ocd-id' 
    
    # CLEAN/FORMAT state_feed, state_data, and election_authorities (3 dataframes)
    try:
        state_feed, state_data, election_authorities = clean_data(state_abbrv, state_feed, state_data, election_authorities)
    except Exception as e:
        raise Exception("Clean, "+str(e)) from e
        
    # GENERATE warnings in state_data
    try:
        warnings = generate_warnings(state_data, state_abbrv, state_feed['official_name'][0])
    except Exception as e:
        raise Exception("Warnings, "+str(e)) from e
    
    # _____________________________________________________________________________________________________________________

    # CREATE IDS | Create IDs on dataframes
    
    if election_authorities.empty:
        # CREATE empty election_authorities DataFrame if state not in election administration sheet
        election_authorities = pd.DataFrame(columns=['ocd_division','election_administration_id','homepage_url',
                                                     'official_title','election_official_person_id'])
    
    else:
        # # SELECT desired cols from election_authorities
        election_authorities = election_authorities[['ocd_division','homepage_url', 'official_title']]
        
        # CREATE 'election_adminstration_id'
        temp = election_authorities[['ocd_division']]
        temp.drop_duplicates(['ocd_division'], inplace=True)
        
        temp['election_administration_id'] = "ea_" + temp["ocd_division"]
        
        # temp['election_administration_id'] = temp.apply(lambda x: "ea" + str(fips_lookup_r[x["ocd_division"]]).zfill(5), axis = 1)
        election_authorities = pd.merge(election_authorities, temp, on =['ocd_division'])
        election_authorities.drop_duplicates(subset=['election_administration_id'], inplace=True) #REMOVE all except first election administration entry for each ocd-id
        
        # CREATE 'election_official_person_id'
        temp = election_authorities[['ocd_division', 'official_title']]
        temp.drop_duplicates(['ocd_division', 'official_title'], keep='first',inplace=True)
        temp['election_official_person_id'] = "per_" + temp["ocd_division"]
        # temp['election_official_person_id'] = temp.apply(lambda x: "per" + str(fips_lookup_r[x["ocd_division"]]).zfill(5), axis = 1)
        election_authorities = pd.merge(election_authorities, temp, on =['ocd_division', 'official_title'])
        
    # CREATE stable IDs for polling locations
    stable_cols = ['location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_state', 'address_zip', "is_drop_box", "is_early_voting"]
    
    state_data = stable_id(state_data, ID = 'polling_location_ids', stable_prfx = "pl", stable_cols = stable_cols)
    state_data = stable_id(state_data, ID = 'hours_open_id', stable_prfx = "ho", stable_cols = stable_cols)
    
    # _____________________________________________________________________________________________________________________

    # FILE CREATION | Generate files for dashboard zip

    # GENERATE 9 .txt files
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
        locality = generate_locality(state_feed, state_data, election_authorities)
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
    
    # GENERATE zip file
    generate_zip(state_abbrv, state_feed, {'state':state, 
                                           'source':source,
                                           'election': election,
                                           'election_administration':election_administration,
                                           'department': department,
                                           'person': person,
                                           'locality':locality,
                                           'polling_location': polling_location,
                                           'schedule': schedule})
    
    # _____________________________________________________________________________________________________________________

    # REPORT | Print zip file sizes, dataframe descriptions, and data warnings
    
    # PRINT state report
    state_report(warnings,
                 state_abbrv, state_feed, state_data, election_authorities,
                 {'state':state, 
                  'source':source,
                  'election': election,
                  'election_administration':election_administration,
                  'department': department,
                  'person': person,
                  'locality':locality,
                  'polling_location': polling_location,
                  'schedule': schedule})
    

    return

    

def clean_data(state_abbrv, state_feed, state_data, election_authorities):
    """
    PURPOSE: cleans and formats state_feed, state_data, & election_authorities to output standards
    INPUT: state_abbrv, state_feed, state_data, election_authorities
    RETURN: state_feed, state_data, election_authorities dataframes
    """

    # CREATE/FORMAT | Adjust variables to desired standards shared across relevant .txt files

    # REPLACE empty strings with NaNs
    state_data = state_data.replace('^\\s*$', np.nan, regex=True)
    
    # TRIM whitespace
    state_data = state_data.apply(lambda x: x.str.strip())

    # FORMAT OCD IDs (2 formatted)
    state_data['OCD_ID'] = state_data['OCD_ID'].str.strip()
    election_authorities['ocd_division'] = election_authorities['ocd_division'].str.strip()

    # FORMAT booleans (4 formatted)
    true_chars = [char for char in 'true' if char not in 'false'] # SET unique chars in 'true' and not in 'false'
    false_chars = [char for char in 'false' if char not in 'true'] # SET unique chars in 'true' and not in 'false'
    lambda_funct = (lambda x: None if not x else ( \
                    'true' if any(char in x for char in true_chars) == True else ('false' if any(char in x for char in false_chars) == True else np.nan)))
    if state_data['is_drop_box'].isnull().all(): # NOTE: is_drop_box is often left empty, which should be treated as false
        state_data['is_drop_box'] = 'false'
    else:
        state_data['is_drop_box'] = state_data['is_drop_box'].str.lower().apply(lambda_funct)
    state_data['is_early_voting'] = state_data['is_early_voting'].str.lower().apply(lambda_funct)
    state_data['is_only_by_appointment'] = state_data['is_only_by_appointment'].str.lower().apply(lambda_funct)
    state_data['is_or_by_appointment'] = state_data['is_or_by_appointment'].str.lower().apply(lambda_funct)

    # FORMAT ocd division ids (2 formatted)
    state_data['OCD_ID'] = state_data['OCD_ID'].str.strip()
    
    state_data['address_line1'] = state_data['address_line1'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['address_line2'] = state_data['address_line2'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['address_line3'] = state_data['address_line3'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['address_city'] = state_data['address_city'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['address_state'] = state_data['address_state'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['location_name'] = state_data['location_name'].str.strip().str.replace('\'S', '\'s')
    
    state_data = state_data.replace("[\r\n]+", "", regex = True)
    
    state_data.loc[state_data["is_only_by_appointment"]=="true","time_zone":"end_time"] = np.nan
    
    # _____________________________________________________________________________________________________________________

    # ERROR HANDLING | Interventionist adjustments to account for state eccentricities and common hand collection mistakes, etc

    return state_feed, state_data, election_authorities



def generate_election(state_feed):
    """
    PURPOSE: generates election dataframe for .txt file
    INPUT: state_feed
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

    # SELECT feature(s) (12 selected)
    polling_location = state_data[['polling_location_ids','location_name', 
                                   'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_state', 'address_zip',
                                   'directions', 'hours_open_id', 'is_drop_box', 'is_early_voting']] 
    
    # FORMAT col(s) (8 formatted)                              
    polling_location.rename(columns={'polling_location_ids':'id', 
                                     'location_name':'name',
                                     'address_line1':'structured_line_1',
                                     'address_line2':'structured_line_2',
                                     'address_line3':'structured_line_3',
                                     'address_city':'structured_city',
                                     'address_state':'structured_state',
                                     'address_zip':'structured_zip'}, inplace=True)
    polling_location.drop_duplicates("id", inplace=True)


    return polling_location



def generate_schedule(state_data, state_feed):
    """
    PURPOSE: generates schedule dataframe for .txt file, calculating UTC offsets and daylight savings time
    INPUT: state_data, state_feed
    RETURN: schedule dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/hours_open.html#schedule
    """ 
    
    # SELECT feature(s) (8 selected)
    schedule = state_data[['time_zone', 'start_time', 'end_time', 'start_date', 'end_date', 'hours_open_id',
                           'is_only_by_appointment', 'is_or_by_appointment']].reset_index(drop=True) 
    
    schedule.replace("", np.nan, inplace = True)

    # Split rows along time zone transitions
    for i, row in schedule.iterrows():
        if row[["time_zone", "start_time", "end_time"]].isna().any():
            continue
        tz = pytz.timezone(row["time_zone"])
        dt_start = tz.localize(datetime.datetime.strptime(row["start_date"]+row["start_time"], "%Y-%m-%d%H:%M:%S"))
        dt_end = tz.localize(datetime.datetime.strptime(row["end_date"]+row["end_time"], "%Y-%m-%d%H:%M:%S"))
        
        if dt_start.utcoffset() != dt_end.utcoffset():
            utc_transition_times = [pytz.utc.localize(x) for x in tz._utc_transition_times]
            try:
                current_transition, = [x for x in utc_transition_times if dt_start < x < dt_end]
            except ValueError:
                print("Time zone error: line {0} spans multiple DST transitions".format(i))
            
            current_transition = current_transition.astimezone(tz)
            
            end_split = current_transition.date() - datetime.timedelta(days = 1)
            
            # Special case: if the location is open before 2:00 AM on the day of the transition, that one day can have different offsets for start and end times
            if datetime.datetime.strptime(row["start_time"], "%H:%M:%S").time() < current_transition.astimezone(tz).time():
                newrow = row.copy()
                newrow["start_date"] = current_transition.strftime("%Y-%m-%d")
                newrow["end_date"] = current_transition.strftime("%Y-%m-%d")
                schedule = pd.concat([schedule, newrow.to_frame().T], axis = 0, ignore_index = True)
                
                begin_split = current_transition.date() + datetime.timedelta(days = 1)
            else:
                begin_split = current_transition.date()
                
            newrow = row.copy()
            newrow["start_date"] = begin_split.strftime("%Y-%m-%d")
            schedule = pd.concat([schedule, newrow.to_frame().T], axis = 0, ignore_index = True)
            
            schedule.iloc[i]["end_date"] = end_split.strftime("%Y-%m-%d")
     
    # after necessary rows have been split, add offsets to the start and end times
    for i, row in schedule.iterrows():
        if row[["time_zone", "start_time", "end_time"]].isna().any():
            continue
        
        tz = pytz.timezone(row["time_zone"])
        dt_start = tz.localize(datetime.datetime.strptime(row["start_date"]+row["start_time"], "%Y-%m-%d%H:%M:%S"))
        dt_end = tz.localize(datetime.datetime.strptime(row["end_date"]+row["end_time"], "%Y-%m-%d%H:%M:%S"))
        
        offset_start = dt_start.strftime("%z")
        offset_end = dt_end.strftime("%z")
        
        offset_start = offset_start[:3]+":"+offset_start[3:]
        offset_end = offset_end[:3]+":"+offset_end[3:]
        
        schedule.iloc[i]["start_time"] = row["start_time"]+offset_start
        schedule.iloc[i]["end_time"] = row["end_time"]+offset_end
    
    # CREATE feature(s) (1 created)
    #schedule.sort_values(["hours_open_id", "start_date"], inplace = True, ignore_index = True)
    schedule.reset_index(drop=True, inplace=True) 
    schedule['id'] = 'sch' + (schedule.index + 1).astype(str).str.zfill(4) 
    
    schedule.drop(columns = "time_zone", inplace = True)

    return schedule


def generate_source(state_feed):
    """
    PURPOSE: generates source dataframe for .txt file
    INPUT: state_feed
    RETURN: source dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/source.html
    """ 
    
    # SELECT feature(s) (1 selected)
    source = state_feed[['state_fips']]

    # CREATE/FORMAT feature(s) (4 created, 1 formatted)    
    source.reset_index(drop=True, inplace=True)
    source['id'] = 'src' + (source.index + 1).astype(str).str.zfill(4)
    source['date_time'] = datetime.datetime.now().replace(microsecond=0).isoformat() 
    source['name'] = 'Democracy Works Outreach Team'
    source['version'] = '5.2' # REFERENCES VIP SPEC
    source.rename(columns={'state_fips':'vip_id'}, inplace=True) # RENAME col(s)
    

    return source



def generate_state(state_feed): 
    """
    PURPOSE: generates state dataframe for .txt file
    INPUT: state_feed
    RETURN: state dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/state.html
    """ 
    
    # SELECT feature(s) (4 selected)
    state = state_feed[['state_id', 'external_identifier_type', 'ocd_division', 'official_name']]

    # FORMAT features (3 formatted)
    state.rename(columns={'state_id':'id', 
                          'ocd_division':'external_identifier_value', 
                          'official_name':'name'}, inplace=True)
    

    return state



def generate_locality(state_feed, state_data, election_authorities):
    """
    PURPOSE: generates locality dataframe for .txt file
    INPUT: state_feed, state_data, election_authorities
    RETURN: locality dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/locality.html
    """ 

    # SELECT feature(s) (2 selected)
    locality = state_data[['OCD_ID', 'polling_location_ids']]

    # GROUP polling_location_ids
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows
    grouped = locality.groupby('OCD_ID')
    ids_joined = pd.DataFrame(grouped.aggregate(lambda x: ' '.join(x))['polling_location_ids']) # FLATTEN aggregated df
    locality = ids_joined.reset_index()  

    # MERGE locality with election_authorities
    temp = election_authorities[['ocd_division', 'election_administration_id']]
    locality = locality.merge(temp, left_on='OCD_ID', right_on='ocd_division', how='left')
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows from merge 

    # CREATE/FORMAT feature(s) (4 created, 1 formatted)
    locality['state_id'] = state_feed['state_id'][0]
    locality['name'] = locality['OCD_ID'].str.extract('([^\\:]*)$', expand=False)
    locality['external_identifier_type'] = state_feed['external_identifier_type'][0]
    locality.reset_index(drop=True, inplace=True) 
    locality['id'] = "loc_" + locality["OCD_ID"]
    locality.rename(columns={'OCD_ID':'external_identifier_value'}, inplace=True)
    
    # REMOVE feature(s) (1 removed)
    locality.drop('ocd_division', axis=1, inplace=True) 


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
    if not election_authorities.empty:
        department.drop_duplicates(inplace=True)
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

    
 
def generate_zip(state_abbrv, state_feed, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt)
    INPUT: state_abbrv, state_feed, files
    RETURN: exports zip of 9 .txt files
    """
    
    reset_path = os.getcwd()
    
    election = state_feed['election_date'][0]

    save_path = os.path.join(OUTPUT_DIR,election,state_abbrv,"early_voting")

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
    zip_filename = 'vipfeed-ev-' + election + '-' + state_abbrv + '.zip'

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

def generate_warnings(state_data, state_abbrv, state_fullname):
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
            warning_date_year(state_data),
            #warning_semi_colon(state_data),
            warning_ocd_id(state_data, state_abbrv),
            warning_bad_zip(state_data),
            warning_bad_timing(state_data)
        ]

    warnings = [x for x in l if x is not None]

    return warnings


def warning_missing_data(state_data):
    """
    PURPOSE: isolate which rows, if any, are missing data in the state data 
    INPUT: state_data
    RETURN: missing_data_rows
    """
    # SELECT feature(s) (all features from state_data except 3 features)
    missing_data_check = state_data[state_data.columns.difference(['address_line2', 'address_line3', 'directions', "time_zone", "start_time", "end_time"])].isnull().any(axis=1)
    missing_data_check.index = missing_data_check.index + 1  # INCREASE INDEX to correspond with google sheets index
    missing_data_rows = missing_data_check.loc[lambda x: x==True].index.values.tolist()
    '''
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
    # SELECT feature(s) (3 selected)
    addresses = state_data[['OCD_ID','location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip', "is_drop_box", "is_early_voting"]].drop_duplicates()
    multi_addresses = addresses[addresses.duplicated(subset=['OCD_ID','location_name', "is_drop_box", "is_early_voting"], keep=False)]
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
    # SELECT feature(s) (4 selected)
    unique_rows = state_data[['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip', 'directions', 'is_drop_box', 'is_early_voting']].drop_duplicates()
    duplicate_locations = unique_rows[unique_rows.duplicated(subset=['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip', 'is_drop_box', 'is_early_voting'],keep=False)]
    duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index

    multi_directions_rows = []
    if not duplicate_locations.empty: # IF there are polling locations with multiple locations
        multi_directions_rows = sorted([tuple(x) for x in \
                                        duplicate_locations.groupby(['OCD_ID', 'location_name', 'address_line1', 'address_line2', 'address_line3', 'address_city', 'address_zip', 'is_drop_box', 'is_early_voting']).groups.values()])

    if multi_directions_rows:
        return warn_obj("Rows with multiple directions for one polling location:", multi_directions_rows, fatal = False)
    else: return None



def warning_ocd_id(state_data, state_abbrv):
    """
    PURPOSE: isolate which issues with ocd_ids
    INPUT: state_data
    RETURN: ocd_id_rows
    """
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
    '''
    # ISOLATE if the number of slashes is incorrect
    if state_abbrv != 'AK': # Alaska ocd-ids have 2 and 3 slashes, depending
        slash_number = 3
        slash_issue = state_data[state_data['OCD_ID'].str.count('/') != slash_number]
        if not slash_issue.empty:
            ocd_id_rows.append(('slashes', str(set(slash_issue.index+1)).strip('{}')))
            '''
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
    temp = pd.DataFrame()
    temp["start_time"] = pd.to_datetime(state_data["start_time"])
    temp["end_time"] = pd.to_datetime(state_data["end_time"])
    temp["start_date"] = pd.to_datetime(state_data["start_date"])
    temp["end_date"] = pd.to_datetime(state_data["end_date"])
    
    bad_timing_df = state_data[(temp["start_time"] > temp["end_time"]) + (temp["start_date"] > temp["end_date"])]
    bad_timing_rows = sorted(list(set(bad_timing_df.index + 1)))
    
    if bad_timing_rows:
        return warn_obj("Rows where start comes after end:", bad_timing_rows, fatal = True)
    else: return None

###########################################################################################################################
# END OF WARNING FUNCTION DEFINITIONS #####################################################################################
###########################################################################################################################



def state_report(warnings,
                 state_abbrv, state_feed, state_data, election_authorities, files):
    """
    PURPOSE: print state report (general descriptive stats and warnings)
    INPUT: multi_directions_rows, multi_address_rows, cross_street_rows, # WARNINGS for state data
           missing_data_rows, semi_colon_rows, date_year_rows, # FATAL ERRORS for state_data
           missing_zipcode_rows, missing_state_abbrvs_rows, 
           timezone_mismatch_rows, ocd_id_rows,
           state_abbrv, state_feed, state_data, election_authorities, files
    RETURN: 
    """
    
    # PRINT state name
    print('\n'*1)
    state_name_with_space = ' ' + state_feed['official_name'][0].upper() + ' '
    print(state_name_with_space.center(PRINT_OUTPUT_WIDTH, '-'))
    print('\n')
    
    # PRINT the length of text files in zip
    print('.txt Size'.center(PRINT_OUTPUT_WIDTH, ' '))
    print()
    for name, df in files.items():

        print(f'{name:>{PRINT_CENTER-2}} | {len(df.index)} row(s)')

    # PRINT count of unique OCD IDs
    sd = state_data[['OCD_ID']] # CREATE count of unique OCD IDs in state_data
    sd.drop_duplicates(inplace=True)
    print('\n'*2) 
    print('# of Unique Counties/Places  '.center(PRINT_OUTPUT_WIDTH, ' ')) 
    print()
    print(f"{'State Data |':>{PRINT_CENTER}} {len(sd)} counties/places")
    if not election_authorities.empty:
        ea = election_authorities[['ocd_division']]
        ea.drop_duplicates(inplace=True)
        print(f"{'Election Authorities |':>{PRINT_CENTER}} {len(ea)} counties/places")
    else:
        print(f"{'Election Authorities |':>{PRINT_CENTER}} 0 counties/places")


    # _____________________________________________________________________________________________________________________

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
    parser.add_argument('-states', nargs='+', required=True)

    print('Timestamp:', datetime.datetime.now().replace(microsecond=0))
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
                cols = ["OCD_ID","locality","location_name",
                        "directions","address_line1","address_line2",
                        "address_line3","address_city","address_state",
                        "address_zip","time_zone","start_time","end_time",
                        "start_date","end_date","is_only_by_appointment",
                        "is_or_by_appointment","is_drop_box","is_early_voting"]
                state_data = state_data_unfiltered.loc[state_data_unfiltered["status"] == "Complete",cols]
                
                # Check for missing vital data:
                missing_rows = state_data[(state_data[["start_date", "end_date", "location_name", "address_line1", "locality", "OCD_ID", "address_city", "address_state", "address_zip"]]=="").any(axis = 1)].index.tolist()
                if missing_rows:
                    raise Exception("Missing vital data on rows: "+" ".join(str(e+2) for e in missing_rows))
                
                # FILTER state_feed and election_authorities
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state_abbrv] # FILTER state_feed_all for selected state
                state_feed.reset_index(drop=True, inplace=True)
                election_authorities = election_authorities_all[election_authorities_all['state'] == state_abbrv] # FILTER election_authorities_all for selected state
                
                # GENERATE zip file and print state report
                vip_build(state_abbrv, state_feed, state_data, election_authorities)

                states_successfully_processed.append(state_abbrv)
                increment_success +=1
            
            except HttpError:
                increment_httperror += 1
                states_failed_to_load.append(state_abbrv)

            except Exception as e:
                increment_processingerror += 1
                exception_string = state_abbrv + ' | ' + str(type(e).__name__) + ': ' + str(e)
                states_failed_to_process.append(exception_string)


    summary_report(len(input_states), increment_httperror, increment_processingerror, increment_success,
                   states_failed_to_load, states_failed_to_process, states_successfully_processed)


    print('Timestamp:', datetime.datetime.now().replace(microsecond=0))
    print(f'Run time: {float((time.time()-start)):.2f} second(s)')

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