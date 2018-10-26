from __future__ import print_function
import warnings
warnings.filterwarnings('ignore')
from sys import argv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http
from oauth2client import file, client, tools
import pandas as pd
import datetime
import argparse
import os
from zipfile import ZipFile
import re
import json
import mysql.connector 
import numpy as np

# _________________________________________________________________________________________________________________________

# GLOBAL VARIABLES | Set global variables and print display formats

pd.set_option('display.max_columns', 100)  # or 1000 or None
pd.set_option('display.max_rows', 100)  # or 1000 or None
PRINT_OUTPUT_WIDTH = 100 # SET print output length
PRINT_CENTER = 50

ELECTION_YEAR = 2018

STATES_USING_TOWNSHIPS = ['NH', 'ME'] # SET states that use townships instead of counties for elections

STATES_WITH_WARNINGS = [] # STORE states that trigger warnings

# _________________________________________________________________________________________________________________________

# GOOGLE API | Set scopes & Google Spreadsheet IDs (2 IDs)

# PRO-TIP: if modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# state data & STATE_FEED
# https://docs.google.com/spreadsheets/d/1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M/edit#gid=2009403147
SPREADSHEET_ID = '1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M' 

# ELECTION_AUTHORITIES
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o' 


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

    # GENERATE warnings/fatal errors in state_data (2 warnings, 5 fatal errors)
    multi_directions_rows, multi_address_rows, cross_street_rows, \
      missing_data_rows, semi_colon_rows, date_year_rows, \
      missing_zipcode_rows, missing_state_abbrvs_rows = generate_warnings_state_data(state_abbrv, state_data)
    
    # GENERATE warnings in target_smart (2 types of warnings)
    missing_counties_count, missing_townships_count, missing_state_count = generate_warnings_target_smart(state_abbrv, target_smart)

    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # first make sure there are leading zeros
    state_feed['state_id'] = state_abbrv.lower() + state_feed['state_fips']

    # CLEAN/FORMAT state_feed, state_data, election_authorities, and target_smart (4 dataframes)
    state_feed, state_data, \
      election_authorities, target_smart = clean_data(state_abbrv, state_feed, state_data, election_authorities, target_smart)

    # GENERATE warnings for matches between counties/places/townships & precincts in state_data & target_smart (2 warnings)
    sd_unmatched_precincts_list, ts_unmatched_precincts_list = generate_warnings_mismatches(state_abbrv, state_data, target_smart)

    # _____________________________________________________________________________________________________________________

    # CREATE IDS | Create ids on dataframes

    # CREATE 'election_official_person_id'
    temp = election_authorities[['county', 'official_title']]
    temp.drop_duplicates(['county', 'official_title'], keep='first',inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['election_official_person_id'] = 'per' + (temp.index + 1).astype(str).str.zfill(4)
    election_authorities = pd.merge(election_authorities, temp, on =['county', 'official_title'])

    # CREATE 'election_adminstration_id'
    temp = election_authorities[['county']]
    temp.drop_duplicates(['county'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['election_administration_id'] = 'ea' + (temp.index + 1).astype(str).str.zfill(4)
    election_authorities = pd.merge(election_authorities, temp, on =['county'])
    election_authorities.drop_duplicates(subset=['election_administration_id'], inplace=True) # REMOVE all except 1st id entry for each location

    # CREATE 'hours_only_id'
    temp = state_data[['location_name', 'address_line', 'directions']]
    temp.drop_duplicates(['location_name', 'address_line', 'directions'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['hours_open_id'] = 'hours' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['location_name','address_line', 'directions'])

    # CREATE 'polling_location_ids'
    temp = state_data[['location_name', 'address_line', 'directions']]
    temp.drop_duplicates(['location_name', 'address_line', 'directions'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['polling_location_ids'] = 'pol' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['location_name','address_line', 'directions'])
    
    # _____________________________________________________________________________________________________________________

    # FILE CREATION | Generate files for dashboard zip

    # GENERATE 11 .txt files
    election = generate_election(state_feed, state_data)
    polling_location = generate_polling_location(state_data)
    schedule = generate_schedule(state_data, state_feed)
    source = generate_source(state_feed)
    state = generate_state(state_feed)
    locality = generate_locality(state_feed, state_data, election_authorities)
    election_administration = generate_election_administration(election_authorities)
    department = generate_department(election_authorities)
    person = generate_person(state_abbrv, election_authorities)
    precinct = generate_precinct(state_data, locality)
    street_segment = generate_street_segment(state_abbrv, target_smart, state_data, precinct)
  
    precinct.drop(['county'], axis=1, inplace=True) # DROP county from precinct after being passed to street_segment

    # GENERATE .txt file warnings (3 warnings)
    missing_precinct_ids_count, missing_house_numbers_count, \
      missing_street_suffixes_count = generate_warnings_txt(state_abbrv, street_segment)

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
    state_report(multi_directions_rows, multi_address_rows, cross_street_rows, # WARNINGS for state data
                 missing_data_rows, semi_colon_rows, date_year_rows, # FATAL ERRORS for state_data
                 missing_zipcode_rows, missing_state_abbrvs_rows, 
                 missing_counties_count, missing_townships_count, missing_state_count, # WARNINGS for target_smart
                 sd_unmatched_precincts_list, ts_unmatched_precincts_list, # WARNINGS for mismatches between state_data & target_smart
                 missing_precinct_ids_count, missing_house_numbers_count, missing_street_suffixes_count, # WARNINGS for .txt dataframes
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

    # FORMAT dates (3 formatted)
    state_feed['election_date'] = pd.to_datetime(state_feed['election_date'])
    state_data['start_date'] = pd.to_datetime(state_data['start_date'])
    state_data['end_date'] = pd.to_datetime(state_data['end_date'])
    
    # FORMAT hours (2 formatted)
    state_data['start_time'] = state_data['start_time'].str.replace(' ', '')
    state_data['start_time'] = state_data['start_time'].str.replace(';', ':')
    state_data['start_time'] = state_data['start_time'].str.replace('-',':00-')
    temp = state_data['start_time'].str.split('-', n=1, expand=True)
    temp[0] = temp[0].str.pad(8, side='left', fillchar='0')
    temp[1] = temp[1].str.pad(5, side='left', fillchar='0')
    state_data['start_time'] = temp[0] + '-' + temp[1]

    state_data['end_time'] = state_data['end_time'].str.replace(' ', '')
    state_data['end_time'] = state_data['end_time'].str.replace(';', ':')
    state_data['end_time'] = state_data['end_time'].str.replace('-',':00-')
    temp = state_data['end_time'].str.split('-', n=1, expand=True)
    temp[0] = temp[0].str.pad(8, side='left', fillchar='0')
    temp[1] = temp[1].str.pad(5, side='left', fillchar='0')
    state_data['end_time'] = temp[0] + '-' + temp[1]
    
    # FORMAT FIPS & ZIPS (2 formatted)
    state_feed['fips'] = state_feed['state_fips'].str.pad(5, side='right', fillchar='0')
    target_smart['vf_reg_zip'] = target_smart['vf_reg_zip'].astype(str)
    target_smart['vf_reg_zip'] = target_smart['vf_reg_zip'].str.pad(5, side='left', fillchar='0')

    # CREATE/FORMAT county (1 created, 2 formatted)
    state_data['county'] = state_data['county'].str.upper().str.strip()
    target_smart['vf_county_name'] = target_smart['vf_county_name'].str.upper().str.strip()
    election_authorities['ocd_division'] = election_authorities['ocd_division'].str.upper().str.strip()
    election_authorities['county'] = election_authorities['ocd_division'].str.extract('\\/\\w+\\:(\\w+\'?\\-?\\~?\\w+?)$', expand=False).str.replace('_', ' ').str.replace('~', "'")
    
    # FORMAT precinct (2 formatted)
    state_data['precinct'] = state_data['precinct'].str.strip().str.upper()
    target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].astype(str).str.strip().str.upper()

    # FORMAT voter file addresses (1 formatted)
    target_smart['vf_reg_address_1'] = target_smart['vf_reg_address_1'].str.upper().str.strip().str.replace('\"', '')
    target_smart['vf_reg_address_1'] = target_smart['vf_reg_address_1'].str.replace('\\s{2,}', ' ')

    # _____________________________________________________________________________________________________________________

    # ERROR HANDLING | Interventionist adjustments to account for state eccentricities and common hand collection mistakes, etc

    # FORMAT address line (1 formatted)
    # NOTE: A common outreach error was missing state abbreviations in the address line
    state_abbrv_with_space = state_feed['state_abbrv'][0].center(4, ' ')
    state_data['address_line'] = state_data['address_line'].str.replace(state_abbrv_with_space, ' ') \
                                                           .str.replace('[ ](?=[^ ]+$)', state_abbrv_with_space)
    
    # CONDITIONAL formattating for 2 states (NH, AZ) 
    if state_abbrv == 'NH':

        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace(' - ', ' ') # REMOVE dash in precinct names
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('"', '').str.replace("'", '') # REMOVE quotes in precinct names
        state_data['county'] = state_data['county'].str.replace("'", '') # REMOVE apostrophes in state_data `county` because target_smart does not incl. them 
        state_data['precinct'] = state_data['precinct'].str.replace("'", '')
        target_smart['vf_township'] = target_smart['vf_township'].str.upper() 

        """ NOTE: In New Hampshire, BERLIN WARD 02 in 2018 was merged with BERLIN WARD 03. 
        For future elections, TargetSmart may update the voterfile to reflect these changes and thus BERLIN WARD 02
        will no longer be an issue. It may be worth isolating how many records listing `BERLIN WARD 02` remain and 
        deleting this if-statement if 0 are identified. """
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('BERLIN WARD 02', 'BERLIN WARD 03')

    elif state_abbrv == 'AZ':

        # NOTE: TargetSmart lists some of the precincts in Arizona as 'PIMA' + #, but not all. The following removes the additional chars.
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('PIMA ', '')


    return state_feed, state_data, election_authorities, target_smart



def generate_election(state_feed, state_data):
    """
    PURPOSE: generates election dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: election dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/election.html
    """

    # SELECT feature(s)
    election = state_feed[['election_date','election_name','state_id']]
    
    # CREATE/FORMAT feature(s) (3 created, 2 formatted)
    election['id'] = 'ele01' 
    election['election_type'] = 'Federal'
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

    # SELECT feature(s) 
    polling_location = state_data[['polling_location_ids', 'location_name', 'address_line', 
                                    'directions', 'hours_open_id']] 

    # CREATE/FORMAT col(s) (2 formatted)                                  
    polling_location.rename(columns={'polling_location_ids':'id', 
                                     'location_name':'name'}, inplace=True)
    polling_location.drop_duplicates(inplace=True)


    return polling_location



def generate_schedule(state_data, state_feed):
    """
    PURPOSE: generates schedule dataframe for .txt file
    INPUT: state_feed
    RETURN: schedule dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/hours_open.html#schedule
    """ 

    # SELECT feature(s)
    schedule = state_data[['start_time', 'end_time', 'start_date', 'end_date', 'hours_open_id']]
    schedule.drop_duplicates(inplace=True)

    # CREATE feature(s) (1 created)
    schedule.reset_index(drop=True, inplace=True) # RESET index
    schedule['id'] = 'sch' + (schedule.index + 1).astype(str).str.zfill(4) # CREATE feature(s)


    return schedule



def generate_source(state_feed):
    """
    PURPOSE: generates source dataframe for .txt file
    INPUT: state_feed
    RETURN: source dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/source.html
    """ 
    
    # SELECT feature(s)
    source = state_feed[['fips']]

    # CREATE/FORMAT feature(s) (4 created, 1 formatted)
    source.reset_index(drop=True, inplace=True)
    source['id'] = 'src' + (source.index + 1).astype(str).str.zfill(4)
    source['date_time'] = datetime.datetime.now().replace(microsecond=0).isoformat() 
    source['name'] = 'Democracy Works Outreach Team'
    source['version'] = '5.1' # REFERENCES VIP SPEC
    source.rename(columns={'fips':'vip_id'}, inplace=True) # RENAME col(s)
    

    return source



def generate_state(state_feed): 
    """
    PURPOSE: generates state dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: state dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/state.html
    """ 

    # SELECT col(s)
    state = state_feed[['state_id','official_name']]

    # FORMAT feature(s) (2 formatted)
    state.rename(columns={'state_id':'id', 
                          'official_name':'name'}, inplace=True)


    return state




def generate_locality(state_feed, state_data, election_authorities):
    """
    PURPOSE: generates locality dataframe for .txt file
    INPUT: state_feed, state_data, election_authorities
    RETURN: locality dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/locality.html
    """ 

    # SELECT feature(s)
    locality = state_data[['county']]

    # MERGE locality with election_authorities
    election_admins = election_authorities[['county', 'election_administration_id']]
    locality = locality.merge(election_admins, on='county', how='left')
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows from merge 

    # CREATE/FORMAT feature(s) (2 created, 1 formatted)
    locality['state_id'] = state_feed['state_id'][0]
    locality.reset_index(drop=True, inplace=True)
    locality['id'] = 'loc' + (locality.index + 1).astype(str).str.zfill(4)
    locality.rename(columns={'county':'name'}, inplace=True)


    return locality



def generate_election_administration(election_authorities):
    """
    PURPOSE: generates election_administration dataframe for .txt file
    INPUT: election_authorities
    RETURN: election_administration dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/election_administration.html
    """

    
    election_administration = election_authorities[['election_administration_id', 'homepage_url']]

    # CREATE/FORMAT feature(s)
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

    # SELECT feature(s)
    department = election_authorities[['election_administration_id', 'election_official_person_id']]

    # CREATE feature(s) (1 created)
    department.drop_duplicates(inplace=True)
    department.reset_index(drop=True, inplace=True)
    department['id'] = 'dep' + (department.index + 1).astype(str).str.zfill(4)

    return department



def generate_person(state_abbrv, election_authorities):
    """
    PURPOSE: generates person dataframe for .txt file
    INPUT: election_authorities
    RETURN: person dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/person.html
    """

    # SELECT feature(s)
    person = election_authorities[['county', 'official_title', 'election_official_person_id']]

    # CREATE/FORMAT feature(s) (2 created, 1 formatted)
    person.drop_duplicates('election_official_person_id', keep='first',inplace=True)
    person['profession'] = 'ELECTION ADMINISTRATOR'
    person.rename(columns={'election_official_person_id':'id'}, inplace=True) 

    person['title'] = person['county'].str.upper() + ' ' + person['official_title'].str.upper()
    
    # REMOVE feature(s) (2 removed)
    person.drop(['county', 'official_title'], axis=1, inplace=True) 


    return person



def generate_precinct(state_data, locality):
    """
    PURPOSE: generates precinct dataframe for .txt file
    INPUT: state_data, locality
    RETURN: precinct dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/precinct.html
    """

    # SELECT feature(s)
    precinct = state_data[['precinct', 'polling_location_ids', 'county']]

    # MERGE precinct with locality
    temp = locality[['name', 'id']]
    precinct = precinct.merge(temp, left_on='county', right_on='name', how='left')

    # GROUP polling_location_ids
    precinct.drop_duplicates(inplace=True) 
    grouped = precinct.groupby(['county', 'id', 'precinct'])
    grouped = pd.DataFrame(grouped.aggregate(lambda x: ' '.join(x))['polling_location_ids']) # FLATTEN aggregated df
    precinct = grouped.reset_index()  

    # CREATE/FORMAT feature(s) (1 created, 2 formatted)
    precinct.drop_duplicates(inplace=True)
    precinct.rename(columns={'precinct':'name',
                             'id':'locality_id'}, inplace=True)
    precinct.reset_index(drop=True, inplace=True)
    precinct['id'] = 'pre' + (precinct.index + 1).astype(str).str.zfill(4)

    return precinct



def generate_street_segment(state_abbrv, target_smart, state_data, precinct):
    """
    PURPOSE: generates street_segment dataframe for .txt file
    INPUT: state_data, target_smart, precinct
    RETURN: street_segment dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/street_segment.html
    """    

    # SELECT feature(s)
    street_segment = target_smart[['vf_reg_address_1', 'vf_reg_address_2', 'vf_reg_city', 
                                   'vf_precinct_name', 'vf_reg_zip', 'vf_county_name', 'vf_township']]

    # REMOVE duplicate rows created by multiple registrants at the same address                           
    street_segment.drop_duplicates(inplace=True)

    # CREATE/FORMAT feature(s) (3 created, 5 formatted)
    street_segment['address_direction'] = street_segment['vf_reg_address_1'].str.extract('\\s+(N|E|S|W|NE|NW|SE|SW|NORTH|EAST|SOUTH|WEST)$') # EXTRACT last cardinal direction in the address
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('\\s+(N|E|S|W|NE|NW|SE|SW|NORTH|EAST|SOUTH|WEST)$', '') # REMOVE last cardinal direction in the address
    street_segment['street_direction'] = street_segment['vf_reg_address_1'].str.extract('\\s+(N|E|S|W|NE|NW|SE|SW)\\s+') # EXTRACT cardinal direction in the middle of the address
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('\\s+(N|E|S|W|NE|NW|SE|SW)\\s+', ' ') # REPLACE cardinal direction in the middle of the address

    street_segment['state'] = state_abbrv
    street_segment.rename(columns={'vf_reg_city':'city', 
                                   'vf_reg_address_2':'unit_number', 
                                   'vf_reg_zip':'zip'}, inplace=True)
    
    # CREATE/FORMAT feature(s) (1 created, 1 formatted) 
    unit_number_list = ['APT .*', 'APT.*', 'APT', 'APARTMENT .*', 'UNIT .*', 'UNIT', 'PMB .*', 'PO BOX .*', 'BSMT .*', 'BOX \\d+', 'BX .*', 
                        'LOT .*', 'LOT', 'RM # .*', 'RM .*', 'DEPT .*', 'PH .*', 'PH', 'STE .*', 'BLDG .*', 'TRLR \\d+', 'SLIP .*', 'FLOOR .*', 
                        'FL .*', 'OFC .*', 'OFC', 'FRNT .*', 'LBBY .*', 'LBBY', 'REAR .*', 'REAR', 'CONDO .*', 
                        'COLBY \\d+', 'TTU .*', 'ROOM .*', 'HOUSE \\d+']
    unit_number_regex = re.compile(r'\b(' + '|'.join(unit_number_list) + r')$\b', re.IGNORECASE)

    street_segment['unit_number_temp0'] = street_segment['vf_reg_address_1'].str.strip().str.extract('(CLASS OF \\d+|VU STA PMB \\d+)$')
    street_segment['unit_number_temp1'] = street_segment['vf_reg_address_1'].str.strip().str.extract(unit_number_regex) # EXTRACT unit number
    street_segment['unit_number_temp2'] = street_segment['vf_reg_address_1'].str.strip().str.extract('(#.*)$') # EXTRACT other unit number
    highway_name_list = ['HIGHWAY', 'HWY', 'RT ', 'RR ', ' SR', ' US ', 'OLD NO']
    street_segment_highway_or_not = lambda x: True if any(highway_name in x for highway_name in highway_name_list) == True else False
    street_segment['unit_number_temp3_flag'] = street_segment['vf_reg_address_1'].astype(str).apply(street_segment_highway_or_not)
    street_segment['unit_number_temp3'] = np.nan
    street_segment.loc[street_segment['unit_number_temp3_flag']==False, 'unit_number_temp3'] = street_segment.loc[street_segment['unit_number_temp3_flag']==False, 'vf_reg_address_1'].str.strip().str.extract('([0-9]*)$')
    
    street_segment['unit_number'] = street_segment['unit_number'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    street_segment['unit_number'].fillna(street_segment['unit_number_temp0'], inplace=True)
    street_segment['unit_number'].fillna(street_segment['unit_number_temp1'], inplace=True)
    street_segment['unit_number'].fillna(street_segment['unit_number_temp2'], inplace=True)
    street_segment['unit_number'].fillna(street_segment['unit_number_temp3'], inplace=True)

    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace(unit_number_regex, ' ') # REMOVE unit number from address
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('#.*$', '')
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].astype(str).apply(lambda x: x if "HIGHWAY" in x or "HWY" in x else re.sub('[0-9]*$', '', x))
    
    # C1 Street Suffix Abbreviations + extra
    # https://pe.usps.com/text/pub28/28apc_002.htm
    # GENERATE regex for street ending #'HIGHWAY .*', 'HWY .*', 
    street_suffix_list = ['STREET EXT', 'RD EXT', 'ROAD EXT', 'ST EXT', 'ST EXTENSION', 'DRIVE EXT', 'LANE EXT', 'AVENUE EXT', 'DR EXT', # NH has odd EXT edge cases 
                          'ALLEE', 'ALLEY', 'ALLY', 'ALY', 'ANNEX', 'ANNX', 'ANX', 'ARC', 'ARCADE', 'AV', 'AVE', 'AVEN', 'AVNUE', 'ACRES', # A
                          'BAY', 'BAYOO', 'BAYOU', 'BYU', 'BCH', 'BEACH', 'BEND', 'BND', 'BLF', 'BLUF', 'BLUFF', 'BLUFFS', 'BFLS', 'BOT', # B
                          'BTM', 'BOTTM', 'BOTTOM', 'BLVD', 'BOUL', 'BOULEVARD', 'BOULV', 'BR', 'BRNCH', 'BRANCH', 'BRDGE', 'BRG', 'BlFS',
                          'BRIDGE', 'BRK', 'BROOK', 'BROOKS', 'BRKS', 'BURG', 'BG', 'BURGS', 'BGS', 'BYP', 'BYPA', 'BYPAS', 'BYPASS', 'BYPS', 
                          'CAMP', 'CP', 'CR', 'CMP', 'CANYN', 'CANYON', 'CNYN', 'CYN', 'CAPE', 'CPE', 'CAUSEWAY', 'CAUSEWA', 'CSWY', 'CEN', # C
                          'CENT', 'CENTER', 'CENTR', 'CENTRE', 'CNTER', 'CNTR', 'CTR', 'CENTERS', 'CIR', 'CIRC', 'CIRCL', 'CIRCLE', 
                          'CRCL', 'CRCLE', 'CIRCLES', 'CLF', 'CLIFF', 'CLIFFS', 'CLFS', 'CLB', 'CLUB', 'COMMON', 'CMN', 'COMMONS', 
                          'CMNS', 'COR', 'CORNER', 'CORNERS', 'CORS', 'COURSE', 'CRSE', 'COURT', 'CT', 'COURTS', 'CTS', 'COVE', 
                          'CV', 'CVE', 'CREEK', 'CRK', 'CRESCENT', 'CRES', 'CRSENT', 'CRSNT', 'CREST', 'CRST', 'CROSSING', 'CRSSNG', 
                          'XING', 'CROSSROAD', 'XRD', 'CROSSROADS', 'XRDS', 'CURVE', 'CURV', 
                          'DALE', 'DL', 'DAM', 'DM', 'DIV', 'DIVIDE', 'DV', 'DVD', 'DR', 'DRIVE', 'DRIV', 'DRV', 'DRIVES', 'DRS', 'DOWN', 'DOWNS', # D
                          'EST', 'ESTATE', 'ESTATES', 'ESTS', 'EXP', 'EXPR', 'EXPRESS', 'EXPRESSWAY', 'EXPW', 'EXPY', 'EXT', # E
                          'EXTENSION', 'EXTN', 'EXTNSN', 'EXTS', 
                          'FALL', 'FALLS', 'FARM', 'FLS', 'FERRY', 'FRRY', 'FRY', 'FIELD', 'FLD', 'FIELDS', 'FLDS', 'FLAT', 'FLT', # F
                          'FLATS', 'FLTS', 'FORD', 'FRD', 'FORDS', 'FRDS', 'FOREST', 'FORESTS', 'FRST', 'FORG', 'FORGE', 'FRG', 'FIREROAD',
                          'FORGES', 'FRGS', 'FORK', 'FRK', 'FORKS', 'FRKS', 'FORT', 'FRT', 'FT', 'FREEWAY', 'FREEWY', 'FRWAY', 'FRWY', 'FWY',
                          'GARDEN', 'GARDN', 'GAP', 'GRDEN', 'GRDN', 'GDN', 'GARDENS', 'GDNS', 'GRDNS', 'GATEWAY', 'GATEWY', 'GATWAY', # G
                          'GTWAY', 'GTWY', 'GLEN', 'GLN', 'GLENS', 'GLNS', 'GREEN', 'GRADE', 'GRN', 'GREENS', 'GRNS', 'GROV', 'GROVE', 'GRV', 'GROVES', 
                          'HARB', 'HARBOR', 'HARBRB', 'HBR', 'HRBOR', 'HARBORS', 'HBRS', 'HAVEN', 'HVN', 'HT', 'HTS', 'HIGHWAY', # H
                          'HIGHWY', 'HIWAY', 'HWY', 'HIWY', 'HWAY', 'HILL', 'HL', 'HILLS', 'HLS', 'HLLW', 'HOLLOW', 'HOLLOWS', 'HOLW', 'HOLWS', 
                          'INLT', 'IS', 'ISLAND', 'ISLND', 'ISLANDS', 'ISLNDS', 'ISS', 'ISLE', 'ISLES', # I
                          'JCT', 'JCTION', 'JCTN', 'JUNCTION', 'JUNCTN', 'JUNCTON', 'JCTNS', 'JCSTS', 'JUNCTIONS', # J
                          'KEY', 'KY', 'KEYS', 'KYS', 'KNL', 'KNOL', 'KNOLL', 'KNLNS', 'KNOLLS', # K
                          'LK', 'LAKE', 'LKS', 'LAKES', 'LAND', 'LANDING', 'LNDG', 'LNDNG', 'LANE', 'LN', 'LGT', 'LIGHT', # L
                          'LIGHTS', 'LGHTS', 'LF', 'LOAF', 'LCK', 'LOCK', 'LDG', 'LDGE', 'LODG', 'LODGE', 'LOOP', 'LOOPS', 'LOCH',
                          'MALL', 'MNR', 'MANOR', 'MANORS', 'MNRS', 'MEADOW', 'MDW', 'MDW', 'MDWS', 'MEADOWS', 'MEDOWS', # M
                          'MEWS', 'MILL', 'ML', 'MILLS', 'MLS', 'MISSN', 'MSSN', 'MSN', 'MOTORWAY', 'MTWY', 'MNT', 'MT', 
                          'MOUNT', 'MNTAIN', 'MNTN', 'MOUNTAIN', 'MOUNTIN', 'MTN', 'MTIN', 'MNTNS', 'MOUNTAINS', 
                          'NCK', 'NECK', 'NOOK', # N
                          'ORCH', 'ORCHARD', 'ORCHRD', 'OVAL', 'OVL', 'OVERPASS', 'OPAS', 'OPASS', 'OVERLOOK', # O
                          'PARK', 'PRK', 'PARKS', 'PRKS', 'PARKWAY', 'PARKWY', 'PKWAY', 'PKWY', 'PKY', 'PK', 'PARKWAYS', # P
                          'PKWYS', 'PASS', 'PASSAGE', 'PSGE', 'PATH', 'PATHS', 'PIKE', 'PIKES', 'PINE', 'PNE', 'PIER',
                          'PINES', 'PNES', 'PL', 'PLAIN', 'PLN', 'PLAINS', 'PLNS', 'PLAZA', 'PLZ', 'PLZA', 'POINT', 'POINTE',  
                          'PT', 'POINTS', 'PTS', 'PORT', 'PRT', 'PORTS', 'PRTS', 'PR', 'PRARIE', 'PRR', 'PROMENADE', 
                          'RAD', 'RADIAL', 'RADL', 'RADIEL', 'RADL', 'RAMP', 'RANCH', 'RANCHES', 'RNCH', 'RNCHS', # R
                          'RAPID', 'RPD', 'RAPIDS', 'RPDS', 'REST', 'RST', 'RDG', 'RIDGE', 'RDGE', 'RDGS', 'RIDGES', 
                          'RIV', 'RIVER', 'RVR', 'RIVR', 'RD', 'ROAD', 'RD', 'ROADS', 'RDS', 'ROUTE', 'ROW', 'RUE', 'RUN', 
                          'SHL', 'SHOAL', 'SHLS', 'SHOALS', 'SHOAR', 'SHORE', 'SHR', 'SHOARS', 'SHORES', 'SHRS', 'SKYWAY', # S
                          'SKWY', 'SPG', 'SPNG', 'SPRING', 'SPRNG', 'SPGS', 'SPNGS', 'SPRINGS', 'SPRNGS', 'SPUR', 'SPURS', 
                          'SQ', 'SQR', 'SQRE', 'SQU', 'SQUARE', 'SQRS', 'SQUARES', 'SQS', 'STA', 'STATION', 'STATN', 'STN', 
                          'STRA', 'STRAV', 'STRAVEN', 'STRAVENUE', 'STREAM', 'STREME' 'STRM', 'STREET', 'STRT', 'ST', 'STR', 
                          'STREETS', 'STS', 'SMT', 'SUMIT', 'SUMITT', 'SUMMIT', 'SPEEDWAY',
                          'TER', 'TERR', 'TERRACE', 'THROUGHWAY', 'TRWY', 'TRACE', 'TRACES', 'TRCE', 'TRACK', 'TRACKS', # T
                          'TRAK', 'TRK', 'TRKS', 'TRAFFICWAY', 'TRFY', 'TRAIL', 'TRAILS', 'TRL', 'TRLS', 'TRAILER', 'TRLR', 
                          'TRLRS', 'TUNEL', 'TUNL', 'TUNLS', 'TUNNEL', 'TUNNELS', 'TUNNL', 'TRNPK', 'TURNPIKE', 'TPKE', 'TURNPK', 'TURN', 'TR',
                          'UNDERPASS', 'UPAS', 'UN', 'UNION', 'UNIONS', 'UNS', # U
                          'VALLEY', 'VALLY', 'VLLY', 'VLY', 'VALLEYS', 'VLYS', 'VDCT', 'VIA', 'VIADCT', 'VIADUCT', 'VIEW', 'VW', # V
                          'VIEWS', 'VWS', 'VILL', 'VILLAG', 'VILLAGE', 'VILLG', 'VILLIAGE', 'VLG', 'VILLE', 'VL', 'VIS', 'VIST', 
                          'VISTA', 'VST', 'VSTA', 
                          'WALK', 'WALKS', 'WALL', 'WAY', 'WY', 'WAYS', 'WELL', 'WL', 'WELLS', 'WLS', 'WHARF', 'WOODS'] # W
    street_suffix_regex = re.compile(r'\b(' + '|'.join(street_suffix_list) + r')$\b', re.IGNORECASE)
    
    # CREATE feature(s) (7 created)
    street_segment['street_suffix'] = street_segment['vf_reg_address_1'].str.strip().str.extract(street_suffix_regex, expand=True)
    street_segment['temp'] = street_segment['vf_reg_address_1'].str.strip().str.extract('^(\\d+\\s+\\d+/\\d+|\\d+\\w+|[A-Z]{1}\\d+|\\d+)', expand=False).astype(str).str.replace('nan', '')
    street_segment['house_number'] = street_segment['temp'].str.extract('^(\\d+)')
    street_segment['start_house_number'] = street_segment['house_number']
    street_segment['end_house_number'] = street_segment['house_number']
    street_segment['odd_even_both'] = 'both'
    street_segment['temp'] = street_segment['vf_reg_address_1'].str.strip().str.replace(street_suffix_regex, '')
    street_segment['street_name'] = street_segment['temp'].str.replace('^(\\d+\\s+\\d+/\\d+|\\d+\\w+|[A-Z]{1}\\d+|\\d+)', '').str.strip()

    # REMOVE feature(s) (4 removed)
    street_segment.drop(['unit_number_temp0', 'unit_number_temp1', 'unit_number_temp2', 'unit_number_temp3', 'unit_number_temp3_flag',
                         'vf_reg_address_1', 'temp', 'house_number'], axis=1, inplace=True)
    street_segment.drop_duplicates(inplace=True) 

    # IDENTIFY if there are any vote centers
    state_data['flag'] = state_data['precinct'].str.contains('VOTE CENTER')
    list_vote_center_counties = state_data[state_data['flag']==True] # FILTER to identify counties w/ vote centers
    list_vote_center_counties = list_vote_center_counties['county'].drop_duplicates().tolist()

    if list_vote_center_counties: # IF there are vote centers

        # SPLIT street_segment dataframe 
        slice_vote_center = street_segment[street_segment['vf_county_name'].isin(list_vote_center_counties)]
        slice_precinct = street_segment[~street_segment['vf_county_name'].isin(list_vote_center_counties)]
        
        if not slice_vote_center.empty: # IF vote centers in state data are present in TargetSmart data
            temp = precinct[['id', 'county']]
            slice_vote_center = slice_vote_center.merge(temp, how='left', left_on=['vf_county_name'], right_on=['county'])
        else:
            print('Counties with vote centers are not present in TargetSmart Data', list_vote_center_counties)
        
        # MERGE street_segment with state_data
        temp = state_data[['precinct', 'county']]
        temp.drop_duplicates(inplace=True)
        slice_precinct = slice_precinct.merge(temp, left_on=['vf_precinct_name', 'vf_county_name'], right_on=['precinct', 'county'], how='left')

        # NOTE: HARDCODED Values for Osceola County, FL
        slice_precinct.loc[(slice_precinct['street_name'] == 'MASSACHUSETTS') & \
                               (slice_precinct['street_suffix'] == 'AVE') & \
                               (slice_precinct['zip'] == '34769') & \
                               (slice_precinct['end_house_number'].isin(['1800', '1806', '1898'])) & \
                               (slice_precinct['city'] == 'SAINT CLOUD'), 'vf_precinct_name'] = '523'
        
        # MERGE street_segment with precinct
        temp = precinct[['name', 'id', 'county']]
        slice_precinct = slice_precinct.merge(temp, how='left', left_on=['vf_precinct_name', 'county'], right_on=['name', 'county'])

        # COMBINE vote center slice and precinct slice
        street_segment = pd.concat([slice_vote_center, slice_precinct])

    else: # IF there are no vote centers

        # MERGE street_segment with state_data
        temp = state_data[['precinct', 'county']]
        temp.drop_duplicates(inplace=True)
        if state_abbrv in STATES_USING_TOWNSHIPS:

            # NOTE: Since NH uses township instead of county to group precincts. The following subsittution accounts for this difference
            list_unique_hc_towns = temp['county'].unique().tolist()
            street_segment['vf_county_name'] = street_segment['city'].apply(lambda x: np.nan if x not in list_unique_hc_towns else x)
            street_segment['vf_county_name'].fillna(street_segment['vf_township'], inplace=True)

            # NOTE: there is weird geography issue in Washington county and stoddard precinct for VALLEY RD addresses
            street_segment.loc[(street_segment['street_name'] == 'VALLEY') & \
                               (street_segment['street_suffix'] == 'RD') & \
                               (street_segment['zip'] == '03280') & \
                               (street_segment['city'] == 'WASHINGTON'), 'vf_precinct_name'] = 'WASHINGTON'
        street_segment = street_segment.merge(temp, left_on=['vf_precinct_name', 'vf_county_name'], right_on=['precinct', 'county'], how='left')

        # MERGE street_segment with precinct
        temp = precinct[['name', 'id', 'county']]
        street_segment = street_segment.merge(temp, how='left', left_on=['vf_precinct_name', 'county'], right_on=['name', 'county'])

    # REMOVE feature(s) (6 removed)
    street_segment.drop(['vf_precinct_name', 'precinct', 'name', 'county', 'vf_county_name', 'vf_township'], axis=1, inplace=True)

    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    street_segment.rename(columns={'id':'precinct_id'}, inplace=True)
    street_segment.drop_duplicates(inplace=True)
    street_segment.reset_index(drop=True, inplace=True) # RESET index
    street_segment['id'] = 'ss' + (street_segment.index + 1).astype(str).str.zfill(9) 


    return street_segment



def generate_zip(state_abbrv, state_feed, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt, precinct.txt, street_segment.txt)
    INPUT: state_abbrv, state_feed, files
    RETURN: exports zip of 11 .txt files
    """

    # WRITE dataframes to txt files
    file_list = []
    for name, df in files.items():

        file = name + '.txt'
        file_list.append(file)
        df.to_csv(file, index=False, encoding='utf-8')

    # CREATE state directory
    if not os.path.exists(state_abbrv):
        os.makedirs(state_abbrv)
    
    # DEFINE name of zipfile
    zip_filename = 'vipfeed-pp-' + str(state_feed['election_date'][0].date()) + '-' + state_abbrv + '.zip'

    # WRITE files to a zipfile
    with ZipFile(zip_filename, 'w') as zip:
        for file in file_list:
            zip.write(file)
            os.rename(file, os.path.join(state_abbrv, file))


    return 



###########################################################################################################################
# END OF VIP BUILD FUNCTION DEFINITIONS ###################################################################################
###########################################################################################################################



def generate_warnings_state_data(state_abbrv, state_data):
    """
    PURPOSE: isolate which rows, if any, have warnings or fatal errors in state_data
    INPUT: state_data
    RETURN: multi_directions_rows, multi_address_rows, cross_street_rows, 
            missing_data_rows, semi_colon_rows, date_year_rows, 
            missing_zipcode_rows, missing_state_abbrvs_rows
    """

    # GENERATE warnings (2 warnings)
    multi_directions_rows = warning_multi_directions(state_data)
    multi_address_rows = warning_multi_addresses(state_data)
    cross_street_rows = warning_cross_street(state_data)

    # GENERATE fatal errors (3 fatal errors)
    missing_data_rows = warning_missing_data(state_data)
    date_year_rows = warning_date_year(state_data)
    semi_colon_rows = warning_semi_colon(state_data)
    missing_zipcode_rows = warning_missing_zipcodes(state_data)
    missing_state_abbrvs_rows = warning_missing_state_abbrvs(state_abbrv, state_data)

    return     multi_directions_rows, multi_address_rows, cross_street_rows, \
               missing_data_rows, semi_colon_rows, date_year_rows, \
               missing_zipcode_rows, missing_state_abbrvs_rows 



def warning_multi_directions(state_data):
    """
    PURPOSE: isolate which polling locations, if any, have multiple directions
             (warning: each unique set of directions is considered a polling location)
    INPUT: state_data
    RETURN: multi_directions_rows
    """

    # SELECT feature(s) (4 selected)
    unique_rows = state_data[['county', 'location_name', 'address_line', 'directions']].drop_duplicates()
    duplicate_locations = unique_rows[unique_rows.duplicated(subset=['county', 'location_name', 'address_line'],keep=False)]
    duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index

    multi_directions_rows = []
    if not duplicate_locations.empty: # IF the dataframe is not empty
        multi_directions_rows = sorted([tuple(x) for x in duplicate_locations.groupby(['county', 'location_name', 'address_line']).groups.values()])


    return multi_directions_rows



def warning_multi_addresses(state_data):
    """
    PURPOSE: isolate which polling locations (county/precinct pair), if any, have multiple addresses
             (warning: each unique set of addresses is considered a polling location)
    INPUT: state_data
    RETURN: multi_address_rows
    """

    # SELECT feature(s) (4 selected)
    addresses = state_data[['county','location_name', 'address_line']].drop_duplicates()
    multi_addresses = addresses[addresses.duplicated(subset=['county','location_name'], keep=False)]
    multi_addresses.index = multi_addresses.index + 1  # INCREASE INDEX to correspond with google sheets index

    multi_address_rows = []
    if not multi_addresses.empty: # IF the dataframe is not empty
        multi_address_rows = sorted([tuple(x) for x in multi_addresses.groupby(['county', 'location_name']).groups.values()])


    return multi_address_rows



def warning_cross_street(state_data):
    """
    PURPOSE: isolate which rows, if any, have invalid cross street (e.g. 1st & Main St)
    INPUT: state_data
    RETURN: cross_street_rows
    """

    # NOTE: invalid cross streets sometimes do not map well on Google's end 
    cross_street_addresses = state_data[state_data['address_line'].str.contains(' & | and ')]
    cross_street_rows = list(cross_street_addresses.index + 1)


    return cross_street_rows



def warning_missing_data(state_data):
    """
    PURPOSE: isolate which rows, if any, are missing data in the state data 
    INPUT: state_data
    RETURN: missing_data_rows
    """

    missing_data_check = state_data[state_data.columns.difference(['directions', 'start_time', 'end_time', 
                                                                   'internal_notes', 'collected_precinct'])].isnull().any(axis=1)
    missing_data_check.index = missing_data_check.index + 1  # INCREASE INDEX to correspond with google sheets index
    missing_data_rows = []

    if missing_data_check.any(): # IF any data is missing
        missing_data_rows = missing_data_check.loc[lambda x: x==True].index.values.tolist()
        if len(missing_data_check) > 30: # IF there are more than 30 rows with missing data then simply notify user
            missing_data_rows = ['More than 30 rows with missing data']


    return missing_data_rows



def warning_date_year(state_data): 
    """
    PURPOSE: isolate which rows, if any, have a start_date or end_date outside of the election year
    INPUT: state_data
    RETURN: date_year_rows
    """

    # FORMAT features (2 formatted)
    state_data['start_date'] = pd.to_datetime(state_data['start_date'])
    state_data['end_date'] = pd.to_datetime(state_data['end_date'])
    
    # ISOLATE data errors in 2 features
    incorrect_start_dates = state_data[state_data['start_date'].dt.year != ELECTION_YEAR]
    incorrect_end_dates = state_data[state_data['end_date'].dt.year != ELECTION_YEAR]
    incorrect_dates = incorrect_start_dates.append(incorrect_end_dates)

    date_year_rows = list(set(incorrect_dates.index + 1))


    return date_year_rows



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

    semi_colon_rows = list(set(semi_colon_times.index + 1))


    return semi_colon_rows



def warning_missing_zipcodes(state_data):
    """
    PURPOSE: isolate which rows, if any, are missing zip codes in the `address_line` col in state data 
    INPUT: state_data
    RETURN: missing_zipcode_rows
    """

    # SELECT feature(s) (1 feature)
    missing_zipcodes = state_data[['address_line']]
    missing_zipcodes = missing_zipcodes[~missing_zipcodes['address_line'].str.strip().str.contains('[0-9]{5}$')]
    missing_zipcode_rows  = list(set(missing_zipcodes.index + 1)) # ADD 1 to index to correspond with Google Sheets
   
    return missing_zipcode_rows



def warning_missing_state_abbrvs(state_abbrv, state_data):
    """
    PURPOSE: isolate which rows, if any, are missing state abbreviations in the `address_line` col in state data 
    INPUT: state_data
    RETURN: missing_state_abbrvs_rows
    """
    
    # SELECT feature(s) (1 feature)
    missing_state_abbrvs = state_data[['address_line']]
    state_abbrv_with_space = ' ' + state_abbrv + ' ' # CREATE state_abbrv with space
    missing_state_abbrvs = missing_state_abbrvs[~missing_state_abbrvs['address_line'].str.upper().str.contains(state_abbrv_with_space)]
    missing_state_abbrvs_rows = list(set(missing_state_abbrvs.index + 1)) # ADD 1 to index to correspond with Google Sheets

    return missing_state_abbrvs_rows  



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
    PURPOSE: count how many rows are missing state data & fill it in
    INPUT: missing_state_count
    RETURN: 
    """
    target_smart['vf_source_state'] = target_smart['vf_source_state'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    missing_state = target_smart[target_smart['vf_source_state'].isnull()]
    missing_state_count = len(missing_state)

    if not missing_state.empty: # IF there are rows missing state data
        street_segment['vf_source_state'].fillna(state_abbrv, inplace=True)
    

    return missing_state_count



###########################################################################################################################
# END OF TARGET_SMART WARNING FUNCTION DEFINITIONS ########################################################################
###########################################################################################################################



def generate_warnings_mismatches(state_abbrv, state_data, target_smart):
    """
    PURPOSE: isolate matching issues between state_data and target_smart
    INPUT: state_abbrv, state_data, target_smart
    RETURN: sd_unmatched_precincts, ts_unmatched_precincts
    """

    # GENERATE warnings (2 warnings)
    sd_unmatched_precincts_list, ts_unmatched_precincts_list = warning_unmatched_precincts(state_abbrv, state_data, target_smart)


    return sd_unmatched_precincts_list, ts_unmatched_precincts_list



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

    # OPTIONAL: Export a csv of the rows with missing precinct_ids
    missing_precinct_ids_filename = 'PP_' + state_abbrv + '_missing_precinct_ids.csv'
    missing_precinct_ids.to_csv(missing_precinct_ids_filename, index=False)

    return missing_precinct_ids_count



def warning_missing_house_numbers(state_abbrv, street_segment):
    """
    PURPOSE: isolate the number of addresses missing a house number
    INPUT: street_segment
    RETURN: missing_precinct_ids_count
    """

    # COUNT the number of rows in street_segment that are missing a house number
    missing_house_numbers = street_segment[street_segment['start_house_number'].isnull()]
    missing_house_numbers_count = len(missing_house_numbers)

    # OPTIONAL: Export a csv of the rows with missing house numbers
    missing_house_numbers_filename = 'PP_' + state_abbrv + '_missing_house_numbers.csv'
    missing_house_numbers.to_csv(missing_house_numbers_filename, index=False)


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

    # OPTIONAL: Export a csv of the rows with missing street suffixes
    missing_street_suffixes_filename = 'PP_' + state_abbrv + '_missing_street_suffixes.csv'
    missing_street_suffixes.to_csv(missing_street_suffixes_filename, index=False)

    return missing_street_suffixes_count




###########################################################################################################################
# END OF WARNING FUNCTION DEFINITIONS #####################################################################################
###########################################################################################################################



def state_report(multi_directions_rows, multi_address_rows, cross_street_rows, # WARNINGS for state data
                 missing_data_rows, semi_colon_rows, date_year_rows, # FATAL ERRORS for state_data
                 missing_zipcode_rows, missing_state_abbrvs_rows, 
                 missing_counties_count, missing_townships_count, missing_state_count, # WARNINGS for target_smart
                 sd_unmatched_precincts_list, ts_unmatched_precincts_list, # WARNINGS for mismatches between state_data & target_smart
                 missing_precinct_ids_count, missing_house_numbers_count, missing_street_suffixes_count, # WARNINGS for .txt dataframes
                 state_abbrv, state_feed, state_data, election_authorities, target_smart, # MAIN dataframes
                 files):
    """
    PURPOSE: print state report (general descriptive stats, warnings, and fatal errors)
    INPUT: multi_directions_rows, multi_address_rows, cross_street_rows, # WARNINGS for state data
           missing_data_rows, semi_colon_rows, date_year_rows, missing_zipcode_rows, # FATAL ERRORS for state_data
           missing_counties_count, missing_townships_count, missing_state_count, # WARNINGS for target_smart
           sd_unmatched_precincts_list, ts_unmatched_precincts_list, # WARNINGS for mismatches between state_data & target_smart
           missing_precinct_ids_count, missing_house_numbers_count, missing_street_suffixes_count, # WARNINGS for .txt dataframes
           state_abbrv, state_feed, state_data, election_authorities, target_smart, # MAIN dataframes
           files
    RETURN: 
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
    ea = election_authorities[['county']]
    ea.drop_duplicates(inplace=True)
    sd = state_data[['county']]
    sd.drop_duplicates(inplace=True)
    if state_abbrv in STATES_USING_TOWNSHIPS: # PROCESS states that use townships
        ts = target_smart[['vf_township']]
    else: # PROCESS states that use counties/places
        ts = target_smart[['vf_county_name']]
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

    
    # WARNINGS | Print warnings (less severe to critical)


    if multi_directions_rows or multi_address_rows or cross_street_rows or \
       missing_data_rows or semi_colon_rows or date_year_rows or \
       missing_zipcode_rows or missing_state_abbrvs_rows or \
       (missing_counties_count > 0) or (missing_townships_count > 0) or (missing_state_count > 0) or \
       sd_unmatched_precincts_list or ts_unmatched_precincts_list or \
       (missing_precinct_ids_count > 0) or (missing_house_numbers_count > 0) or (missing_street_suffixes_count > 0): 

        STATES_WITH_WARNINGS.append(state_abbrv) # STORE states with warnings

    if multi_directions_rows or multi_address_rows or cross_street_rows:      
        print('\n'*2)
        print('------------------ STATE DATA WARNINGS ------------------'.center(PRINT_OUTPUT_WIDTH, ' '))

        if multi_directions_rows:
            print('\n')
            print('Multiple Directions for the Same Polling Location'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(multi_directions_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

        if multi_address_rows:
            print('\n')
            print('Multiple Addresses for the Same Polling Location'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(multi_address_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))
           
        if cross_street_rows:
            print('\n')
            print('Problematic Cross-Street Formats'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(cross_street_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))
    

    if missing_data_rows or semi_colon_rows or date_year_rows or missing_zipcode_rows or missing_state_abbrvs_rows:
        print('\n')
        print('---------------- STATE DATA FATAL ERRORS ----------------'.center(PRINT_OUTPUT_WIDTH, ' '))

        if missing_data_rows:
            print('\n')
            print('Missing Data'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_data_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

        if semi_colon_rows:
            print('\n')
            print('Hours have ;\'s Instead of :\'s'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(semi_colon_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

        if date_year_rows:
            print('\n')
            print('Dates have Invalid Years in'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(date_year_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

        if missing_zipcode_rows:
            print('\n')
            print('Missing Zipcodes from Location Addresses'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_zipcode_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))
            
        if missing_state_abbrvs_rows:
            print('\n')
            print('Missing State Abbreviations from Location Addresses'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_state_abbrvs_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))


    if state_abbrv in STATES_USING_TOWNSHIPS: # PROCESS states that use townships
        if (missing_townships_count > 0) or (missing_state_count > 0):
            print('\n'*2)
            print('----------------- TARGET SMART WARNINGS -----------------'.center(PRINT_OUTPUT_WIDTH, ' '))

            if missing_townships_count > 0:
                    print('\n')
                    print('# of Rows Missing Townships'.center(PRINT_OUTPUT_WIDTH, ' '))
                    print()
                    print(str(missing_townships_count).center(PRINT_OUTPUT_WIDTH, ' '))

            if missing_state_count > 0:
                print('\n')
                print('# of Missing State Abbreviations Filled'.center(PRINT_OUTPUT_WIDTH, ' '))
                print()
                print(str(missing_state_count).center(PRINT_OUTPUT_WIDTH, ' '))

    else: # PROCESS states that use counties
        if (missing_counties_count > 0) or (missing_state_count > 0):
            print('\n'*2)
            print('----------------- TARGET SMART WARNINGS -----------------'.center(PRINT_OUTPUT_WIDTH, ' '))

            if missing_counties_count > 0:
                print('\n')
                print('# of Rows Missing Counties'.center(PRINT_OUTPUT_WIDTH, ' '))
                print()
                print(str(missing_counties_count).center(PRINT_OUTPUT_WIDTH, ' '))

            if missing_state_count > 0:
                print('\n')
                print('# of Missing State Abbreviations Filled'.center(PRINT_OUTPUT_WIDTH, ' '))
                print()
                print(str(missing_state_count).center(PRINT_OUTPUT_WIDTH, ' '))


    if sd_unmatched_precincts_list or ts_unmatched_precincts_list:      
        print('\n'*2)
        print('------------------- MISMATCH WARNINGS -------------------'.center(PRINT_OUTPUT_WIDTH, ' '))

        if sd_unmatched_precincts_list:
            print('\n')
            print('Precincts in State Data not found in TargetSmart'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            for i in range(len(sd_unmatched_precincts_list) // 2 + 1):
                print (str(sd_unmatched_precincts_list[i * 2:(i + 1) * 2]).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

        if ts_unmatched_precincts_list:
            print('\n')
            print('Precincts in TargetSmart not found in State Data'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            for i in range(len(ts_unmatched_precincts_list) // 2 + 1):
                print (str(ts_unmatched_precincts_list[i * 2:(i + 1) * 2]).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))
    

    if (missing_precinct_ids_count > 0) or (missing_house_numbers_count > 0) or (missing_street_suffixes_count > 0):
        print('\n'*2)
        print('---------------- STREET SEGMENT WARNINGS ----------------'.center(PRINT_OUTPUT_WIDTH, ' '))

        if missing_precinct_ids_count > 0:
            print('\n')
            print('# of Rows Missing Precinct Ids'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_precinct_ids_count).center(PRINT_OUTPUT_WIDTH, ' '))

        if missing_house_numbers_count > 0:
            print('\n')
            print('# of Rows Missing Address Numbers'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_house_numbers_count).center(PRINT_OUTPUT_WIDTH, ' '))

        if missing_street_suffixes_count > 0:
            print('\n')
            print('# of Rows Missing Street Suffixes'.center(PRINT_OUTPUT_WIDTH, ' '))
            print()
            print(str(missing_street_suffixes_count).center(PRINT_OUTPUT_WIDTH, ' '))

        
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
        print(str(states_failed_to_load).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
        
    if states_failed_to_process:
        print('\n'*1)
        print('States that failed to process'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(states_failed_to_process).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))

    if STATES_WITH_WARNINGS:      
        print('\n'*1)
        print('States that processed with warnings'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(STATES_WITH_WARNINGS).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
        
    if states_successfully_processed:
        print('\n'*1)
        print('States that sucessfully processed'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(states_successfully_processed).strip('[]').replace('\'', '').center(PRINT_OUTPUT_WIDTH, ' '))
    
    print('\n'*3)


    return


###########################################################################################################################
# END OF REPORT RELATED DEFINITIONS #######################################################################################
###########################################################################################################################


if __name__ == '__main__':
    

    # SET UP command line inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('-states', nargs='+')

    # PRINT timestamp to help user guage processing times
    print('Timestamp:', datetime.datetime.now().replace(microsecond=0))

    # _____________________________________________________________________________________________________________________

    # SET UP Google API credentials
    # REQUIRES a local 'token.json' file & 'credentials.json' file
    # https://developers.google.com/sheets/api/quickstart/python
    
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))
    
    try: 
        # LOAD state feed data
        state_feed_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, 
                                                                range='STATE_FEED').execute()
        state_feed_values = state_feed_result.get('values', [])
    except:
        print('Error | STATE_FEED Google Sheet is either missing from the workbook or there is data reading error.')
        raise

    try: 
        # LOAD election authorities data
        election_authorities_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_EA_ID, 
                                                                          range='ELECTION_AUTHORITIES').execute()
        election_authorities_values = election_authorities_result.get('values', [])
    except:
        print('Error | ELECTION_AUTHORITIES Google Sheet is either missing from the workbook or there is data reading error.')
        raise

    # _____________________________________________________________________________________________________________________

    # ESTABLISH connection to AWS MySQL database

    try:
        # CONNECT to AWS RDS MySQL database
        targetsmart_creds = open('targetsmart_credentials.json', 'r')
        conn_string = json.load(targetsmart_creds)
        conn = mysql.connector.connect(**conn_string)
    except:
        print('Error | There is a database connection error.')

    # SET list of vars selected in MySQL query
    targetsmart_sql_col_list = ['vf_precinct_name', 'vf_reg_address_1', 'vf_reg_address_2', 'vf_reg_city', 
                                'vf_source_state', 'vf_reg_zip', 'vf_county_name', 'vf_township']
    targetsmart_sql_col_string = ', '.join(targetsmart_sql_col_list)

    # SET list of fields to filter out if they are empty in TargetSmart
    targetsmart_sql_filter_string = " WHERE vf_reg_city <> '' AND vf_precinct_name <> '' AND \
                                     vf_reg_address_1 <> '' AND vf_reg_zip <> ''"

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
                state_data = pd.DataFrame(state_data_values[0:], columns=state_data_values[0])
                state_data.drop([0], inplace=True)
                
                # FILTER dataframes for selected state (2 dataframes)
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state_abbrv] 
                election_authorities = election_authorities_all[election_authorities_all['state'] == state_abbrv] 

                state_feed.reset_index(inplace=True) # RESET index
                sql_table_name = state_feed['official_name'].str.lower().str.replace(' ', '') # CREATE MYSQL table name (format: full name, lowercase, no spaces)


                try: # LOAD TargetSmart data for a single state 
                                                    

                    curs = conn.cursor() # OPEN database connection

                    # SET MySQL query
                    if state_abbrv == 'AZ':
                        
                        query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name[0] + \
                                targetsmart_sql_filter_string + " AND vf_county_name = 'PIMA';"

                    elif state_abbrv == 'FL':
                        query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name[0] + \
                                targetsmart_sql_filter_string + " AND vf_county_name IN ('OSCEOLA', 'BAY', 'ST LUCIE');"
                    else: 
                        query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name[0] + targetsmart_sql_filter_string + ";"

                    curs.execute(query) # EXECUTE MySQL query

                    target_smart_values = curs.fetchall() # LOAD in data

                    curs.close() # CLOSE cursor object
                    conn.rollback() # REFRESH database connection
                    
                    # GENERATE target_smart dataframe
                    target_smart = pd.DataFrame(list(target_smart_values), columns=targetsmart_sql_col_list, dtype='object')

                    # REMOVE headers in TargetSmart dataframe
                    # NOTE: Occasionaly TargetSmart includes a header as a row (specifically for MT & ME)
                    target_smart = target_smart[target_smart['vf_precinct_name'] != 'vf_precinct_name'] 

                except:
                    print('ERROR | TargetSmart data for', state_abbrv, 'is either missing from the database or there is data reading error.')


                # GENERATE zip file and print state report
                vip_build(state_abbrv, state_feed, state_data, election_authorities, target_smart)
                

                states_successfully_processed.append(state_abbrv)
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


    print('Timestamp:', datetime.datetime.now().replace(microsecond=0))

