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

pd.set_option('display.max_columns', None)  # or 1000
pd.set_option('display.max_rows', None)  # or 1000


# PRO-TIP: if modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# SET Google Spreadsheet IDs (2 IDs)

# states & STATE_FEED
# https://docs.google.com/spreadsheets/d/1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M/edit#gid=2009403147
SPREADSHEET_ID = '1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M' 

# ELECTION_AUTHORITIES
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o' 


def vip_build(state_data, state_feed, election_authorities, target_smart):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 11 .txt files
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt, precinct.txt, street_segment.txt)
    INPUT: state_data, state_feed, election_authorities, target_smart
    RETURN: None
    """
    
    # CREATE/FORMAT feature(s) referenced by multiple .txts (6 created, 1 formatted)

    #state_feed.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # first make sure there are leading zeros
    state_feed['state_id'] = state_feed['state_abbrv'].str.lower() + state_feed['state_fips']

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
    

    # GENERATE 11 .txt files
    election = generate_election(state_feed, state_data)
    polling_location = generate_polling_location(state_data)
    schedule = generate_schedule(state_data, state_feed)
    source = generate_source(state_feed)
    state = generate_state(state_feed)
    locality = generate_locality(state_feed, state_data, election_authorities)
    election_administration = generate_election_administration(election_authorities)
    department = generate_department(election_authorities)
    person = generate_person(state_feed['state_abbrv'][0], election_authorities)
    precinct = generate_precinct(state_data, locality)
    street_segment = generate_street_segment(state_feed['state_abbrv'][0], target_smart, state_data, precinct)
  
    precinct.drop(['county'], axis=1, inplace=True) # DROP county from precinct after being passed to street_segment


    # GENERATE zip file
    generate_zip(state_feed['state_abbrv'][0],   {'election': election,
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
    
    
    # PRINT report on OCD IDs for the state being processed 

    # CREATE dataframes of unique OCD IDs for election authorities and state data
    ea = election_authorities[['county']]
    ea.drop_duplicates(inplace=True)
    sd = state_data[['county']]
    sd.drop_duplicates(inplace=True)
    if state_feed['state_abbrv'][0] == 'NH':
        ts = target_smart[['vf_township']]
    else:
        ts = target_smart[['vf_county_name']]
    ts.drop_duplicates(inplace=True)
    
    # PRINT count of unique OCD IDs
    print()
    print(f"{'Unique counties/places listed in state_data':53} | {len(sd)}")
    print(f"{'Unique counties/places listed in election_authorities':53} | {len(ea)}")
    print(f"{'Unique counties/places listed in target_smart':53} | {len(ts)}")
    
    # # PRINT count of OCD IDs that match with election authorities
    # sd_match_ea = sd[sd.isin(ea)].dropna()
    # print('Percent of counties/places in state data matching those in election authorities |', '{:.2%}'.format(len(sd_match_ea)/len(ea)))
    
    # # PRINT count of OCD IDs that are unique to state data
    # sd_doesnotmatch_ea = sd[~sd.isin(ea)].dropna()
    # print('Percent of counties/places found in state data but not in election authorities |', '{:.2%}'.format(len(sd_doesnotmatch_ea)/len(sd)))

    # # PRINT count of OCD IDs that are unique to target_smart
    # ts_doesnotmatch_sd = target_smart[~target_smart.isin(sd)].dropna()
    # print('Percent of counties/places found in TargetSmart data but not state data |', '{:.2%}'.format(len(ts_doesnotmatch_sd)/len(ts)))
    
    # print()
    # print('_'*85)
    # print('\n'*1)
   

    return
            
    

def clean_data(state_feed, state_data, election_authorities, target_smart):
    """
    PURPOSE: cleans & formats state_feed, state_data, election_authorities, sand target_smart to output standards
    INPUT: state_feed, state_data, election_authorities, target_smart
    RETURN: state_feed, state_data dataframe, election_authorities, target_smart dataframes
    """

    # FORMAT dates (3 formatted)
    state_feed['election_date'] = pd.to_datetime(state_feed['election_date'])
    state_data['start_date'] = pd.to_datetime(state_data['start_date'])
    state_data['end_date'] = pd.to_datetime(state_data['end_date'])
    
    # FORMAT hours (2 formatted)
    state_data['start_time'] = state_data['start_time'].str.replace(' ', '')
    state_data['start_time'] = state_data['start_time'].str.replace('-',':00-')
    temp = state_data['start_time'].str.split('-', n=1, expand=True)
    temp[0] = temp[0].str.pad(8, side='left', fillchar='0')
    temp[1] = temp[1].str.pad(5, side='left', fillchar='0')
    state_data['start_time'] = temp[0] + '-' + temp[1]

    state_data['end_time'] = state_data['end_time'].str.replace(' ', '')
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
    election_authorities['county'] = election_authorities['ocd_division'].str.extract('\\/\\w+\\:(\\w+\'?\\-?\\~?\\w+?)$').str.replace('_', ' ').str.replace('~', "'")
    
    # FORMAT precinct (2 formatted)
    state_data['precinct'] = state_data['precinct'].str.strip().str.upper()
    target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.strip().str.upper()

    # FORMAT voter file addresses (1 formatted)
    target_smart['vf_reg_address_1'] = target_smart['vf_reg_address_1'].str.upper().str.strip()

    # REMOVE headers in TargetSmart dataframe
    # NOTE: Occasionaly TargetSmart includes a header as a row (specifically for MT), so this accounts for this error
    target_smart = target_smart[target_smart['vf_precinct_name'] != 'VF_PRECINCT_NAME'] 

    # FORMAT address line (1 formatted)
    # NOTE: A common outreach error was missing state abbreviations in the address line
    state_abbrv_with_space = state_feed['state_abbrv'][0].center(4, ' ')
    insert_state_abbrv = lambda x: re.sub('[ ](?=[^ ]+$)', state_abbrv_with_space,  x) if state_abbrv_with_space not in x else x 
    state_data['address_line'] = state_data['address_line'].apply(lambda x: insert_state_abbrv(x))

    # CONDITIONAL formattating for 2 states (NH, AZ) 
    if state_feed['state_abbrv'][0] == 'NH':

        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.upper().str.replace('-', ' ').str.split().str.join(' ') # REMOVE dash in precinct names
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('"', '') # REMOVE quotes in precinct names
        state_data['county'] = state_data['county'].str.replace("'", '') # REMOVE apostrophes in state_data `county` because target_smart does not incl. them 
        target_smart['vf_township'] = target_smart['vf_township'].str.upper() 

        """ NOTE: In New Hampshire, BERLIN WARD 02 in 2018 was merged with BERLIN WARD 03. 
        For future elections, TargetSmart may update the voterfile to reflect these changes and thus BERLIN WARD 02
        will no longer be an issue. It may be worth isolating how many records listing `BERLIN WARD 02` remain and 
        deleting this if-statement if 0 are identified. """
        target_smart['vf_precinct_name'] = target_smart['vf_precinct_name'].str.replace('BERLIN WARD 02', 'BERLIN WARD 03') 

    elif state_feed['state_abbrv'][0] == 'AZ':
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

    polling_location.to_csv('polling_location_check.txt')

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

    precinct.to_csv('check.txt')
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
    street_segment['address_direction'] = street_segment['vf_reg_address_1'].str.extract('\\s+(N|E|S|W|NE|NW|SE|SW)$') # EXTRACT last cardinal direction in the address
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('\\s+(N|E|S|W|NE|NW|SE|SW)$', '') # REMOVE last cardinal direction in the address
    street_segment['street_direction'] = street_segment['vf_reg_address_1'].str.extract('\\s+(N|E|S|W|NE|NW|SE|SW)\\s+') # EXTRACT cardinal direction in the middle of the address
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('\\s+(N|E|S|W|NE|NW|SE|SW)\\s+', ' ') # REPLACE cardinal direction in the middle of the address

    street_segment['state'] = state_abbrv
    street_segment.rename(columns={'vf_reg_city':'city', 
                                   'vf_reg_address_2':'unit_number', 
                                   'vf_reg_zip':'zip'}, inplace=True)
    
    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    street_segment['unit_number_temp'] = street_segment['vf_reg_address_1'].str.strip().str.extract('(APT .*|UNIT .*|# .*|PMB .*)$') # EXTRACT unit number
    street_segment['unit_number'] = street_segment['unit_number'].replace('', np.nan, regex=True) # REPLACE empty strings with NaNs
    street_segment['unit_number'].fillna(street_segment['unit_number_temp'], inplace=True)
    street_segment['vf_reg_address_1'] = street_segment['vf_reg_address_1'].str.replace('(APT .*|UNIT .*|# .*|PMB .*)$', '') # REMOVE unit number from address

    # C1 Street Suffix Abbreviations (https://pe.usps.com/text/pub28/28apc_002.htm)
    # GENERATE regex for street ending
    street_suffix_list = ['ALLEE', 'ALLEY', 'ALLY', 'ALY', 'ANNEX', 'ANNX', 'ANX', 'ARC', 'ARCADE', 'AV', 'AVE', 'AVEN', 'AVNUE', 
                          'BAYOO', 'BAYOU', 'BYU', 'BCH', 'BEACH', 'BEND', 'BND', 'BLF', 'BLUF', 'BLUFF', 'BLUFFS', 'BFLS', 'BOT', 'BTM', 'BOTTM', 'BOTTOM', 'BLVD', 
                          'BOUL', 'BOULEVARD', 'BOULV', 'BR', 'BRNCH', 'BRANCH', 'BRDGE', 'BRG', 'BRIDGE', 'BRK', 'BROOK', 'BROOKS', 'BRKS', 'BURG', 'BG', 'BURGS', 'BGS',
                          'BYP', 'BYPA', 'BYPAS', 'BYPASS', 'BYPS', 
                          'CAMP', 'CP', 'CMP', 'CANYN', 'CANYON', 'CNYN', 'CYN', 'CAPE', 'CPE', 'CAUSEWAY', 'CAUSEWA', 'CSWY', 'CEN', 'CENT', 'CENTER', 'CENTR', 'CENTRE', 'CNTER', 
                          'CNTR', 'CTR', 'CENTERS', 'CIR', 'CIRC', 'CIRCL', 'CIRCLE', 'CRCL', 'CRCLE', 'CIRCLES', 'CLF', 'CLIFF', 'CLIFFS', 'CLFS', 'CLB', 'CLUB', 'COMMON', 'CMN', 
                          'COMMONS', 'CMNS', 'COR', 'CORNER', 'CORNERS', 'CORS', 'COURSE', 'CRSE', 'COURT', 'CT', 'COURTS', 'CTS', 'COVE', 'CV', 'CREEK', 'CRK', 'CRESCENT', 'CRES',
                          'CRSENT', 'CRSNT', 'CREST', 'CRST', 'CROSSING', 'CRSSNG', 'XING', 'CROSSROAD', 'XRD', 'CROSSROADS', 'XRDS', 'CURVE', 'CURV', 'DALE', 'DL', 'DAM', 'DM', 
                          'DIV', 'DIVIDE', 'DV', 'DVD', 'DR', 'DRIVE', 'DRIV', 'DRV', 'DRIVES', 'DRS', 
                          'EST', 'ESTATE', 'ESTATES', 'ESTS', 'EXP', 'EXPR', 'EXPRESS', 'EXPRESSWAY', 'EXPW', 'EXPY', 'EXT', 'EXTENSION', 'EXTN', 'EXTNSN', 'EXTS', 
                          'FALL', 'FALLS', 'FLS', 'FERRY', 'FRRY', 'FRY', 'FIELD', 'FLD', 'FIELDS', 'FLDS', 'FLAT', 'FLT', 'FLATS', 'FLTS', 'FORD', 'FRD', 'FORDS', 'FRDS', 'FOREST', 
                          'FORESTS', 'FRST', 'FORG', 'FORGE', 'FRG', 'FORGES', 'FRGS', 'FORK', 'FRK', 'FORKS', 'FRKS', 'FORT', 'FRT', 'FT', 'FREEWAY', 'FREEWY', 'FRWAY', 'FRWY', 'FWY',
                          'GARDEN', 'GARDN', 'GRDEN', 'GRDN', 'GDN', 'GARDENS', 'GDNS', 'GRDNS', 'GATEWAY', 'GATEWY', 'GATWAY', 'GTWAY', 'GTWY', 'GLEN', 'GLN', 'GLENS', 'GLNS', 'GREEN', 
                          'GRN', 'GREENS', 'GRNS', 'GROV', 'GROVE', 'GRV', 'GROVES', 
                          'HARB', 'HARBOR', 'HARBRB', 'HBR', 'HRBOR', 'HARBORS', 'HBRS', 'HAVEN', 'HVN', 'HT', 'HTS', 'HIGHWAY', 'HIGHWY', 'HIWAY', 'HWY', 'HIWY', 'HWAY', 'HILL', 'HL', 
                          'HILLS', 'HLS', 'HLLW', 'HOLLOW', 'HOLLOWS', 'HOLW', 'HOLWS', 
                          'INLT', 'IS', 'ISLAND', 'ISLND', 'ISLANDS', 'ISLNDS', 'ISS', 'ISLE', 'ISLES', 
                          'JCT', 'JCTION', 'JCTN', 'JUNCTION', 'JUNCTN', 'JUNCTON', 'JCTNS', 'JCSTS', 'JUNCTIONS', 
                          'KEY', 'KY', 'KEYS', 'KYS', 'KNL', 'KNOL', 'KNOLL', 'KNLNS', 'KNOLLS', 
                          'LK', 'LAKE', 'LKS', 'LAKES', 'LAND', 'LANDING', 'LNDG', 'LNDNG', 'LANE', 'LN', 'LGT', 'LIGHT', 'LIGHTS', 'LGHTS', 'LF', 'LOAF', 'LCK', 'LOCK', 'LDG', 'LDGE', 
                          'LODG', 'LODGE', 'LOOP', 'LOOPS', 
                          'MALL', 'MNR', 'MANOR', 'MANORS', 'MNRS', 'MEADOW', 'MDW', 'MDW', 'MDWS', 'MEADOWS', 'MEDOWS', 'MEWS', 'MILL', 'ML', 'MILLS', 'MLS', 'MISSN', 'MSSN', 'MSN', 
                          'MOTORWAY', 'MTWY', 'MNT', 'MT', 'MOUNT', 'MNTAIN', 'MNTN', 'MOUNTAIN', 'MOUNTIN', 'MTN', 'MTIN', 'MNTNS', 'MOUNTAINS', 
                          'NCK', 'NECK', 
                          'ORCH', 'ORCHARD', 'ORCHRD', 'OVAL', 'OVL', 'OVERPASS', 'OPAS', 'OPASS', 
                          'PARK', 'PRK', 'PARKS', 'PRKS', 'PARKWAY', 'PARKWY', 'PKWAY', 'PKWY', 'PKY', 'PARKWAYS', 'PKWYS', 'PASS', 'PASSAGE', 'PSGE', 'PATH', 'PATHS', 'PIKE', 'PIKES', 
                          'PINE', 'PNE', 'PINES', 'PNES', 'PL', 'PLAIN', 'PLN', 'PLAINS', 'PLNS', 'PLAZA', 'PLZ', 'PLZA', 'POINT', 'PT', 'POINTS', 'PTS', 'PORT', 'PRT', 'PORTS', 'PRTS', 
                          'PR', 'PRARIE', 'PRR', 
                          'RAD', 'RADIAL', 'RADL', 'RADIEL', 'RADL', 'RAMP', 'RANCH', 'RANCHES', 'RNCH', 'RNCHS', 'RAPID', 'RPD', 'RAPIDS', 'RPDS', 'REST', 'RST', 'RDG', 'RIDGE', 'RDGE', 
                          'RDGS', 'RIDGES', 'RIV', 'RIVER', 'RVR', 'RIVR', 'RD', 'ROAD', 'RD', 'ROADS', 'RDS', 'ROUTE', 'ROW', 'RUE', 'RUN', 
                          'SHL', 'SHOAL', 'SHLS', 'SHOALS', 'SHOAR', 'SHORE', 'SHR', 'SHOARS', 'SHORES', 'SHRS', 'SKYWAY', 'SKWY', 'SPG', 'SPNG', 'SPRING', 'SPRNG', 'SPGS', 'SPNGS', 'SPRINGS', 
                          'SPRNGS', 'SPUR', 'SPURS', 'SQ', 'SQR', 'SQRE', 'SQU', 'SQUARE', 'SQRS', 'SQUARES', 'SQS', 'STA', 'STATION', 'STATN', 'STN', 'STRA', 'STRAV', 'STRAVEN', 'STRAVENUE', 
                          'STREAM', 'STREME' 'STRM', 'STREET', 'STRT', 'ST', 'STR', 'STREETS', 'STS', 'SMT', 'SUMIT', 'SUMITT', 'SUMMIT', 
                          'TER', 'TERR', 'TERRACE', 'THROUGHWAY', 'TRWY', 'TRACE', 'TRACES', 'TRCE', 'TRACK', 'TRACKS', 'TRAK', 'TRK', 'TRKS', 'TRAFFICWAY', 'TRFY', 'TRAIL', 'TRAILS', 'TRL',
                          'TRLS', 'TRAILER', 'TRLR', 'TRLRS', 'TUNEL', 'TUNL', 'TUNLS', 'TUNNEL', 'TUNNELS', 'TUNNL', 'TRNPK', 'TURNPIKE', 'TPKE', 'TURNPK', 
                          'UNDERPASS', 'UPAS', 'UN', 'UNION', 'UNIONS', 'UNS', 
                          'VALLEY', 'VALLY', 'VLLY', 'VLY', 'VALLEYS', 'VLYS', 'VDCT', 'VIA', 'VIADCT', 'VIADUCT', 'VIEW', 'VW', 'VIEWS', 'VWS', 'VILL', 'VILLAG', 'VILLAGE', 'VILLG', 'VILLIAGE', 
                          'VLG', 'VILLE', 'VL', 'VIS', 'VIST', 'VISTA', 'VST', 'VSTA', 
                          'WALK', 'WALKS', 'WALL', 'WAY', 'WY', 'WAYS', 'WELL', 'WL', 'WELLS', 'WLS']
    street_suffix_regex = re.compile(r'\b(' + '|'.join(street_suffix_list) + r')$\b', re.IGNORECASE)
    
    # CREATE feature(s) (6 created)
    street_segment['street_suffix'] = street_segment['vf_reg_address_1'].str.strip().str.extract(street_suffix_regex, expand = True)
    street_segment['house_number'] = street_segment['vf_reg_address_1'].str.extract('^(\\d+)').astype(str).str.replace('nan', '')
    street_segment['start_house_number'] = street_segment['house_number']
    street_segment['end_house_number'] = street_segment['house_number']
    street_segment['odd_even_both'] = 'both'
    street_segment['temp'] = street_segment['vf_reg_address_1'].str.strip().str.replace(street_suffix_regex, '')
    street_segment['street_name'] = street_segment['temp'].str.replace('^\\d+\\s+', '').str.strip()

    # REMOVE feature(s) (4 removed)
    street_segment.drop(['vf_reg_address_1', 'temp', 'unit_number_temp', 'house_number'], axis=1, inplace=True)
    street_segment.drop_duplicates(inplace=True) 

    # MERGE street_segment with state_data
    temp = state_data[['precinct', 'county']]
    temp.drop_duplicates(inplace=True)
    if state_abbrv == 'NH':
        # NOTE: Since NH uses township instead of county to group precincts. The following subsittution accounts for this difference
        street_segment['vf_county_name'] = street_segment['vf_township'] # NOTE: NH does precincts by town/city
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



def generate_zip(state_abbrv, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt, precinct.txt, street_segment.txt)
    INPUT: state_abbrv, files
    RETURN: exports zip of 11 .txt files
    """


    # WRITE dataframes to txt files
    file_list = []
    for name, df in files.items():

        file = name + '.txt'
        file_list.append(file)
        df.to_csv(file, index=False, encoding='utf-8')
        
        line = '|'
        print(f'{state_abbrv:2} {name:23} | {len(df.index)} row(s)')
        #print(state_abbrv, name, "|", len(df.index), "row(s)")

    # CREATE state directory
    if not os.path.exists(state_abbrv):
        os.makedirs(state_abbrv)
    
    # DEFINE name of zipfile
    zip_filename = 'vipfeed-pp-' + str(state_feed['election_date'][0].date()) + '-' + state_feed['state_abbrv'][0] + '.zip'

    # WRITE files to a zipfile
    with ZipFile(zip_filename, 'w') as zip:
        for file in file_list:
            zip.write(file)
            os.rename(file, os.path.join(state_abbrv, file))

    return 


def warning_missing_data(state_abbrv, state_data):
    """
    PURPOSE: To display a warning for empty fields in select columns of state_data
    INPUT: state_abbrv, state_data
    RETURN: True if there is missing data, False otherwise
    """

    missing_data_check = state_data[state_data.columns.difference(['directions', 'start_time', 'end_time', 'internal_notes'])].isnull().any(axis=1)
    missing_data_check.index = missing_data_check.index + 1  # INCREASE INDEX to correspond with google sheets index
    if missing_data_check.any(): # IF any rows have missing data (col set to True)
        print()
        print('!!! WARNING !!!', state_abbrv, 'is missing data from the following rows:')
        missing_data_list = missing_data_check.loc[lambda x: x==True].index.values.tolist()
        if len(missing_data_list) < 50:
            print (missing_data_check.loc[lambda x: x==True].index.values.tolist())
        else:
            print('[ More than 50 rows are missing data ]')
        print()
        print()
        return True

    return False


# def warning_multiple_directions(state_abbrv, state_data):
#     # Tested with ND (2018 EV data)
#     """
#     PURPOSE: To display a warning for locations in state_data that are listed with multiple directions
#     INPUT: state_abbrv, state_data
#     RETURN: True if there are locations with multiple directions, False otherwise
#     """

#     unique_rows = state_data[['OCD_ID', 'location_name', 'address_line', 'directions']].drop_duplicates()
#     duplicate_locations = unique_rows[unique_rows.duplicated(subset=['OCD_ID', 'location_name', 'address_line'],keep=False)]

#     if not duplicate_locations.empty:
#         duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index
#         groupby_duplicate_locations = sorted([tuple(x) for x in duplicate_locations.groupby(['OCD_ID', 'location_name', 'address_line']).groups.values()])
#         print()
#         print('!!! WARNING !!!', state_abbrv, 'has locations listed with multiple directions in the following rows:')
#         print(groupby_duplicate_locations)
#         print()
#         print()
#         return True

#     return False


def warning_cross_streets(state_abbrv, state_data):
    """
    PURPOSE: To display a warning for addresses in state_data that are formatted as cross-streets
    INPUT: state_abbrv, state_data
    RETURN: True if there are addresses included as cross-streets, False otherwise
    """

    cross_street_addresses = state_data[state_data['address_line'].str.contains(' & | and ')]

    if not cross_street_addresses.empty:
        print()
        print('!!! WARNING !!!', state_abbrv, 'contains addresses with invalid cross-streets format in the following rows:')
        print(list(cross_street_addresses.index + 1))
        print()
        print()
        return True

    return False


def warning_invalid_year(state_abbrv, state_data):
    """
    PURPOSE: To display a warning for dates in state_data that include an invalid year (defined as year != 2018)
    INPUT: state_abbrv, state_data
    RETURN: True if there are dates with an invalid year, False otherwise
    """

    incorrect_start_dates = state_data[state_data['start_date'].dt.year != 2018]
    incorrect_end_dates = state_data[state_data['end_date'].dt.year != 2018]
    incorrect_dates = incorrect_start_dates.append(incorrect_end_dates)

    if not incorrect_dates.empty:
        print()
        print('!!! WARNING !!!', state_abbrv, 'contains dates with an invalid year in the following rows:')
        print(list(set(incorrect_dates.index + 1)))
        print()
        print()
        return True

    return False           
    
##################################################### END OF FUNCTION DEFINITIONS



if __name__ == '__main__':
    

    # SET UP command line inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('--nargs', nargs='+')
    
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
        state_feed_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range='STATE_FEED').execute()
        state_feed_values = state_feed_result.get('values', [])
    except:
        print('Error: STATE_FEED Google Sheet is either missing from the Google workbook or there is data reading error.')
        raise

    try: 
        # LOAD election authorities data
        election_authorities_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_EA_ID, range='ELECTION_AUTHORITIES').execute()
        election_authorities_values = election_authorities_result.get('values', [])
    except:
        print('Error: ELECTION_AUTHORITIES Google Sheet is either missing from the Google workbook or there is data reading error.')
        raise

    try:
        # CONNECT to AWS RDS MySQL database
        targetsmart_creds = open('targetsmart_credentials.json', 'r')
        conn_string = json.load(targetsmart_creds)
        conn = mysql.connector.connect(**conn_string)
    except:
        print('Error: There is a database connection error.')


    # SET list of vars selected in MySQL query
    targetsmart_sql_col_list = ['vf_precinct_name', 'vf_reg_address_1', 'vf_reg_address_2', 'vf_reg_city', 
                                'vf_source_state', 'vf_reg_zip', 'vf_county_name', 'vf_township']
    targetsmart_sql_col_string = ', '.join(targetsmart_sql_col_list)
        
    states_successfully_processed = [] # STORE states that successfully create zip files
    states_with_warnings = [] # STORE states that have data warnings
    states_not_processed = [] # STORE states that are not processed
    increment_success = 0 # STORE error counts
    increment_httperror = 0
    increment_processingerror = 0
    
    # PROCESS each state individually
    for _, input_states in parser.parse_args()._get_kwargs(): # ITERATE through input arguments


        input_states = [state.upper() for state in input_states] # FORMAT all inputs as uppercase
        
        # GENERATE state_feed & election_authorities dataframe
        state_feed_all = pd.DataFrame(state_feed_values[1:],columns=state_feed_values[0])
        election_authorities_all = pd.DataFrame(election_authorities_values[0:],columns=election_authorities_values[0])
        election_authorities_all.drop([0], inplace=True)


        if 'ALL' in input_states: # IF user requests all states to be processed
            
            input_states = state_feed_all['state_abbrv'].unique().tolist() # EXTRACT unique list of 50 state abbreviations

        
        for state in input_states:
        
            try:
            
                # LOAD state data
                state_data_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=state).execute()
                state_data_values = state_data_result.get('values', [])
                state_data = pd.DataFrame(state_data_values[0:],columns=state_data_values[0])
                state_data.drop([0], inplace=True)
                
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state] # FILTER state_feed_all for selected state
                election_authorities = election_authorities_all[election_authorities_all['state'] == state] # FILTER election_authorities_all for selected state

                state_feed.reset_index(inplace=True) # RESET index
                sql_table_name = state_feed['official_name'].str.lower().str.replace(' ', '') # CREATE MYSQL table name (format: full name, lowercase, no spaces)
               

                try:
                    # LOAD TargetSmart data for a single state

                    curs = conn.cursor() # OPEN database connection

                    # SET MySQL query
                    if state == 'AZ':
                        
                        query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name[0] + \
                                " WHERE vf_county_name = 'PIMA' and vf_reg_address_1;"
                                
                    else: 
                        query = "SELECT " + targetsmart_sql_col_string + " FROM " + sql_table_name[0] + ";"

                    curs.execute(query) # EXECUTE MySQL query

                    target_smart_values = curs.fetchall() # LOAD in data

                    curs.close() # CLOSE cursor object
                    conn.rollback() # REFRESH database connection
                    
                    # GENERATE target_smart dataframe
                    target_smart = pd.DataFrame(list(target_smart_values), columns=targetsmart_sql_col_list, dtype='object')

                except:
                    print('ERROR: TargetSmart data for', state, 'is either missing from the database or there is data reading error.')

                print(datetime.datetime.now().replace(microsecond=0).isoformat())


                # CLEAN/FORMAT state_feed and state_data
                state_feed, state_data, election_authorities, target_smart = clean_data(state_feed, state_data, election_authorities, target_smart)

                # PRINT state name
                print('\n'*1)
                print(state_feed['official_name'][0].center(80, '-'))
                print()

                # WARNINGS for missing/incorrect/unusual data
                missing_data = warning_missing_data(state_feed['state_abbrv'][0], state_data)
                # multiple_directions = warning_multiple_directions(state_feed['state_abbrv'][0], state_data)
                cross_streets = warning_cross_streets(state_feed['state_abbrv'][0], state_data)
                invalid_year = warning_invalid_year(state_feed['state_abbrv'][0], state_data)
                if missing_data or cross_streets or invalid_year:
                    states_with_warnings.append(state)

                # VIP BUILD
                vip_build(state_data, state_feed, election_authorities, target_smart)

                states_successfully_processed.append(state)
                increment_success +=1
                
                
            except HttpError:
                print ('ERROR:', state, 'could not be found or retrieved from Google Sheets.')
                increment_httperror += 1
                
            except Exception as e:
                print ('ERROR:', state, 'could not be processed.', type(e).__name__, ':', e)
                increment_processingerror += 1
                states_not_processed.append(state)


    conn.close() # CLOSE MySQL database connection 


    # PRINT final report
    print('\n'*1)
    print('Summary Report'.center(80, ' '))
    print('\n'*1)
    print('Number of states that could not be found or retrieved from Google Sheets:', increment_httperror)
    print('Number of states that could not be processed:', increment_processingerror)
    print('Number of states that processed sucessfully:', increment_success)
    print()
    print('List of states that did not process:')
    print(states_not_processed)
    print()
    print('List of states with data warnings:')
    print(states_with_warnings)
    print()
    print('List of states that processed sucessfully:')
    print(states_successfully_processed)
    print('\n'*1)
    print('_'*80)
    print('\n'*2)
