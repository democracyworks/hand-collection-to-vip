from __future__ import print_function
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
import numpy as np
import re
#from IPython.display import display
import warnings
warnings.filterwarnings('ignore')


# _________________________________________________________________________________________________________________________

# SET global variables and print display formats
pd.set_option('display.max_columns', 100)  # or 1000 or None
pd.set_option('display.max_rows', 100)  # or 1000 or None
PRINT_OUTPUT_WIDTH = 100 # SET print output length
PRINT_CENTER = int(PRINT_OUTPUT_WIDTH/2) # SET print output middle

ELECTION_YEAR = 2018

STATES_WITH_WARNINGS = [] # STORE states that trigger warnings

# _________________________________________________________________________________________________________________________

# SET SCOPES & Google Spreadsheet IDs (2 IDs)

# PRO-TIP: if modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# states & STATE_FEED
# https://docs.google.com/spreadsheets/u/1/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=366784608
SPREADSHEET_ID = '1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4' 

# ELECTION_AUTHORITIES
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o' 


# _________________________________________________________________________________________________________________________


def vip_build(state_abbrv, state_feed, state_data, election_authorities):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 9 .txt files
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt)
    INPUT: state_abbrv, state_data, state_feed, election_authorities
    RETURN: None
    """


    # CREATE/FORMAT feature(s) (1 created, 1 formatted)
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # first make sure there are leading zeros
    state_feed['state_id'] = state_abbrv.lower() + state_feed['state_fips']
    state_feed['external_identifier_type'] = 'ocd-id' 
    
    # CLEAN/FORMAT state_feed, state_data, and election_authorities (3 dataframes)
    state_feed, state_data, election_authorities = clean_data(state_abbrv, state_feed, state_data, election_authorities)


    if election_authorities.empty:
        # CREATE empty election_authorities DataFrame if state not in election administration sheet
        election_authorities = pd.DataFrame(columns=['ocd_division','election_administration_id','homepage_url',
                                                     'official_title','election_official_person_id'])

    else:
        # CREATE 'election_adminstration_id'
        temp = election_authorities[['ocd_division']]
        temp.drop_duplicates(['ocd_division'], inplace=True)
        temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
        temp['election_administration_id'] = 'ea' + (temp.index + 1).astype(str).str.zfill(4)
        election_authorities = pd.merge(election_authorities, temp, on =['ocd_division'])
        election_authorities.drop_duplicates(subset=['election_administration_id'], inplace=True) #REMOVE all except first election administration entry for each ocd-id

        # CREATE 'election_official_person_id'
        temp = election_authorities[['ocd_division', 'official_title']]
        temp.drop_duplicates(['ocd_division', 'official_title'], keep='first',inplace=True)
        temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
        temp['election_official_person_id'] = 'per' + (temp.index + 1).astype(str).str.zfill(4)
        election_authorities = pd.merge(election_authorities, temp, on =['ocd_division', 'official_title'])
    
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
    
    
    # GENERATE 9 .txt files
    election = generate_election(state_feed)
    polling_location = generate_polling_location(state_data)
    schedule = generate_schedule(state_data, state_feed)
    source = generate_source(state_feed)
    state = generate_state(state_feed)
    locality = generate_locality(state_feed, state_data, election_authorities)
    election_administration = generate_election_administration(election_authorities)
    department = generate_department(election_authorities)
    person = generate_person(election_authorities)


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
                                                 
    # PRINT state report
    state_report(state_abbrv, state_feed, state_data, 
                 election_authorities, {'state':state, 
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

    # RESET state_feed index
    state_feed.reset_index(drop=True, inplace=True)
    state_data.reset_index(drop=True, inplace=True)

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
    
    # FORMAT booleans (4 formatted)
    true_chars = [char for char in 'true' if char not in 'false'] # SET unique chars in 'true' and not in 'false'
    false_chars = [char for char in 'false' if char not in 'true'] # SET unique chars in 'true' and not in 'false'
    lambda_funct = (lambda x: None if not x else ( \
                    'true' if any(char in x for char in true_chars) == True else ('false' if any(char in x for char in false_chars) == True else np.nan)))
    if state_data['is_drop_box'].isnull().all():
        state_data['is_drop_box'] = 'false'
    else:
        state_data['is_drop_box'] = state_data['is_drop_box'].str.lower().apply(lambda_funct)
    state_data['is_early_voting'] = state_data['is_early_voting'].str.lower().apply(lambda_funct)
    state_data['is_only_by_appointment'] = state_data['is_only_by_appointment'].str.lower().apply(lambda_funct)
    state_data['is_or_by_appointment'] = state_data['is_or_by_appointment'].str.lower().apply(lambda_funct)

    # FORMAT ocd division ids (2 formatted)
    state_data['OCD_ID'] = state_data['OCD_ID'].str.strip()
    if not election_authorities.empty:
        election_authorities['ocd_division'] = election_authorities['ocd_division'].str.upper().str.strip()
    
    state_data['address_line'] = state_data['address_line'].str.strip().str.replace('\\s{2,}', ' ')
    state_data['location_name'] = state_data['location_name'].str.strip().str.replace('\'S', '\'s')
    
    # _____________________________________________________________________________________________________________________

    # ERROR HANDLING | Interventionist adjustments to account for state eccentricities and common hand collection mistakes, etc

    # FORMAT address line (1 formatted)
    # NOTE: A common outreach error was missing state abbreviations in the address line
    state_abbrv_with_space = state_feed['state_abbrv'][0].center(4, ' ')
    state_data['address_line'] = state_data['address_line'].str.replace('.','')
    insert_state_abbrv = lambda x: x if re.search('(\\d{5})$', x) == None else \
                                  (re.sub('[ ](?=[^ ]+$)', state_abbrv_with_space,  x) if state_abbrv_with_space not in x else x)
    state_data['address_line'] = state_data['address_line'].apply(lambda x: insert_state_abbrv(x))


    return state_feed, state_data, election_authorities



def generate_election(state_feed):
    """
    PURPOSE: generates election dataframe for .txt file
    INPUT: state_feed
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
    polling_location = state_data[['polling_location_ids','location_name', 'address_line', 'directions',
                                   'hours_open_id', 'is_drop_box', 'is_early_voting']] 

    # FORMAT col(s) (2 formatted)                              
    polling_location.rename(columns={'polling_location_ids':'id', 
                                     'location_name':'name'}, inplace=True)
    polling_location.drop_duplicates(inplace=True)


    return polling_location



def generate_schedule(state_data, state_feed):
    """
    PURPOSE: generates schedule dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: schedule dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/hours_open.html#schedule
    """ 

    # SELECT feature(s)
    schedule = state_data[['start_time', 'end_time', 'start_date', 'end_date', 'hours_open_id',
                           'is_only_by_appointment', 'is_or_by_appointment']]

    # CREATE feature(s) (1 created)
    schedule.reset_index(drop=True, inplace=True) 
    schedule['id'] = 'sch' + (schedule.index + 1).astype(str).str.zfill(4) 


    return schedule



def generate_source(state_feed):
    """
    PURPOSE: generates source dataframe for .txt file
    INPUT: state_feed
    RETURN: source dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/source.html
    """ 
    
    # SELECT feature(s)
    source = state_feed[['state_fips']]

    # CREATE/FORMAT feature(s) (4 created, 1 formatted)    
    source.reset_index(drop=True, inplace=True)
    source['id'] = 'src' + (source.index + 1).astype(str).str.zfill(4)
    source['date_time'] = datetime.datetime.now().replace(microsecond=0).isoformat() 
    source['name'] = 'Democracy Works Outreach Team'
    source['version'] = '5.1' # REFERENCES VIP SPEC
    source.rename(columns={'state_fips':'vip_id'}, inplace=True) # RENAME col(s)
    

    return source



def generate_state(state_feed): 
    """
    PURPOSE: generates state dataframe for .txt file
    INPUT: state_feed
    RETURN: state dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/state.html
    """ 
    
    # SELECT col(s)
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

    # SELECT feature(s)
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
    locality['id'] = 'loc' + (locality.index + 1).astype(str).str.zfill(4)
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

    # SELECT feature(s)
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

    # SELECT feature(s)
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

    # SELECT feature(s)
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
    zip_filename = 'vipfeed-ev-' + str(state_feed['election_date'][0].date()) + '-' + state_abbrv + '.zip'

    # WRITE files to a zipfile
    with ZipFile(zip_filename, 'w') as zip:
        for file in file_list:
            zip.write(file)
            os.rename(file, os.path.join(state_abbrv, file))


    return 




###########################################################################################################################
# END OF VIP BUILD FUNCTION DEFINITIONS ###################################################################################
###########################################################################################################################



def state_report(state_abbrv, state_feed, state_data, election_authorities, files):
    """
    PURPOSE: print state report (general descriptive stats and warnings)
    INPUT: state_abbrv, state_feed, state_data, election_authorities, files
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

    # _____________________________________________________________________________________________________________________


    # GENERATE warnings
    missing_data_rows = warning_missing_data(state_data)
    multi_directions_rows = warning_multi_directions(state_data)
    cross_street_rows = warning_cross_street(state_data)
    date_year_rows = warning_date_year(state_data)

    if missing_data_rows or multi_directions_rows or cross_street_rows or date_year_rows:
        print('\n'*2)
        print( '---------------------- WARNINGS ----------------------'.center(PRINT_OUTPUT_WIDTH, ' ')) 
        STATES_WITH_WARNINGS.append(state_abbrv) # RECORD states with warnings

    if missing_data_rows:
        print('\n')
        print('Missing Data'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(missing_data_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

    if multi_directions_rows:
        print('\n')
        print('Polling Locations have Multiple Directions'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(multi_directions_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

    if cross_street_rows:
        print('\n')
        print('Problematic Cross-Street Formats'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(cross_street_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))

    if date_year_rows:
        print()
        print('Dates have Invalid Years'.center(PRINT_OUTPUT_WIDTH, ' '))
        print()
        print(str(date_year_rows).strip('[]').center(PRINT_OUTPUT_WIDTH, ' '))


    print('\n'*1)
    print('_'*PRINT_OUTPUT_WIDTH)
    print('\n'*2)


    return 



def warning_missing_data(state_data):
    """
    PURPOSE: isolate which rows, if any, are missing data in the state data 
    INPUT: state_data
    RETURN: missing_data_rows
    """

    missing_data_check = state_data[state_data.columns.difference(['directions', 'start_time', 'end_time', 'internal_notes'])].isnull().any(axis=1)
    missing_data_check.index = missing_data_check.index + 1  # INCREASE INDEX to correspond with google sheets index
    missing_data_rows = []
    
    if missing_data_check.any(): # IF any data is missing
        missing_data_rows = missing_data_check.loc[lambda x: x==True].index.values.tolist()
        if len(missing_data_rows) > 30:  # IF there are more than 30 rows with missing data then simply notify user
            missing_data_rows = ['More than 30 rows with missing data']

            
    return missing_data_rows



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



def warning_multi_directions(state_data):
    """
    PURPOSE: isolate which polling locations, if any, have multiple directions (warning: each unique set of directions is considered a polling location)
    INPUT: state_data
    RETURN: multi_directions_rows
    """

    # SELECT feature(s) (4 selected)
    unique_rows = state_data[['OCD_ID', 'location_name', 'address_line', 'directions']].drop_duplicates()
    duplicate_locations = unique_rows[unique_rows.duplicated(subset=['OCD_ID', 'location_name', 'address_line'],keep=False)]
    duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index

    multi_directions_rows = []
    if not duplicate_locations.empty: # IF the dataframe is not empty
        multi_directions_rows = sorted([tuple(x) for x in duplicate_locations.groupby(['OCD_ID', 'location_name', 'address_line']).groups.values()])


    return multi_directions_rows



def warning_date_year(state_data): # CRITICAL
    """
    PURPOSE: isolate which rows, if any, have a start_date or end_date outside of the election year
    INPUT: state_data
    RETURN: date_year_rows
    """

    # ISOLATE data errors in 2 features
    incorrect_start_dates = state_data[state_data['start_date'].dt.year != ELECTION_YEAR]
    incorrect_end_dates = state_data[state_data['end_date'].dt.year != ELECTION_YEAR]
    incorrect_dates = incorrect_start_dates.append(incorrect_end_dates)

    date_year_rows = list(set(incorrect_dates.index + 1))


    return date_year_rows



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
        print('States that failed to process & why'.center(PRINT_OUTPUT_WIDTH, ' '))
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
        print('Error: STATE_FEED Google Sheet is either missing from the workbook or there is data reading error.')
        raise

    try: 
        # LOAD election authorities data
        election_authorities_result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_EA_ID, 
                                                                          range='ELECTION_AUTHORITIES').execute()
        election_authorities_values = election_authorities_result.get('values', [])
    except:
        print('Error: ELECTION_AUTHORITIES Google Sheet is either missing from the workbook or there is data reading error.')
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
                state_data = pd.DataFrame(state_data_values[0:],columns=state_data_values[0])
                state_data.drop([0], inplace=True)
                    
                # FILTER state_feed and election_authorities
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state_abbrv] # FILTER state_feed_all for selected state
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
                exception_string = state_abbrv + ' | ' + str(type(e).__name__) + ' ' + str(e) + '\n'
                states_failed_to_process.append(exception_string)


    summary_report(len(input_states), increment_httperror, increment_processingerror, increment_success,
                   states_failed_to_load, states_failed_to_process, states_successfully_processed)


    print('Timestamp:', datetime.datetime.now().replace(microsecond=0))


