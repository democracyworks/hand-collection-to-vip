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
#from IPython.display import display
import warnings
warnings.filterwarnings('ignore')

# PRO-TIP: if modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# SET Google Spreadsheet IDs (2 IDs)

# states & STATE_FEED
# https://docs.google.com/spreadsheets/u/1/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=366784608
SPREADSHEET_ID = '1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4' 

# ELECTION_AUTHORITIES
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o' 


def vip_build(state_data, state_feed, election_authorities):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 9 .txt files
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt)
    INPUT: state_data, state_feed, election_authorities
    RETURN: None
    """


    # CREATE/FORMAT feature(s) referenced by multiple .txts (6 created, 1 formatted)
    state_feed['state_fips'] = state_feed['state_fips'].str.pad(2, side='left', fillchar='0') # PAD for leading zeros
    state_feed['state_id'] = state_feed['state_abbrv'].str.lower() + state_feed['state_fips']
    state_feed['external_identifier_type'] = 'ocd-id' 
    
    if election_authorities.empty:
        # CREATE empty election_authorities DataFrame if state not in Election Administration sheet
        election_authorities = pd.DataFrame(columns=['ocd_division','election_administration_id','homepage_url','official_title','election_official_person_id'])

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
    temp = state_data[['OCD_ID', 'location_name', 'address_line', 'directions']]
    temp.drop_duplicates(['OCD_ID', 'location_name', 'address_line', 'directions'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['hours_open_id'] = 'hours' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['OCD_ID','location_name','address_line', 'directions'])

    # CREATE 'polling_location_ids'
    temp = state_data[['OCD_ID', 'location_name', 'address_line', 'directions']]
    temp.drop_duplicates(['OCD_ID', 'location_name', 'address_line', 'directions'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['polling_location_ids'] = 'pol' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['OCD_ID','location_name','address_line', 'directions'])
    
    
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
    generate_zip(state_feed['state_abbrv'][0], 
                 state_feed['official_name'][0], {'election': election,
                                                  'polling_location': polling_location,
                                                  'schedule': schedule,
                                                  'source':source,
                                                  'state':state,
                                                  'locality':locality,
                                                  'election_administration':election_administration,
                                                  'department': department, 
                                                  'person': person})
                                                 
    print_state_report(state_feed['state_abbrv'][0], state_data, election_authorities)

    return

    
def clean_data(state_feed, state_data, election_authorities):
    """
    PURPOSE: cleans and formats state_feed, state_data, & election_authorities to output standards
    INPUT: state_feed, state_data
    RETURN: state_feed, state_data dataframes
    """

    # RESET state_feed index
    state_feed.reset_index(drop=True, inplace=True)

    # REPLACE empty strings with NaNs
    state_data = state_data.replace('^\\s*$', np.nan, regex=True)

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
    election_authorities['ocd_division'] = election_authorities['ocd_division'].str.strip()
    
    state_data['address_line'] = state_data['address_line'].str.strip()
    state_data['location_name'] = state_data['location_name'].str.strip().str.replace('\'S', '\'s')


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
    locality['name'] = locality['OCD_ID'].str.extract('([^\\:]*)$')
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
    person['title'] = person['ocd_division'].str.extract('([^\\:]*)$').str.upper() + ' ' + person['official_title'].str.upper()
    person.rename(columns={'election_official_person_id':'id'}, inplace=True)

    # REMOVE feature(s) (2 removed)
    person.drop(['ocd_division', 'official_title'], axis=1, inplace=True)


    return person

    
 
def generate_zip(state_abbrv, official_name, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, schedule.txt, source.txt, state.txt, locality.txt, 
    election_administration.txt, department.txt, person.txt)
    INPUT: state_abbrv, official_name, files
    RETURN: exports zip of 9 .txt files
    """

    # WRITE dataframes to txt files
    file_list = []
    for name, df in files.items():

        file = name + '.txt'
        file_list.append(file)
        df.to_csv(file, index=False, encoding='utf-8')
        
        print(state_abbrv, name, "|", len(df.index), "row(s)")

    # CREATE state directory
    if not os.path.exists(state_abbrv):
        os.makedirs(state_abbrv)

    # DEFINE name of zipfile
    zip_filename = 'vipfeed-ev-' + str(state_feed['election_date'][0].date()) + '-' + state_feed['state_abbrv'][0] + '.zip'

    # WRITE files to a zipfile
    with ZipFile(zip_filename, 'w') as zip:
        for file in file_list:
            zip.write(file)
            os.rename(file, os.path.join(state_abbrv, file))

    return


def print_state_report(state_abbrv, state_data, election_authorities):
    """
    PURPOSE: to print report on OCD IDs for the state being processed 
    INPUT: state_data, election_authorities
    RETURN: N/A
    """
    # HANDLE case where election_authorities is empty
    if election_authorities.empty:
        print()
        print('Note:', state_abbrv, 'does not include Election Authorities data.')

    else:
        # CREATE dataframes of unique OCD IDs for election authorities and state data
        ea = election_authorities[['ocd_division']]
        ea.drop_duplicates(inplace=True)
        sd = state_data[['OCD_ID']]
        sd.drop_duplicates(inplace=True)

        # PRINT count of unique OCD IDs
        print()
        print('Unique OCD IDs listed |', len(sd), ' in state data <>', len(ea), ' in election authorities')
        
        # PRINT count of OCD IDs that match with election authorities
        diff_election_authorities = ea.merge(sd, how = 'left', left_on = 'ocd_division', right_on = 'OCD_ID')
        diff_election_authorities = diff_election_authorities[diff_election_authorities['OCD_ID'].isnull() == False]
        print('Percent of OCD IDs in state data matching those in election authorities |', '{:.2%}'.format(len(diff_election_authorities)/len(ea)))
        
        # PRINT count of OCD IDs that are unique to state data
        diff_state_data = sd.merge(ea, how = 'left', left_on = 'OCD_ID', right_on = 'ocd_division')
        diff_state_data = diff_state_data[diff_state_data['ocd_division'].isnull()]
        print('Percent of OCD IDs found only in state data |', '{:.2%}'.format(len(diff_state_data)/len(sd)))

    print()

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


def warning_multiple_directions(state_abbrv, state_data):
    # Tested with ND (2018 EV data)
    """
    PURPOSE: To display a warning for locations in state_data that are listed with multiple directions
    INPUT: state_abbrv, state_data
    RETURN: True if there are locations with multiple directions, False otherwise
    """

    unique_rows = state_data[['OCD_ID', 'location_name', 'address_line', 'directions']].drop_duplicates()
    duplicate_locations = unique_rows[unique_rows.duplicated(subset=['OCD_ID', 'location_name', 'address_line'],keep=False)]

    if not duplicate_locations.empty:
        duplicate_locations.index = duplicate_locations.index + 1  # INCREASE INDEX to correspond with google sheets index
        groupby_duplicate_locations = sorted([tuple(x) for x in duplicate_locations.groupby(['OCD_ID', 'location_name', 'address_line']).groups.values()])
        print()
        print('!!! WARNING !!!', state_abbrv, 'has locations listed with multiple directions in the following rows:')
        print(groupby_duplicate_locations)
        print()
        print()
        return True

    return False


def warning_cross_streets(state_abbrv, state_data):
    cross_street_addresses = state_data[state_data['address_line'].str.contains(' & | and ')]

    if not cross_street_addresses.empty:
        print()
        print('!!! WARNING !!!', state_abbrv, 'contains addresses with invalid cross-streets format in the following rows:')
        print(list(cross_street_addresses.index + 1))
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
    

    states_successfully_processed = [] # STORE states that successfully create zip files
    states_with_warnings = [] # STORE states that are missing data
    increment_success = 0 # STORE count of states successfully processed
    increment_httperror = 0 # STORE count of states that could not be retrieved or found in Google Sheets
    increment_processingerror = 0 # STORE count of states that could not be processed
    
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
                    
                # FILTER state_feed and election_authorities
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state] # FILTER state_feed_all for selected state
                election_authorities = election_authorities_all[election_authorities_all['state'] == state] # FILTER election_authorities_all for selected state

                # CLEAN/FORMAT state_feed and state_data
                state_feed, state_data, election_authorities = clean_data(state_feed, state_data, election_authorities)

                # PRINT state name
                print('\n'*1)
                print(state_feed['official_name'][0].center(80, '-'))
                print()

                # WARNINGS for missing/incorrect/unusual data
                missing_data = warning_missing_data(state_feed['state_abbrv'][0], state_data)
                multiple_directions = warning_multiple_directions(state_feed['state_abbrv'][0], state_data)
                cross_streets = warning_cross_streets(state_feed['state_abbrv'][0], state_data)
                if missing_data or multiple_directions or cross_streets:
                    states_with_warnings.append(state)

                # VIP BUILD
                vip_build(state_data, state_feed, election_authorities)
                
                states_successfully_processed.append(state)
                increment_success +=1

            except HttpError:
                print ('ERROR:', state, 'could not be found or retrieved from Google Sheets.')
                increment_httperror += 1

            except Exception as e:
                print ('ERROR:', state, 'could not be processed.', type(e).__name__, ':', e)
                increment_processingerror += 1


    # PRINT final report
    print('_'*80)
    print('\n'*1)
    print('Summary Report'.center(80, ' '))
    print('\n'*1)
    print('Number of states that could not be found or retrieved from Google Sheets:', increment_httperror)
    print('Number of states that could not be processed:', increment_processingerror)
    print('Number of states that processed sucessfully:', increment_success)

    print()
    print('List of states with data warnings:')
    print(states_with_warnings)
    print()
    print('List of states that processed sucessfully:')
    print(states_successfully_processed)
    print('\n'*1)
    print('_'*80)
    print('\n'*2)
    
