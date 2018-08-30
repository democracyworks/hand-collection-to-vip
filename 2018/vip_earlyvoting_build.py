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
#from IPython.display import display
import warnings
warnings.filterwarnings('ignore')



# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# SET Google Spreadsheet IDs
SPREADSHEET_ID = '1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4' # MAIN (states & STATE_FEED)
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o' # ELECTION_AUTHORITIES

def vip_build(state_data, state_feed, election_authorities):
    """
    PURPOSE: transforms state_data and state_feed data into .txt files, exports zip of 6 .txt files
    (election_administration.txt, department.txt, election.txt, polling_location.txt, locality.txt, 
    schedule.txt, source.txt, state.txt)
    INPUT: state_data, state_feed
    RETURN: None
    """
    
    # CLEAN/FORMAT state_feed and state_data
    state_feed, state_data, election_authorities = clean_data(state_feed, state_data, election_authorities)

    # CREATE/FORMAT feature(s) referenced by multiple .txts
    state_feed.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    state_feed['state_id'] = state_feed['state_abbrv'].str.lower() + state_feed['state_fips']
    state_feed['external_identifier_type'] = 'ocd-id' 
    
    # CREATE 'election_adminstration_id'
    temp = election_authorities[['ocd_division']]
    temp.drop_duplicates(['ocd_division'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['election_administration_id'] = 'ea' + (temp.index + 1).astype(str).str.zfill(4)
    election_authorities = pd.merge(election_authorities, temp, on =['ocd_division'])
    
    # CREATE 'hours_only_id'
    temp = state_data[['OCD_ID', 'location_name', 'address_1']]
    temp.drop_duplicates(['OCD_ID', 'location_name', 'address_1'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['hours_open_id'] = 'hours' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['OCD_ID','location_name','address_1'])

    # CREATE 'polling_location_ids'
    temp = state_data[['OCD_ID', 'location_name', 'address_1']]
    temp.drop_duplicates(['OCD_ID', 'location_name', 'address_1'], inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['polling_location_ids'] = 'pol' + (temp.index + 1).astype(str).str.zfill(4)
    state_data = pd.merge(state_data, temp, on =['OCD_ID','location_name','address_1'])
    
    # CREATE 'election_official_person_id'
    temp = election_authorities[['ocd_division', 'official_title']]
    temp.drop_duplicates(['ocd_division', 'official_title'], keep='first',inplace=True)
    temp.reset_index(drop=True, inplace=True) # RESET index prior to creating id
    temp['election_official_person_id'] = 'per' + (temp.index + 1).astype(str).str.zfill(4)
    election_authorities = pd.merge(election_authorities, temp, on =['ocd_division', 'official_title'])
    
    # GENERATE 8 .txt files
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
                 state_feed['office_name'][0], {'election': election,
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
    
    return
            
    
def clean_data(state_feed, state_data, election_authorities):
    """
    PURPOSE: cleans and formats state_feed & state_data to output standards
    INPUT: state_feed, state_data
    RETURN: state_feed, state_data dataframe
    """

    # FORMAT dates
    state_feed['election_date'] = pd.to_datetime(state_feed['election_date'])
    state_data['start_date'] = pd.to_datetime(state_data['start_date'])
    state_data['end_date'] = pd.to_datetime(state_data['end_date'])
    
    # FORMAT hours
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
    
    state_data['is_drop_box'] = state_data['is_drop_box'].str.lower().str.strip()
    state_data['is_early_voting'] = state_data['is_early_voting'].str.lower().str.strip()
    state_data['is_only_by_appointment'] = state_data['is_only_by_appointment'].str.lower().str.strip()
    state_data['is_or_by_appointment'] = state_data['is_or_by_appointment'].str.lower().str.strip()

    state_data['OCD_ID'] = state_data['OCD_ID'].str.strip()
    election_authorities['ocd_division'] = election_authorities['ocd_division'].str.strip()
    
    return state_feed, state_data, election_authorities

def generate_person(election_authorities):
    """
    PURPOSE: generates person dataframe for .txt file
    INPUT: election_authorities
    RETURN: person dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/person.html
    """

    # CREATE/FORMAT feature(s)
    person = election_authorities[['ocd_division', 'official_title', 'election_official_person_id', 'state']]
    person.drop_duplicates('election_official_person_id', keep='first',inplace=True)
    person['date_of_birth'] = ''
    person['first_name'] = ''
    person['middle_name'] = ''
    person['last_name'] = ''
    person['nickname'] = ''
    person['gender'] = ''
    person['party_id'] = ''
    person['prefix'] = ''
    person['suffix'] = ''
    person['profession'] = 'ELECTION ADMINISTRATOR'
    
    # EXTRACT county/place and CREATE/FORMAT official_title
    person['official_title'] = person['ocd_division'].str.extract('\\/\\w+\\:(\\w+)$').str.upper() + ' COUNTY ' + person['official_title'].str.upper()
    
    person.rename(columns={'official_title':'title', 'election_official_person_id':'id', }, inplace=True) # RENAME col(s)
    person.drop(['ocd_division', 'state'], axis=1, inplace=True)

    return person

    
def generate_election(state_feed):
    """
    PURPOSE: generates election dataframe for .txt file
    INPUT: state_feed
    RETURN: election dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/election.html
    """

    # CREATE/FORMAT feature(s)
    election = state_feed[['election_date','election_name','state_id']]
    election['id'] = 'ele01' 
    election['election_type'] = 'Federal'
    election['is_statewide'] = 'true'
    election.rename(columns={'election_date':'date', 'election_name':'name'}, inplace=True) # RENAME col(s)
    
    return election

def generate_polling_location(state_data):
    """
    PURPOSE: generates polling_location dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: polling_location dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/polling_location.html
    """ 
    polling_location = state_data[['polling_location_ids','location_name', 'address_1', 'dirs',
                                   'hours_open_id', 'is_drop_box', 'is_early_voting']] # SELECT col(s)
    polling_location.rename(columns={'polling_location_ids':'id', # RENAME col(s)
                                     'location_name':'name', 
                                     'address_1':'address_line',
                                     'dirs':'directions'}, inplace=True)
    polling_location.drop_duplicates(inplace=True)
    return polling_location

def generate_schedule(state_data, state_feed):
    """
    PURPOSE: generates schedule dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: schedule dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/hours_open.html#schedule
    """ 
    schedule = state_data[['start_time', 'end_time', 'start_date', 'end_date', # SELECT col(s)
                           'is_only_by_appointment', 'is_or_by_appointment', 'hours_open_id']]
    schedule.reset_index(drop=True, inplace=True) 
    schedule['id'] = 'sch' + (schedule.index + 1).astype(str).str.zfill(4) # CREATE feature(s)

    return schedule

def generate_source(state_feed):
    """
    PURPOSE: generates source dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: source dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/source.html
    """ 
    
    # CREATE/FORMAT feature(s)

    source = state_feed[['state_fips']]
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
    INPUT: state_data, state_feed
    RETURN: state dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/state.html
    """ 
    ### include polling_location_ids?
    state = state_feed[['state_id','external_identifier_type','ocd_division','office_name']]
    state.rename(columns={'state_id':'id', 
                          'ocd_division':'external_identifier_value', # RENAME col(s)
                          'office_name':'name'}, inplace=True)
    return state

def generate_locality(state_data, state_feed, election_authorities):
    """
    PURPOSE: generates locality dataframe for .txt file
    INPUT: state_data, state_feed
    RETURN: locality dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/built_rst/xml/elements/locality.html
    """ 

    # SELECT feature(s)
    locality = state_feed[['OCD_ID', 'polling_location_ids']]

    # GROUP polling_location_ids
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows
    grouped = locality.groupby('OCD_ID')
    ids_joined = pd.DataFrame(grouped.aggregate(lambda x: ' '.join(x))['polling_location_ids']) 
    locality = ids_joined.reset_index()  # FLATTEN aggregated df

    # CREATE feature(s)
    locality['state_id'] = state_data['state_id'][0]
    locality['name'] = locality['OCD_ID'].str.extract('\\/\\w+\\:(\\w+\'?\\-?\\w+?)$')
    locality['external_identifier_type'] = state_data['external_identifier_type'][0]

    # Add election_administration_id
    election_admins = election_authorities[['ocd_division', 'election_administration_id']]
    # election_admins['ocd_division'] = election_admins['ocd_division'].str.replace('_town', '').str.replace('_city', '')
    locality = locality.merge(election_admins, left_on='OCD_ID', right_on='ocd_division', how='left')
    locality.drop('ocd_division', axis=1, inplace=True)
    locality.drop_duplicates(inplace=True)  # REMOVE duplicate rows from merge with election_authorities df

    # FORMAT feature(s)
    locality.rename(columns={'OCD_ID':'external_identifier_value'}, inplace=True)
    locality.reset_index(drop=True, inplace=True) 

    # CREATE locality id
    locality['id'] = 'loc' + (locality.index + 1).astype(str).str.zfill(4)
    
    return locality
  
def generate_election_administration(election_authorities):
    """
    PURPOSE: generates election_administration dataframe for .txt file
    INPUT: election_authorities
    RETURN: election_administration dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/election_administration.html
    """

    # CREATE/FORMAT feature(s)
    election_administration = election_authorities[['election_administration_id', 'homepage_url']]
    election_administration.drop_duplicates(inplace=True)
    election_administration.rename(columns={'election_administration_id':'id'}, inplace=True)
    election_administration.rename(columns={'homepage_url':'elections_uri'}, inplace=True)
    election_administration['absentee_uri'] = ''
    election_administration['am_i_registered_uri'] = ''
    election_administration['registration_uri'] = ''
    election_administration['rules_uri'] = ''
    election_administration['what_is_on_my_ballot_uri'] = ''
    election_administration['where_do_i_vote_uri'] = ''

    return election_administration
    
    
def generate_department(election_authorities):
    """
    PURPOSE: generates department dataframe for .txt file
    INPUT: election_authorities
    RETURN: department dataframe
    SPEC: https://vip-specification.readthedocs.io/en/latest/csv/element_files/department.html
    """
    # CREATE/FORMAT feature(s)
    department = election_authorities[['election_administration_id', 'election_official_person_id']]
    department.drop_duplicates(inplace=True)
    department['id'] = 'dep' + (department.index + 1).astype(str).str.zfill(4)

    return department

  
def generate_zip(state_abbrv, office_name, files):
    """
    PURPOSE: create .txt files and export into a folder for 1 state
    (election.txt, polling_location.txt, locality.txt, schedule.txt, source.txt, state.txt)
    INPUT: state_abbrv, files
    RETURN: exports zip of 8 .txt files
    """
    print()
    print('<<',office_name,'>>')
    # writing dataframes to txt files
    file_list = []
    for name, df in files.items():
        file = name + '.txt'
        file_list.append(file)
        df.to_csv(file, index=False, encoding='utf-8')
        
        print(state_abbrv, name, "|", len(df.index), "row(s)")

    # define name of zipfile
    zip_filename = 'vipfeed-' + str(state_feed['election_date'][0].date()) + '-' + state_feed['state_abbrv'][0] + '.zip'

    # writing files to a zipfile
    with ZipFile(zip_filename, 'w') as zip:
        for file in file_list:
            zip.write(file)

    return
           
    
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
    increment_success = 0 
    increment_httperror = 0
    increment_processingerror = 0
    
    # PROCESS each state individually
    for _, input_states in parser.parse_args()._get_kwargs(): # ITERATE through input arguments

        input_states = [state.upper() for state in input_states] # FORMAT all inputs as uppercase
        
        # GENERATE state_feed & election_authorities dataframe
        state_feed_all = pd.DataFrame(state_feed_values[1:],columns=state_feed_values[0])
        election_authorities_all = pd.DataFrame(election_authorities_values[0:],columns=election_authorities_values[0])
        election_authorities_all.drop([0], inplace=True)

        if 'ALL' in input_states:
            
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
                
                vip_build(state_data, state_feed, election_authorities)
                
                states_successfully_processed.append(state)
                increment_success +=1
                
                print() 

                
            except HttpError:
                print ('ERROR:', state, 'could not be found or retrieved from Google Sheets.')
                increment_httperror += 1
                
            except:
                print ('ERROR:', state, 'could not be processed.')
                increment_processingerror += 1

    print('Number of states that could not be found or retrieved from Google Sheets:', increment_httperror)
    print('Number of states that could not be processed:', increment_processingerror)
    print('Number of states that processed sucessfully:', increment_success)
    print()
    print('List of states that processed sucessfully:')
    print(states_successfully_processed)
    print()

                    
            

