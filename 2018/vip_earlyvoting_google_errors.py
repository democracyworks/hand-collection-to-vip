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
from IPython.display import display
import warnings
from prettypandas import PrettyPandas
warnings.filterwarnings('ignore')
pd.set_option('max_colwidth', 200)


# PRO-TIP: if modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# SET Google Spreadsheet IDs (2 IDs)

# states & STATE_FEED
# https://docs.google.com/spreadsheets/u/1/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=366784608
SPREADSHEET_ID = '1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4' 

# ELECTION_AUTHORITIES
# https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198
SPREADSHEET_EA_ID = '1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o'

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

                # FILTER state_feed and election_authorities
                state_feed = state_feed_all[state_feed_all['state_abbrv'] == state] # FILTER state_feed_all for selected state
                election_authorities = election_authorities_all[election_authorities_all['state'] == state] # FILTER election_authorities_all for selected state
                
                # CREATE dataframes of unique OCD IDs for election authorities and state data
                ea = election_authorities[['ocd_division']]
                ea.drop_duplicates(inplace=True)
                sd = state_data[['OCD_ID']]
                sd.drop_duplicates(inplace=True)

                title = 'Investigating Errors Reported by Google' # SET title
                length = (32*2)+len(title) # LENGTH of width
                
                # MERGE & PRINT lists
                print()
                print()
                print(title.center(length, ' '))
                print()
                print('(1) OCD IDs in hand collection data but not election authorities'.center(length, ' '))
                print('(2) OCD IDs in election authorities but not hand collection data'.center(length, ' '))
                print('(3) Empty values in hand collection data'.center(length, ' '))
                print()
                print('-'*length)
                print()
                print()
                print('(1)'.center(length, ' '))
                print()
                print('OCD IDs unique to hand collection data. They do not match any ids listed in election authorities.'.center(length, ' '))
                print()
                diff_state_data = sd.merge(ea, how = 'left', left_on = 'OCD_ID', right_on = 'ocd_division')
                diff_state_data = diff_state_data[diff_state_data['ocd_division'].isnull()]
                print(diff_state_data['OCD_ID'])
                print()
                print('WHAT THIS INDICATES:'.center(length, ' '), '\n')
                print('If there are OCD IDs listed here,'.center(length, ' '), '\n', 'they are likely mispelled or county/place is incorrect.'.center(length, ' '))
                print()
                print()
                print('-'*length)
                print()
                print()
                print('(2)'.center(length, ' '))
                print()
                print('OCD IDs unqiue to election authorities. They do not match any ids listed in hand collection data.'.center(length, ' '))
                print()
                diff_election_authorities = ea.merge(sd, how = 'left', left_on = 'ocd_division', right_on = 'OCD_ID')
                diff_election_authorities = diff_election_authorities[diff_election_authorities['OCD_ID'].isnull()]
                print(diff_election_authorities['ocd_division'])
                print()
                print('WHAT THIS INDICATES:'.center(length, ' '), '\n')
                print('If there are OCD IDs listed here,'.center(length, ' '), '\n', 'the county/place is likely missing entirely from hand collection data.'.center(length, ' '))
                print()
                print()
                print('-'*length)
                print()
                print()
                print('(3)'.center(length, ' '))
                print()
                print('Count of empty cells per column in hand collection data'.center(length, ' '))
                print()
                print(state_data.isnull().sum())
                print()
                print('If \'is_only_by_appointment\' includes \'TRUE\', then some empty cells are acceptable for \'is_only_by_appointment\'.')
                print(state_data['is_only_by_appointment'].value_counts())
                print()
                print('If \'is_or_by_appointment\' includes \'TRUE\', then some empty cells are acceptable for \'is_or_by_appointment\'.')
                print(state_data['is_or_by_appointment'].value_counts())
                print()
                print('If \'is_drop_box\' includes \'TRUE\', then some empty cells are acceptable for \'is_drop_box\'.')
                print(state_data['is_drop_box'].value_counts())
                print()
                print('WHAT THIS INDICATES:'.center(length, ' '), '\n')
                print('If there are empty cells,'.center(length, ' '), '\n', 'outreach may still be collecting data, data was mistakenly omitted,'.center(length, ' '),
                      '\n', 'or data is not properly loading and/or processing in the associated script.'.center(length, ' '))
                print()
                
                
                states_successfully_processed.append(state)
                increment_success +=1
                
                print() 

                
            except HttpError:
                print ('ERROR:', state, 'could not be found or retrieved from Google Sheets.')
                increment_httperror += 1
                
            except:
                print ('ERROR:', state, 'could not be processed.')
                increment_processingerror += 1


