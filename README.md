# Program Overview   |   Early Voting Hand Collection 2018
###### Voting Information Project (VIP) Version 5.1

Program takes hand-collected state data related to early voting locations, standardizes the data, and outputs the data in a format that can be read by the VIP Dashboard.  The data was collected by the Democracy Works VIP Outreach Team.

## How to run program 

```python ‘program file name’ --nargs <state abbreviations or ‘all’>```

##### Sample run
```python vip_build_v5.py --nargs WY hi sD Nj```

```python vip_build_v5.py --nargs all```

## Required input data

2 Google Sheets workbooks are required to run the program

### 1) Early Hand Collecting
https://docs.google.com/spreadsheets/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=892894361

##### Required sheets & features

###### STATE_FEED sheet
office_name, ocd_division, election_date, election_name, state_abbrv, state_fips

###### STATE sheets 
OCD_ID, location_name, address_1, dirs, start_time, end_time, start_date, end_date, is_only_by_appointment, is_or_by_appointment, is_drop_box, is_early_voting

### 2) Election Authorities

##### Required sheets & features

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_uri, state
