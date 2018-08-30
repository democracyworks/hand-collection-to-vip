# Program Overview   |   Early Voting Hand Collection 2018
###### Voting Information Project (VIP) Version 5.1

Program takes hand-collected state data related to early voting locations, standardizes the data, and outputs the data in a format that can be read by the VIP Dashboard.  The data was collected by the Democracy Works VIP Outreach Team.

<br> </br>

## How to run program 

```python <program file name> --nargs <state abbreviations or ‘all’>```

##### Sample run(s)
```python vip_earlyvoting_build.py --nargs WY hi sD Nj```

```python vip_earlyvoting_build.py --nargs all```

<br> </br>

## Required input data

2 Google Sheets workbooks are required to run the program

### 1) Early Hand Collecting
https://docs.google.com/spreadsheets/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=892894361

##### Required sheets & features:

###### STATE_FEED sheet
office_name, ocd_division, election_date, election_name, state_abbrv, state_fips

###### STATE sheets 
OCD_ID, location_name, address_1, dirs, start_time, end_time, start_date, end_date, is_only_by_appointment, is_or_by_appointment, is_drop_box, is_early_voting

### 2) Election Authorities

##### Required sheets & features:

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_uri, state

### Data Dictionary
https://docs.google.com/spreadsheets/d/1s3ZayYvPBGSmyXzxq1pNdKosbEoh6QnE85e4-l8CuUc/edit#gid=2035147249

<br> </br>

## Output

The tool outputs a single zip file per state feed, which contains the following files:

* department.txt
* election_administration.txt
* election.txt
* locality.txt
* person.txt
* polling_location.txt
* schedule.txt
* source.txt
* state.txt

###### Documentation for the output files: 
https://vip-specification.readthedocs.io/en/latest/index.html

#### How to upload output file to VIP Dashboard:
Use the following command to upload a single zip file to the VIP Dashboard:

```sh upload_script_staging.sh  <state abbreviation>.zip```

##### Sample run(s)  
 
```sh upload_script_staging.sh vipfeed-2018-11-06-SD.zip```

<br> </br>

## Print out sample
```
<< South Dakota >>
SD election | 1 row(s)
SD polling_location | 69 row(s)
SD schedule | 698 row(s)
SD source | 1 row(s)
SD state | 1 row(s)
SD locality | 63 row(s)
SD election_administration | 66 row(s)
SD department | 66 row(s)
SD person | 66 row(s)

Number of states that could not be found or retrieved from Google Sheets: 0
Number of states that could not be processed: 0
Number of states that processed successfully: 1

List of states that processed successfully:
['SD']
```

<br> </br>

## Explanation of common errors

##### 'ERROR: <state> could not be found or retrieved from Google Sheets.'
Indicates the tab for the requested state is not in the Google Sheet doc. The tab might either not be included or is misspelled.

##### 'ERROR: <state> could not be processed.'
Indicates a critical error in building the .txt files. The error might be a type or formatting issue. For debugging, comment out the try and except clauses. 

##### 'Error: ELECTION_AUTHORITIES Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates ELECTION_AUTHORITIES is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled.

##### 'Error: STATE_FEED Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates STATE_FEED is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled. 

