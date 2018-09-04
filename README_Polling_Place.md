# Program Overview   |   Polling Place Hand Collection 2018
###### Voting Information Project (VIP) Version 5.1

Program takes hand-collected state data related to polling place locations, standardizes the data, and outputs the data in a format that can be read by the VIP Dashboard.  The data was collected by the Democracy Works VIP Outreach Team.

States Processed in 2018:
AR, AZ, ME, NH, SD, TN, TX

<br> </br>

## How to run program 

```python <program file name> --nargs <state abbreviations or ‘all’>```

##### Sample run(s)
```python vip_pollingplace.py --nargs AR az sD Tn```

```python vip_pollingplace.py --nargs all```

<br> </br>

## Required input data

2 Google Sheets workbooks are required to run the program

### 1) Polling Place Hand Collecting
https://docs.google.com/spreadsheets/d/1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M/edit#gid=1405854820

##### Required sheets & features:

###### STATE_FEED sheet
office_name, ocd_division, election_date, election_name, state_abbrv, state_fips

###### STATE sheets 
county,	precinct,	location_name,	address_line,	directions,	start_time,	end_time,	start_date,	end_date

### 2) Election Authorities
https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198

##### Required sheets & features:

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_url, state

### Data Dictionary
https://docs.google.com/spreadsheets/d/19g4qfdMu16bULxHrs3RkWapgTVQ3siS1XJS9BRTA2cg/edit#gid=2035147249

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
* precinct.txt
* street_segment.txt

###### Documentation for the output files: 
https://vip-specification.readthedocs.io/en/latest/index.html

#### How to upload output file to VIP Dashboard:
Use the following command to upload a single zip file to the VIP Dashboard:

```sh upload_script_staging.sh  <state abbreviation>.zip```

##### Sample run(s)  
 
```sh upload_script_staging.sh vipfeed-pp-2018-11-06-TN.zip```

<br> </br>

## Print out sample
```
------------------------- Tennessee -------------------------

TN election | 1 row(s)
TN polling_location | 15 row(s)
TN schedule | 15 row(s)
TN source | 1 row(s)
TN state | 1 row(s)
TN locality | 1 row(s)
TN election_administration | 95 row(s)
TN department | 95 row(s)
TN person | 95 row(s)
TN precinct | 15 row(s)
TN street_segment | 4233 row(s)

Total precincts collected in polling place data: 12
Total precincts provided in TargetSmart data: 12

-------------------------------------------------------------


Number of states that could not be found or retrieved from Google Sheets: 0
Number of states that could not be processed: 0
Number of states that processed successfully: 1

List of states that processed successfully:
['TN']
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

