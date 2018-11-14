# Program Overview   |   Early Voting Hand Collection 2018
###### Voting Information Project (VIP) Version 5.1

Program takes hand-collected state data related to early voting locations, standardizes the data, and outputs the data in a format that can be read by the VIP Dashboard.  The data was collected by the Democracy Works VIP Outreach Team.

<br> </br>

## How to run program 

```python <program file name> -states <state abbreviations or ‘all’>```

##### Sample run(s)
```python vip_earlyvoting.py -states WY hi sD Nj```

```python vip_earlyvoting.py -states all```

<br> </br>

## Required input data

Two Google Sheets workbooks are required to run the program

### 1) Early Hand Collecting
https://docs.google.com/spreadsheets/d/1utF9ybiOcCc9GvZ_KMqKO1TDaVqUxmHl4xmK48YkZj4/edit#gid=892894361

##### Required sheets & features:

###### STATE_FEED sheet
official_name, ocd_division, election_date, election_name, state_abbrv, state_fips

###### STATE sheets 
OCD_ID,	location_name,	address_line,	directions,	start_time,	end_time,	start_date,	end_date,	is_only_by_appointment,	is_or_by_appointment,	is_drop_box,	is_early_voting,	internal_notes

### 2) Election Authorities
https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198

##### Required sheets & features:

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_url, state

<br> </br>

### Data Dictionary
https://docs.google.com/spreadsheets/d/1s3ZayYvPBGSmyXzxq1pNdKosbEoh6QnE85e4-l8CuUc/edit#gid=2035147249


<br> </br>

## Output

The tool outputs a single zip file per state feed, which contains the following 9 files:

* department.txt
* election_administration.txt
* election.txt
* locality.txt
* person.txt
* polling_location.txt
* schedule.txt
* source.txt
* state.txt

##### Documentation for the output files: 
https://vip-specification.readthedocs.io/en/latest/index.html

#### How to upload output file to VIP Dashboard:
Use the following command to upload a single zip file to the VIP Dashboard:

```sh upload_script_staging.sh  <state abbreviation>.zip```

##### Sample run(s)  
 
```sh upload_script_staging.sh vipfeed-ev-2018-11-06-SD.zip```

<br> </br>

## Print out sample
```
--------------------------------------------- ARIZONA ----------------------------------------------


                                             .txt Size                                              

                                           state | 1 row(s)
                                          source | 1 row(s)
                                        election | 1 row(s)
                         election_administration | 15 row(s)
                                      department | 15 row(s)
                                          person | 15 row(s)
                                        locality | 15 row(s)
                                polling_location | 88 row(s)
                                        schedule | 241 row(s)



                                   # of Unique Counties/Places                                      

                                      State Data | 15 counties/places
                            Election Authorities | 15 counties/places



                       ---------------------- WARNINGS ----------------------                       


                                            Missing Data                                            

                                           100, 126, 127                                            


                                  Problematic Cross-Street Formats                                  

                                         156, 158, 160, 164                                         


                              Missing Zipcodes from Location Addresses                              

                                      147, 239, 240, 241, 242                                       


                        Missing State Abbreviations from Location Addresses                         

                                      239, 240, 241, 242, 147                                       


____________________________________________________________________________________________________



                                           SUMMARY REPORT                                           


                               Final Status for All Requested States                                

                       Failed to load state data | 0 state(s) out of 1
                               Failed to process | 0 state(s) out of 1
                          Successfully processed | 1 state(s) out of 1


                                States that processed with warnings                                 

                                                 AZ                                                 


                                 States that sucessfully processed                                  

                                                 AZ                                                 

```

<br> </br>

## Explanation of common errors

##### 'States that failed to load state data'
Indicates the tab for the requested state is not in the Google Sheet doc. The tab might not be included or is misspelled.

##### 'States that failed to process & why'
Indicates a critical error in building the .txt files. The error might be a type or formatting issue. For debugging, comment out the try and except clauses. 

##### 'Error: ELECTION_AUTHORITIES Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates ELECTION_AUTHORITIES is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled.

##### 'Error: STATE_FEED Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates STATE_FEED is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled. 

<br> </br>

## Explanation of warnings

##### 'Missing Data'
Indicates that there are one or more empty fields in the corresponding rows of the Early Voting Hand Collection google sheet.  The warning detects missing values from all columns except 'directions', 'start_time', 'end_time', and 'internal_notes'.

##### 'Polling Locations have Multiple Directions'
Indicates that a polling location is listed with multiple values in the 'directions' field.  This can indicate that the location uses multiple rooms within the same building, depending on the time/day.  Alternatively, this can indicate a data collection mistake, particularly if one of the values is blank.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.  Each tuple in the list includes rows from a single polling location.
 
##### 'Problematic Cross-Street Formats'
Indicates that the address provided is an intersection (written as cross-streets) rather than street and house number. The warning detects '&' and 'and' strings in the address_line column.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.
 
##### 'Missing Zipcodes from Location Addresses'
Indicates that the address provided in the address_line column does not contain a zipcode.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.
 
##### 'Missing State Abbreviations from Location Addresses'
Indicates that the address provided in the address_line column does not contain a state abbreviation.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.
 
##### 'Incorrect OCD ID Formats'
Indicates that the OCD ID provided in the OCD_ID column is incorrect or has an unusual format.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.
 
##### 'Hours have ;'s Instead of :'s'
Indicates that there are semicolons in place of colons in the start_time or end_time columns.  This is a common data entry error and all instances must be corrected.  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.
 
##### 'Dates have Invalid Years'
Indicates that the year provided in the start_date or end_date columns is incorrect (does not match the election year).  The list of rows corresponds to row numbers in the Early Voting Hand Collection google sheet.

<br> </br>

## Notes
DC does not have election authority information listed in the Election Administration google sheet.  Therefore, the election_administration.txt, department.txt, and person.txt output files are blank (aside from headers).
