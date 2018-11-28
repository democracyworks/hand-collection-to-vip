# Program Overview   |   Polling Place Hand Collection 2018
###### Voting Information Project (VIP) Version 5.1

Program matches TargetSmart data with hand-collected polling place data, standardizes the data, and outputs the data in a format that can be read by the VIP Dashboard.  The data was collected by the Democracy Works VIP Outreach Team.

States Processed in 2018:
ME, MT, NH, TN, TX, SD, AZ (Pima), FL (Bay, Osceola, St Lucie)

<br> </br>

## How to run program 

```python <program file name> -states <state abbreviations or ‘all’>```

##### Sample run(s)
```python vip_pollingplace.py -states AR az sD Tn```

```python vip_pollingplace.py -states all```

<br> </br>

## Required input data

Two Google Sheets workbooks and one TargetSmart file (per state) are required to run the program

### 1) Polling Place Hand Collecting
https://docs.google.com/spreadsheets/d/1o68iC82jt7WOoTYn_rdda2472Fn2_2_FRBsgZla5v8M/edit#gid=1405854820

##### Required sheets & features:

###### STATE_FEED sheet
office_name, ocd_division, election_date, election_name, state_abbrv, state_fips

###### STATE sheets (with each sheet named according to its state abbreviation, for example, 'NY')
county,	precinct,	location_name,	address_line,	directions,	start_time,	end_time,	start_date,	end_date

### 2) Election Authorities
https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o

##### Required sheets & features:

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_url, state

### 3) TargetSmart

##### Required features:

###### TargetSmart data
vf_precinct_name, vf_reg_address_1, vf_reg_address_2, vf_reg_city, vf_source_state, vf_reg_zip, vf_county_name, vf_township

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

##### Documentation for the output files: 
https://vip-specification.readthedocs.io/en/latest/index.html

#### How to upload output file to VIP Dashboard:
Use the following command to upload a single zip file to the VIP Dashboard:

```sh upload_script_staging.sh  <state abbreviation>.zip```

For batch uploads of all the zips in your folder, use the following command:

```for file in *.zip; do sh upload_script_staging.sh $file; done```

Before a batch upload, remove all the old zip files before running the program:

```rm *.zip```

Bonus: remove all folders within the folder:

```rm -r -- */```

##### Sample run(s)  
 
```sh upload_script_staging.sh vipfeed-pp-2018-11-06-TN.zip```


<br> </br>

## Print out sample
```
Timestamp: 2018-11-28 00:20:28


------------------------------------------ NEW HAMPSHIRE -------------------------------------------


                                             .txt Size                                              

                                           state | 1 row(s)
                                          source | 1 row(s)
                                        election | 1 row(s)
                         election_administration | 135 row(s)
                                      department | 135 row(s)
                                          person | 135 row(s)
                                        locality | 241 row(s)
                                polling_location | 312 row(s)
                                        precinct | 320 row(s)
                                        schedule | 312 row(s)
                                  street_segment | 480269 row(s)



                                       # of Unique Townships                                        

                                      State Data | 241 townships
                                     TargetSmart | 241 townships
                            Election Authorities | 135 townships


                --------------------- STATE DATA FATAL ERRORS ---------------------                 


                    Rows w/ Missing State Abbreviations from Location Addresses                     

                             13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23                             
                             24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34                             
                             35, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46                             
                             47, 48, 50, 51, 52, 53, 54, 55, 56, 57, 58                             
                             59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 70                             
                             71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81                             
                             82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92                             
                           93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103                         



                --------------------- STREET SEGMENT WARNINGS ---------------------                 


                                   # of Rows Missing Precinct Ids                                   

                                                3665                                                


                                 # of Rows Missing Address Numbers                                  

                                                208                                                 


                                 # of Rows Missing Street Suffixes                                  

                                                6585                                                


____________________________________________________________________________________________________





                                           SUMMARY REPORT                                           


                               Final Status for All Requested States                                

                       Failed to load state data | 0 state(s) out of 1
                               Failed to process | 0 state(s) out of 1
                          Successfully processed | 1 state(s) out of 1


                                States that processed with warnings                                 

                                                 NH                                                 


                                 States that sucessfully processed                                  

                                                 NH                                                 




Timestamp: 2018-11-28 00:21:22
Run time: 0.91 minute(s)                                            
```

<br> </br>

## Explanation of _general program errors_

##### 'States that failed to load state data'
Indicates the tab for the requested state is not in the Google Sheet doc. The tab might not be included or is misspelled.

##### 'States that failed to process'
Indicates a critical error in building the .txt files. The error might be a type or formatting issue. For debugging, comment out the try and except clauses. 

##### 'ERROR | ELECTION_AUTHORITIES Google Sheet is either missing from the workbook or there is data reading error.'
Indicates ELECTION_AUTHORITIES is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled.

##### 'ERROR | STATE_FEED Google Sheet is either missing from the workbook or there is data reading error.'
Indicates STATE_FEED is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled. 

##### 'ERROR | There is a database connection error.'
Indicates that there is an error connecting to the AWS MySQL database.

##### 'ERROR | TargetSmart data for <state_abbrv> is either missing from the database or there is data reading error.'
Indicates that there is an error reading the TargetSmart data from the AWS database.

<br> </br>

## Explanation of _STATE DATA WARNINGS_

##### 'Rows w/ Multiple Directions for the Same Polling Location'
Indicates that a polling location is listed with multiple unique values in the 'directions' field.  This can indicate that the location uses multiple rooms within the same building, depending on the time/day.  Alternatively, this can indicate a data collection mistake, particularly if one of the values is blank.  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet. Each tuple in the list includes rows from a single polling location.

##### 'Rows w/ Multiple Addresses for the Same Polling Location'
Indicates a paired `OCD_ID` and `location_name`, aka a unique polling location, has multiple addresses listed. This can indicate a spelling error or a mismatched `location_name`. The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet. Each tuple in the list includes rows from a single polling location.

##### 'Rows w/ Problematic Cross-Street Formats'
Indicates that the address provided is an intersection (written as cross-streets) rather than street and house number. The warning detects '&' and 'and' strings in the address_line column.  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet.

<br> </br>

## Explanation of _STATE DATA FATAL ERRORS_

##### 'Rows w/ Missing Data'
Indicates that there are one or more empty fields in the corresponding rows of the Early Voting Hand Collection Google Sheet.  The warning detects missing values from all columns except 'directions', 'start_time', 'end_time', and 'internal_notes'.

##### 'Rows w/ ;\'s Instead of :\'s in Start and/or End Hours'
Indicates that there are semicolons in place of colons in the start_time or end_time columns.  This is a common data entry error and all instances must be corrected.  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet.
 
##### 'Rows w/ Invalid Years in Start and/or End Dates'
Indicates that the year provided in the start_date or end_date columns is incorrect (does not match the election year).  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet.

##### 'Rows w/ Missing or Invalid Zipcodes from Location Addresses'
Indicates that the address provided in the address_line column does not contain a zipcode.  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet.
 
##### 'Rows w/ Missing State Abbreviations from Location Addresses'
Indicates that the address provided in the address_line column does not contain a state abbreviation.  The list of rows corresponds to row numbers in the Early Voting Hand Collection Google Sheet.

##### 'Rows w/ Mismatched Timezones between Start and End Times'
Indicates a hand collection error where the GMT adjustments are different for start and end times in the same row.
 
##### 'Rows w/ Polling Locations with Different Timezones'
Indicates a hand collection error where the same election day polling location has more than one unique GMT adjustment listed in start and/or end times.

## Explanation of _TARGET SMART WARNINGS_

##### '# of Rows Missing Townships'
Indicates the number of rows missing township values in the TargetSmart file.  This is especially important for states using townships rather than counties.

##### '# of Rows Missing Counties'
Indicates the number of rows missing county values in the TargetSmart file.  This is especially important for states using counties rather than townships.

##### '# of Missing State Abbreviations Filled'
Indicates the number of rows missing state abbreviation values in the TargetSmart file.

## Explanation of _MISMATCH WARNINGS_

##### 'Precincts in State Data not found in TargetSmart'
Indicates which precincts (if any) in the hand collected state data are not found in the TargetSmart data.

##### 'Precincts in TargetSmart not found in State Data'
Indicates which precincts (if any) in the TargetSmart data are not found in the hand-collected state data.

## Explanation of _STREET SEGMENT WARNINGS_

##### '# of Rows Missing Precinct Ids'
Indicates the number of addresses in the street segment data that do not have precinct ids.

##### '# of Rows Missing Address Numbers'
Indicates the number of addresses that are missing a house number.

##### '# of Rows Missing Street Suffixes'
Indicates the number of addresses that are missing street suffixes.


<br> </br>

## Notes
##### AZ 
- Script filters TargetSmart data for Pima County.  

##### FL
- Script filters TargetSmart data for Bay County, Osceola County, and St Lucie County. 
- Script hardcodes precinct names for several addresses in Osceola County.  

##### NH
- Script hardcodes precinct names for several addresses in Washington County.  

##### General
- Minimum python version required to run, primarily for f-strings: Python 3.7.0 
