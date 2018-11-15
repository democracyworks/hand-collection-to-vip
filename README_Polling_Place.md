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
https://docs.google.com/spreadsheets/d/1bopYqaQzBVd0JGV9ymPiOsTjtlUCzyFOv6mUhjt_y2o/edit#gid=1572182198

##### Required sheets & features:

###### ELECTION_AUTHORITIES sheet
ocd_division, official_title, hompage_url, state

### 3) TargetSmart

##### Required features:

###### TargetSmart data
vf_precinct_name, vf_reg_address_1, vf_reg_address_2, vf_reg_city, vf_source_state, vf_reg_zip, vf_county_name, vf_township

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

##### Documentation for the output files: 
https://vip-specification.readthedocs.io/en/latest/index.html

#### How to upload output file to VIP Dashboard:
Use the following command to upload a single zip file to the VIP Dashboard:

```sh upload_script_staging.sh  <state abbreviation>.zip```

##### Sample run(s)  
 
```sh upload_script_staging.sh vipfeed-pp-2018-11-06-TN.zip```

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
                                        locality | 1 row(s)
                                polling_location | 241 row(s)
                                        precinct | 249 row(s)
                                        schedule | 241 row(s)
                                  street_segment | 340770 row(s)



                                    # of Unique Counties/Places                                     

                                      State Data | 1 counties/places
                                     TargetSmart | 1 counties/places
                            Election Authorities | 15 counties/places



                     ------------------ STATE DATA WARNINGS ------------------                      


                         Multiple Directions for the Same Polling Location                          

      (24, 191), (32, 33), (47, 66), (88, 231), (93, 229), (132, 182), (151, 226), (220, 227)       


                          Multiple Addresses for the Same Polling Location                          

                (17, 152), (20, 233), (28, 241), (36, 162), (43, 167), (55, 197)


                                  Problematic Cross-Street Formats                                  

                                               5, 77                                                



                     ------------------- MISMATCH WARNINGS -------------------                      


                          Precincts in State Data not found in TargetSmart                          

                                          ('PIMA', '027')                                           


                          Precincts in TargetSmart not found in State Data                          

                                  ('PIMA', '268'), ('PIMA', '279')                                  
                                  ('PIMA', '291'), ('PIMA', '294')                                  
                                  ('PIMA', '316'), ('PIMA', '332')                                  
                                  ('PIMA', '362'), ('PIMA', '368')                                  
                                                                                                    


                     ---------------- STREET SEGMENT WARNINGS ----------------                      


                                   # of Rows Missing Precinct Ids                                   

                                                 8                                                  


                                 # of Rows Missing Street Suffixes                                  

                                               53754                                                


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

##### 'States that failed to process'
Indicates a critical error in building the .txt files. The error might be a type or formatting issue. For debugging, comment out the try and except clauses. 

##### 'Error: ELECTION_AUTHORITIES Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates ELECTION_AUTHORITIES is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled.

##### 'Error: STATE_FEED Google Sheets is either missing from the Google workbook or there is data reading error.'
Indicates STATE_FEED is missing from the Google Sheet or has a read-in issue. The tab might either not be included or is misspelled. 

##### 'Error: There is a database connection error.'
Indicates that there is an error connecting to the AWS MySQL database.

##### 'ERROR | TargetSmart data for <state_abbrv> is either missing from the database or there is data reading error.'
Indicates that there is an error reading the TargetSmart data from the database.


<br> </br>

## Explanation of warnings

### State Data

##### 'Missing Data'
Indicates that there are one or more empty fields in the corresponding rows of the Polling Place Hand Collection Google Sheet.  The warning detects missing values from all columns except 'directions', 'start_time', 'end_time', and 'internal_notes'.

##### 'Multiple Directions for the Same Polling Location'
Indicates that a polling location is listed with multiple values in the 'directions' field.  This can indicate that the location uses multiple rooms within the same building, depending on the time/day.  Alternatively, this can indicate a data collection mistake, particularly if one of the values is blank.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.  Each tuple in the list includes rows from a single polling location.

##### 'Multiple Addresses for the Same Polling Location'
Indicates that a polling location name is listed with multiple addresses, within the same county.  This can indicate a data collection mistake or simply two separate polling locations with the same name.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.  Each tuple in the list includes rows from a single polling location.
 
##### 'Problematic Cross-Street Formats'
Indicates that the address provided is an intersection (written as cross-streets) rather than street and house number. The warning detects '&' and 'and' strings in the address_line column.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.
 
##### 'Missing Zipcodes from Location Addresses'
Indicates that the address provided in the address_line column does not contain a zipcode.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.
 
##### 'Missing State Abbreviations from Location Addresses'
Indicates that the address provided in the address_line column does not contain a state abbreviation.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.
 
##### 'Hours have ;'s Instead of :'s'
Indicates that there are semicolons in place of colons in the start_time or end_time columns.  This is a common data entry error and all instances must be corrected.  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.
 
##### 'Dates have Invalid Years'
Indicates that the year provided in the start_date or end_date columns is incorrect (does not match the election year).  The list of rows corresponds to row numbers in the Polling Place Hand Collection Google Sheet.

### TargetSmart Data

##### '# of Rows Missing Townships'
Indicates the number of rows missing township values in the TargetSmart file.  This is especially important for states using townships rather than counties.

##### '# of Rows Missing Counties'
Indicates the number of rows missing county values in the TargetSmart file.  This is especially important for states using counties rather than townships.

##### '# of Missing State Abbreviations Filled'
Indicates the number of rows missing state abbreviation values in the TargetSmart file.

### State Data & TargetSmart Data

##### 'Precincts in State Data not found in TargetSmart'
Indicates which precincts (if any) in the hand-collected state data are not found in the TargetSmart data.

##### 'Precincts in TargetSmart not found in State Data'
Indicates which precincts (if any) in the TargetSmart data are not found in the hand-collected state data.

### Street Segment Data

##### '# of Rows Missing Precinct Ids'
Indicates the number of addresses in the street segment data that do not have precinct ids.

##### '# of Rows Missing Address Numbers'
Indicates the number of addresses that are missing a house number.

##### '# of Rows Missing Street Suffixes'
Indicates the number of addresses that are missing street suffixes.


<br> </br>

## Notes
##### AZ 
- Script filters TargetSmart data for Pima County  

##### FL
- Script filters TargetSmart data for Bay County, Osceola County, and St Lucie County 
- Script hardcodes precinct names for several addresses in Osceola County  

##### NH
- Script hardcodes precinct names for several addresses in Washington County  

