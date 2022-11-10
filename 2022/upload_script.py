"""
upload_script.py --- Script that is intended to upload feeds to Dashboard
Programmer: Aaron Eisenberg
Last Updated: September 4, 2020
Last Updated by: Robert Mitchell
Notes: Uploads hand collection scripts to appropriate Dashboard folder
Sample run: "python3 upload_script.py vipfeed-[pp|ev]-YYYY-MM-DD-XX.zip"

Currently uses the staging bucket; eventually needs to move to data-suite-production-uploaded-feeds.
Update BUCKET accordingly once the production Dashboard can process 5.2 feeds.
"""

import os
import sys
import re

#dictionary that will convert state abbreviation to FIPS code
state_codes = {
            'AL': '01', 'AK': '02',             'AZ': '04', 'AR': '05', 'CA': '06',             'CO': '08', 'CT': '09', 
'DE': '10', 'DC': '11', 'FL': '12', 'GA': '13',             'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19', 
'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29', 
'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39', 
'OK': '40', 'OR': '41', 'PA': '42',             'RI': '44', 'SC': '45', 'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 
'VT': '50', 'VA': '51',             'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56',                                     'PR': '72'
}
def upload(ARG):
    if not os.path.isfile(ARG):
        raise Exception("Not a valid file")
    PATH, FILE = os.path.split(ARG)
    match = re.fullmatch(r"^vipfeed-(?:ev|pp)-\d{4}-\d{2}-\d{2}-\D{2}.zip$", FILE)
    if not match:
        raise Exception("Filename must match pattern 'vipfeed-[pp|ev]-YYYY-MM-DD-XX.zip'")
    if PATH:
        os.chdir(os.path.expanduser(PATH))
    ELECTION_DATE = "-".join(FILE.split("-")[2:5])
    FIPS = state_codes[FILE.split("-")[-1].split(".")[0]]
    print("Processing", FILE)
    
    #BUCKET needs upading, rest of os.system should run the same.
    BUCKET="data-dashboard-staging-unprocessed"
    #BUCKET="data-suite-production-uploaded-feeds" #will be the future bucket
    bucket_message = "-".join(BUCKET.split("-")[0:3])
    
    os.system('/usr/local/bin/aws s3 cp {0} s3://{1}/{2}/{3}/{0}'.format(FILE, BUCKET, FIPS, ELECTION_DATE))
    
    esc_quote = '\\"'
    
    message_body = '{{:filename {4}{0}/{1}/{2}{4} :bucket {4}{3}{4} :post-process-street-segments? true :notify? false}}'.format(FIPS, ELECTION_DATE, FILE, BUCKET, esc_quote)
    
    os.system('/usr/local/bin/aws sqs send-message --queue-url https://sqs.us-east-1.amazonaws.com/858394542481/{0} --message-body "{1}"'.format(bucket_message, message_body))
    
    print("Uploaded!")

if __name__ == '__main__':
    #File should be provided on the command line.
    ARG = sys.argv[1]
    upload(ARG)