"""
Contains class that generates the 'precinct.txt' file.

ballot_style_id,
electoral_district_ids,
external_identifier_type,
external_identifier_othertype,
external_identifier_value,
is_mail_only,
locality_id,
name,
number,
polling_location_ids,
precinct_split_name,
ward,
id




"""

import pandas as pd
#import dask.dataframe as dd
import csv
import numpy as np
import re
import config
import os
import tempfile
import sqlite3
from pima_county_locality import LocalityTxt
from pima_county_polling_location import PollingLocationTxt

# voter file
f = 'tsmart_google_geo_20160817_AZ_20160317.txt'
#f = 'tsmart_google_geo_20160817_NH_20160520.txt'

#voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\new_hampshire_pp\source\\" + f
voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\arizona-pp\source\\" + f


def main():


    # state voter file import
    #for df in pd.read_csv('matrix.txt', sep=',', header=None, chunksize=1):
    df = pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
    #for df in pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
                     usecols=['vf_source_state', 'vf_county_code', 'vf_county_name',
                              'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward', 'vf_precinct_id',
                              'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full', 'vf_reg_cass_city',
                              'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num',
                              'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix',
                              'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator', 'vf_reg_cass_apt_num',
                              'van_precinctid'],
                     encoding='ISO-8859-1',
                     skiprows=1,
                     iterator=True,
                     chunksize=1000,
                     # dtype={'vf_county_code': str, 'vf_county_name': str}
                     #dtype={'vf_reg_cass_zip': 'int64'},
                     dtype='str'
                     )

    voter_file_df = pd.concat(df, ignore_index=True)
    #voter_file_df = pd.concat([chunk[chunk['vf_county_name'] == 'PIMA'] for chunk in df])
    del df

    pima_voter_file_df = voter_file_df[voter_file_df['vf_county_name'] == 'PIMA']
    print pima_voter_file_df.count
    del voter_file_df


    pima_voter_file_df = pima_voter_file_df[pima_voter_file_df.vf_reg_cass_zip.notnull()]  # drop rows where zip code is missing
    pima_voter_file_df = pima_voter_file_df[pima_voter_file_df['vf_reg_cass_street_name'].isin(['UOCAVA', 'CONFIDENTIAL', 'TEMPORARY ABSENCE']) == False]
    pima_voter_file_df.to_csv(config.output + 'pima_county_vf.txt', index=False, encoding='utf-8')


if __name__ == '__main__':

    main()