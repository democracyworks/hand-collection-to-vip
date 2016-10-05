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
import dask.dataframe as dd
import csv
import numpy as np
import re
import config
import os
import tempfile
import sqlite3
from nh_locality import LocalityTxt
from nh_polling_location import PollingLocationTxt

#f = 'tsmart_google_geo_20160817_SD_20150914.txt'
#f = 'tsmart_google_geo_20160817_OH_20160708.txt'
f = 'tsmart_google_geo_20160817_NH_20160520.txt'

voter_file = "/home/acg/democracyworks/hand-collection-to-vip/new_hampshire/source/" + f



def main():



    colnames = ['county', 'official_title', 'ocd_division', 'homepage_url', 'phone', 'email', 'street', 'city',
                'state', 'zip_code', 'start_time', 'end_time', 'start_date', 'end_date']


    early_voting_df = pd.read_csv(config.input_path + config.state_file, names=colnames, encoding='ISO-8859-1', skiprows=1)
    early_voting_df['index'] = early_voting_df.index + 1
    #print early_voting_df

    #pl = PollingLocationTxt(early_voting_df, config.early_voting)

    #ev_df = pd.read_csv(config.input_path + config.state_file, names=colnames, encoding='utf-8', skiprows=1)
    #ev_df['index'] = ev_df.index + 1
    #print  ev_df

    pl = PollingLocationTxt(early_voting_df)
    ev_df = pl.export_for_schedule_and_locality()

    lt = LocalityTxt(ev_df, config.state)
    lt_df = lt.export_for_precinct()


    # state voter file import
    df = dd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
                     usecols=['vf_source_state', 'vf_county_code', 'vf_county_name',
                              'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward', 'vf_precinct_id',
                              'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full', 'vf_reg_cass_city',
                              'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num',
                              'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix',
                              'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator', 'vf_reg_cass_apt_num',
                              'van_precinctid'],
                     encoding='ISO-8859-1',
                     skiprows=1,
                     #iterator=True,
                     #chunksize=1000,
                     #dtype={'vf_county_code': str, 'vf_county_name': str}
                     dtype='str'
                     )

    #df = dd.concat(df, ignore_index=True)
    df['index'] = df.index + 1


    #
    pre_street_seg_df = pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
                     usecols=[ 'vf_reg_cass_street_num', 'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix',
                              'van_precinctid'],
                     encoding='ISO-8859-1',
                     skiprows=1,
                     #iterator=True,
                     #chunksize=1000,
                     dtype='str'
                     )

 #   print df

#    info = df.memory_usage(index=True, deep=False)
#    print info

#    cols = ['index', 'vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_township', 'vf_ward', 'vf_precinct_id',
#                    'vf_precinct_name', 'vf_county_council', 'reg_place_name', 'vf_reg_address_1', 'vf_reg_address_2',
#                    'vf_reg_city', 'vf_reg_state', 'vf_reg_zip', 'vf_reg_zip4', 'vf_reg_cass_address_full',
#                    'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
#                    'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
#                    'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'van_precinctid',
#                    'election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
#                    'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type', 'other_type',
#                    'id']

    #df = df.reindex(columns=cols )

    df['merge_key'] = df['vf_township']
    #print df


    merged_df = dd.merge(df, lt_df, on='merge_key', how='outer')
    print merged_df

#print merged
    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()
    #pr.write()
    #pr.export_for_street_segments()


class PrecinctTxt(object):
    """#
    """

    def __init__(self, merged_df):
        self.base_df = merged_df
        #print self.base_df

    def ballot_style_id(self):
        """#"""
        return ''

    def electoral_district_ids(self):
        """#"""
        return ''

    def get_external_identifier_type(self):
        """#"""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        """#"""
        return ''

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""
        return external_identifier_value

    def is_mail_only(self):
        """#"""
        pass

    def locality_id(self, locality_id):
        """Returns the value from the 'id_y' column. Columns appended with '_y' in the merged dataframe
         are from locality.txt"""
        return locality_id

    def name(self, vf_precinct_name):
        """#"""
        return vf_precinct_name

    def number(self):
        """#"""
        return ''

    def get_other_type(self):
        # create conditional when/if column is present
        return ''

    def polling_location_ids(self, polling_location_ids):
        return polling_location_ids

    def precinct_split_name(self):
        pass

    def ward(self, vf_ward):
        """#"""
        return vf_ward


    #def create_id(self, van_precinctid):
    #    """Creates a sequential id by concatenating 'pre' with an 'index_str' based on the Dataframe's row index.
    #    '0s' are added, if necesary, to maintain a consistent id length.
    #    """

    #    return 'pre' + van_precinctid

        # TODO: use 'van_precinctid',
        # TODO: use 'van_precinctid' for groupby in street_segments, separate export method

    def build_precinct_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """
        m = self.base_df

        self.base_df['ballot_style_id'] = self.base_df.apply(
            lambda row: self.ballot_style_id(), axis=1)

        self.base_df['electoral_district_ids'] = self.base_df.apply(
            lambda row: self.electoral_district_ids(), axis=1)

        self.base_df['external_identifier_type'] = self.base_df.apply(
            lambda row: self.get_external_identifier_type(), axis=1)

        self.base_df['external_identifier_othertype'] = self.base_df.apply(
            lambda row: self.get_external_identifier_othertype(), axis=1)

        self.base_df['external_identifier_value'] = self.base_df.apply(
            lambda row: self.get_external_identifier_value(row['external_identifier_value']), axis=1)

        self.base_df['is_mail_only'] = self.base_df.apply(
            lambda row: self.is_mail_only(), axis=1)

        self.base_df['locality_id'] = self.base_df.apply(
            lambda row: self.locality_id(row['id']), axis=1)

        self.base_df['name'] = self.base_df.apply(
            lambda row: self.name(row['vf_precinct_name']), axis=1)

        self.base_df['number'] = self.base_df.apply(
            lambda row: self.number(), axis=1)

        self.base_df['polling_location_ids'] = self.base_df.apply(
            lambda row: self.polling_location_ids(row['polling_location_ids']), axis=1)

        self.base_df['precinct_split_name'] = self.base_df.apply(
            lambda row: self.precinct_split_name(), axis=1)

        self.base_df['ward'] = self.base_df.apply(
            lambda row: self.ward(row['vf_ward']), axis=1)

        #self.base_df['id'] = self.base_df.apply(
        #    lambda row: self.create_id(), args=('van_precinctid'), meta=('new_van_precinctid', str), axis=1)

        # TODO: this should be renamed in the write method
        self.base_df['van_precinctid'] = self.base_df.apply(lambda row: 'pre' + str(row['van_precinctid'])[:-2], axis=1, meta=('new_van_precinctid', str))

        #print self.base_df

        return self.base_df


    def export_for_street_segments(self):
        test = self.build_precinct_txt()

        # TODO: some of the columns should have been dropped earlier
        tp = test.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                 'vf_reg_cass_zip4', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                 'vf_reg_cass_apt_num', 'merge_key', 'state_id', 'type', 'other_type', 'grouped_index',
                        'external_identifier_type', 'external_identifier_othertype', 'polling_location_ids',
                        'ballot_style_id', 'electoral_district_ids', 'is_mail_only', 'locality_id',
                        'precinct_split_name', 'ward'
                       #'van_precinctid'
                       ], axis=1).compute()
        del test


        #tp.rename({'van_precinctid': 'merge_key'})

        #tp.to_csv(config.output + 'for_street_seg.txt', index=False, encoding='utf-8')

        return tp

    def final_build(self):
        # TODO: reset index on dataframe from build_precinct, add precinct split (precinct id + index)
        pass


    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
        #print type(pt)
        print pt
        # TODO: reindex

        prt = pt.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                 'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
                 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
                 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                 'vf_reg_cass_apt_num', 'merge_key', 'state_id', 'type', 'other_type', 'grouped_index'
                       #'van_precinctid'
                       ], axis=1).compute()



        #pt.drop(pt.columns[[0, 1, 2, 3]], axis=1)
        #pt.set_index([])
        print pt

        # TODO: use itterrows and standard csv write method

        #pt.to_csv(config.output, index=False, encoding='utf-8')  # send to txt file
        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to txt file
        #pt.to_csv(config.output + 'precinct.csv', index=False, encoding='utf-8')  # send to csv file

    def write(self):
        pt = self.build_precinct_txt()

        prt = pt.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                 'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
                 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
                 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                 'vf_reg_cass_apt_num', 'merge_key', 'state_id', 'type', 'other_type', 'grouped_index'
                       #'van_precinctid'
                       ], axis=1).compute()

        with open(config.output + "precinct_test.txt",
                  'ab') as f:
            fieldnames = ["ballot_style_id", "electoral_district_ids", "external_identifier_type",
                          "external_identifier_othertype",
                          "external_identifier_value", "is_mail_only", "locality_id", "name", "number",
                          "polling_location_ids",
                          "precinct_split_name", "ward", "id"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in prt.itertuples():
                dict = {'ballot_style_id': row[10],
                        'electoral_district_ids': row[11],
                        'external_identifier_type': row[3],
                        'external_identifier_othertype': row[4],
                        'external_identifier_value': row[5],
                        'is_mail_only': row[12],
                        'locality_id': row[13],
                        'name': row[6],
                        'number': row[14],
                        'polling_location_ids': row[7],
                        'precinct_split_name': row[15],
                        'ward': row[16],
                        'id': row[9]
                        }
                writer.writerow(dict)


if __name__ == '__main__':

    main()