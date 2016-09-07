"""
Contains class that generates the 'street_segment.txt' file.

address_direction,
city,
includes_all_addresses,
includes_all_streets,
odd_even_both,
precinct_id,
start_house_number,
end_house_number,
state,
street_direction,
street_name,
street_suffix,
unit_number,
zip,
id

"""

import pandas as pd
import dask.dataframe as dd
import re
import config
import os
import tempfile
import sqlite3
from sd_locality import LocalityTxt
from sd_polling_location import PollingLocationTxt

f = 'tsmart_google_geo_20160817_SD_20150914.txt'
#f = 'tsmart_google_geo_20160817_OH_20160708.txt'

voter_file = "/home/acg/democracyworks/hand-collection-to-vip/dev_scripts/source/" + f



def main():


    # create locality dataframe
    state_file = 'sd_early_voting_info.csv'

    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/south_dakota/early_voting_input/" + state_file

    colnames = ['ocd_division', 'email', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change']

    ev_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
    ev_df['index'] = ev_df.index + 1
    #print  ev_df

    pl = PollingLocationTxt(ev_df)
    ev_df = pl.export_for_schedule_and_locality()

    lt = LocalityTxt(ev_df, config.state)
    lt_df = lt.export_for_precinct()


    # state voter file import
    colnames = ['voterbase_id', 'tsmart_exact_track', 'tsmart_exact_address_track', 'vf_voterfile_update_date',
                'vf_source_state',
                'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward',
                'vf_precinct_id',
                'vf_precinct_name', 'vf_county_council', 'vf_city_council', 'vf_municipal_district',
                'vf_school_district',
                'vf_judicial_district', 'reg_latitude', 'reg_longitude', 'reg_level', 'reg_census_id', 'reg_dma',
                'reg_dma_name', 'reg_place', 'reg_place_name', 'vf_reg_address_1', 'vf_reg_address_2',
                'vf_reg_city', 'vf_reg_state',
                'vf_reg_zip', 'vf_reg_zip4', 'vf_reg_cass_address_full', 'vf_reg_cass_city', 'vf_reg_cass_state',
                'vf_reg_cass_zip',
                'vf_reg_cass_zip4', 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional',
                'vf_reg_cass_street_name',
                'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                'vf_reg_cass_apt_num',
                'reg_address_usps_address_code', 'reg_address_carrier_route', 'reg_address_dpv_confirm_code',
                'reg_address_dpv_footnote', 'vf_mail_street', 'vf_mail_city', 'vf_mail_state', 'vf_mail_zip5',
                'vf_mail_zip4',
                'vf_mail_house_number', 'vf_mail_pre_direction', 'vf_mail_street_name', 'vf_mail_street_type',
                'vf_mail_post_direction', 'vf_mail_apt_type', 'vf_mail_apt_num', 'van_precinctid']

    df = dd.read_csv(voter_file, sep='\t', names=colnames,
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
                     dtype={'vf_county_code': str, 'vf_county_name': str}
                     )

    #df = dd.concat(df, ignore_index=True)
    df['index'] = df.index + 1

 #   print df

#    info = df.memory_usage(index=True, deep=False)
#    print info

    cols = ['index', 'vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_township', 'vf_ward', 'vf_precinct_id',
                    'vf_precinct_name', 'vf_county_council', 'reg_place_name', 'vf_reg_address_1', 'vf_reg_address_2',
                    'vf_reg_city', 'vf_reg_state', 'vf_reg_zip', 'vf_reg_zip4', 'vf_reg_cass_address_full',
                    'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
                    'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
                    'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'van_precinctid',
                    'election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
                    'external_identifier_value', 'name', 'polling_location_ids', 'state_id', 'type', 'other_type',
                    'id']

    #df = df.reindex(columns=cols )

    df['merge_key'] = df['vf_county_name']
    #print df


    merged_df = dd.merge(df, lt_df, on='merge_key', how='outer')
    print merged_df

#print merged
    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()


    #merged_df.to_csv(config.output + 'precinct_test.csv', index=False, encoding='utf-8')
#    merged_df.to_csv(config.output, index=False, encoding='utf-8')
    #return merged


#    con = sqlite3.connect('test4.db')

#    merged.to_sql('test', con, flavor='sqlite',
#               schema=None, if_exists='replace', index=True,
#               index_label=None, chunksize=None, dtype=None)

#    con.close()


class StreetSegmentTxt(object):
    """#


address_direction,
city,
includes_all_addresses,
includes_all_streets,
odd_even_both,
precinct_id,
start_house_number,
end_house_number,
state,
street_direction,
street_name,
street_suffix,
unit_number,
zip,
id
    """



    def __init__(self, merged_df):
        self.base_df = merged_df
        #print self.base_df

    @staticmethod
    def even(value):
        return value % 2 == 0

    def address_direction(self):
        """#"""
        return ''

    def city(self):
        """#"""
        return ''

    def includes_all_addresses(self):
        """#"""
        return ''

    def includes_all_streets(self):
        """#"""
        return ''

    def odd_even_both(self):
        """#"""
        return ''

    def precinct_id(self):
        """#"""
        pass

    def start_house_number(self):
        """#"""
        return ''

    def end_house_number(self):
        """#"""
        return ''

    def state(self):
        """#"""
        return ''

    def street_direction(self):
        """#"""
        return ''

    def street_name(self):
        return ''

    def street_suffix(self):
        pass

    def unit_number(self):
        """#"""
        return ''

    def get_zip(self):
        """#"""
        return ''



    def create_id(self, index):
        """Creates a sequential id by concatenating 'pre' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        # TODO: or use 'van_precinctid',
        # TODO: use 'van_precinctid' for groupby in street_segments, separate export method

        if index <=9:
            index_str = '0000' + str(index)
            return 'pre' + index_str

        elif index in range(10,100):
            index_str = '000' + str(index)
            return 'pre' + index_str

        elif index in range(100, 1000):
            index_str = '00' + str(index)
            return 'pre' + index_str

        elif index:
            index_str = str(index)
            return 'pre' + index_str

        else:
            return ''

    def build_precinct_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.

        address_direction,
city,
includes_all_addresses,
includes_all_streets,
odd_even_both,
precinct_id,
start_house_number,
end_house_number,
state,
street_direction,
street_name,
street_suffix,
unit_number,
zip,
id


        """


        self.base_df['address_direction'] = self.base_df.apply(
            lambda row: self.address_direction(), axis=1)

        self.base_df['city'] = self.base_df.apply(
            lambda row: self.city(), axis=1)

        self.base_df['includes_all_addresses'] = self.base_df.apply(
            lambda row: self.includes_all_addresses(), axis=1)

        self.base_df['includes_all_streets'] = self.base_df.apply(
            lambda row: self.includes_all_streets(), axis=1)

        self.base_df['odd_even_both'] = self.base_df.apply(
            lambda row: self.odd_even_both(), axis=1)

        self.base_df['precinct_id'] = self.base_df.apply(
            lambda row: self.precinct_id(), axis=1)

        self.base_df['start_house_number'] = self.base_df.apply(
            lambda row: self.start_house_number(), axis=1)

        self.base_df['end_house_number'] = self.base_df.apply(
            lambda row: self.end_house_number(), axis=1)

        self.base_df['state'] = self.base_df.apply(
            lambda row: self.state(), axis=1)

        self.base_df['street_direction'] = self.base_df.apply(
            lambda row: self.street_direction(), axis=1)

        self.base_df['street_name'] = self.base_df.apply(
            lambda row: self.street_name(), axis=1)

        self.base_df['street_suffix'] = self.base_df.apply(
            lambda row: self.street_suffix(), axis=1)

        self.base_df['unit_number'] = self.base_df.apply(
            lambda row: self.unit_number(), axis=1)

        self.base_df['zip'] = self.base_df.apply(
            lambda row: self.get_zip(), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index']), axis=1)

        #print self.base_df

        return self.base_df

#    def group_polling_location_ids(self, frame):
        #frame = self.build_locality_txt()
#        return pd.concat(g for _, g in frame.groupby("external_identifier_value") if len(g) > 1)
        #return frame.groupby('external_identifier_value')

#    def export_for_street_segments(self):
#        return self.build_precinct_txt()


    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
        #print type(pt)
        #print pt
        # TODO: reindex

        prt = pt.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                 'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
                 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
                 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                 'vf_reg_cass_apt_num', 'van_precinctid'], axis=1).compute()



        #pt.drop(pt.columns[[0, 1, 2, 3]], axis=1)
        #pt.set_index([])
        print pt

        # TODO: use standard csv write method

        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to txt file
        #pt.to_csv(config.output + 'precinct.csv', index=False, encoding='utf-8')  # send to csv file


if __name__ == '__main__':

    #s = address_direction,city,includes_all_addresses,includes_all_streets,odd_even_both,precinct_id,start_house_number,end_house_number,state,street_direction,street_name,street_suffix,unit_number,zip,id


    main()