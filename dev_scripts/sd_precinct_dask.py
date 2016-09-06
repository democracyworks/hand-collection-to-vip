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

#    def iter_merge(self):
#        mk_list = self.vf_merge_key_list
#        locality_df = self.locality
        #b = LocalityTxt().export_for_precinct()


#        for i in mk_list:
#            print type(i)

            #query = "SELECT * from test WHERE vf_county_name==?", ('PENNINGTON',)
#            query = "SELECT * from test WHERE vf_county_name==%s" % i
            # Read sqlite query results into a pandas DataFrame
#            con = sqlite3.connect("test2.db")
#            df = pd.read_sql(query, con)

#            print df

            #merged = pd.merge(df, locality_df, on='merge_key')
        #print merged

        # TODO: add conditional to set header flag to false after first row.

        #merged.reindex(columns=['ballot_style_id','external_identifier_value','merge_key'])

        #merged['external_identifier_value'] = merged.apply(
        #    lambda row: self.get_external_identifier_value(row['ocd_division']), axis=1)

#        merged.to_sql('test.db', con=con, flavor='sqlite', schema=None, if_exists='fail', index=True, index_label=None,
#                         chunksize=None, dtype=None)

#        p = tempfile.NamedTemporaryFile()
#        merged.to_csv(f, encoding="utf-8")




        #merged.to_csv(config.output + 'merge_test.csv', mode='a', header=True)
        #return merged

    def build_precinct_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

#        voter_file_df = self.base_df
#        locality_df = self.locality
        #b = LocalityTxt().export_for_precinct()

#        print voter_file_df
#        print locality_df
#        merged_df = pd.merge(voter_file_df, locality_df, on='merge_key')
#        print merged_df

        # TODO: add conditional to set header flag to false after first row.

        #merged_df.to_csv(config.output + 'merge_test.csv', mode='a', header=True)

#        merged_df = self.iter_merge()
#        print merged_df


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

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['index']), axis=1)

        #print self.base_df

        return self.base_df

#    def group_polling_location_ids(self, frame):
        #frame = self.build_locality_txt()
#        return pd.concat(g for _, g in frame.groupby("external_identifier_value") if len(g) > 1)
        #return frame.groupby('external_identifier_value')

    def export_for_street_segments(self):
        return self.build_precinct_txt()


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

        prt.to_csv(config.output + 'pr.text', index=False, encoding='utf-8')  # send to txt file
        #pt.to_csv(config.output + 'precinct.csv', index=False, encoding='utf-8')  # send to csv file



#class MergeLocalityVF(object):


#def main_old():

#    colnames = ['voterbase_id', 'tsmart_exact_track', 'tsmart_exact_address_track', 'vf_voterfile_update_date',
#                'vf_source_state',
#                'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward',
#                'vf_precinct_id',
#                'vf_precinct_name', 'vf_county_council', 'vf_city_council', 'vf_municipal_district',
#                'vf_school_district',
#                'vf_judicial_district', 'reg_latitude', 'reg_longitude', 'reg_level', 'reg_census_id', 'reg_dma',
#                'reg_dma_name', 'reg_place', 'reg_place_name', 'vf_reg_address_1', 'vf_reg_address_2',
#                'vf_reg_city', 'vf_reg_state',
#                'vf_reg_zip', 'vf_reg_zip4', 'vf_reg_cass_address_full', 'vf_reg_cass_city', 'vf_reg_cass_state',
#                'vf_reg_cass_zip',
#                'vf_reg_cass_zip4', 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional',
#                'vf_reg_cass_street_name',
#                'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
#                'vf_reg_cass_apt_num',
#                'reg_address_usps_address_code', 'reg_address_carrier_route', 'reg_address_dpv_confirm_code',
#                'reg_address_dpv_footnote', 'vf_mail_street', 'vf_mail_city', 'vf_mail_state', 'vf_mail_zip5',
#                'vf_mail_zip4',
#                'vf_mail_house_number', 'vf_mail_pre_direction', 'vf_mail_street_name', 'vf_mail_street_type',
#                'vf_mail_post_direction', 'vf_mail_apt_type', 'vf_mail_apt_num', 'van_precinctid']

#    df = pd.read_csv(voter_file, sep='\t', names=colnames, encoding='utf-8', skiprows=1,
#                     dtype={'vf_county_code': str, 'vf_county_name': str})

#    df_list = df['vf_county_name'].tolist()
#    df_list = [str(i) for i in df_list]
#    print df_list


#    state_file = 'sd_early_voting_info.csv'

#    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/south_dakota/early_voting_input/" + state_file

#    colnames = ['ocd_division', 'email', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
#                'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
#                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change']

#    ev_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
#    ev_df['index'] = ev_df.index + 1
    #print  ev_df

#    pl = PollingLocationTxt(ev_df)
#    ev_df = pl.export_for_schedule_and_locality()

#    lt = LocalityTxt(ev_df, config.state)
#    lt_df = lt.export_for_precinct()



#    pr = PrecinctTxt(df_list, lt_df)
#    pr.iter_merge()












if __name__ == '__main__':
    #s = 'ballot_style_id, electoral_district_ids, external_identifier_type, external_identifier_othertype, external_identifier_value, is_mail_only, locality_id, name, number, polling_location_ids, precinct_split_name, ward, id'.split(',')

    #s = 'vf_source_state	vf_county_code vf_county_name vf_township vf_ward vf_precinct_id vf_precinct_name vf_county_council reg_place_name vf_reg_address_1 vf_reg_address_2 vf_reg_city vf_reg_state vf_reg_zip vf_reg_zip4 vf_reg_cass_address_full vf_reg_cass_city vf_reg_cass_state vf_reg_cass_zip vf_reg_cass_zip4 vf_reg_cass_street_num vf_reg_cass_pre_directional vf_reg_cass_street_name vf_reg_cass_street_suffix vf_reg_cass_post_directional van_precinctid'.split(' ')
    #print s

    #s2 = 'election_administration_id,external_identifier_type,external_identifier_othertype,external_identifier_value,name,polling_location_ids,state_id,type,other_type,id'.split(',')


    #s.extend(s2)
    #print s

   # s = 'index vf_source_state vf_county_code vf_county_name vf_township vf_ward vf_precinct_id vf_precinct_name vf_county_council reg_place_name vf_reg_address_1 vf_reg_address_2 vf_reg_city vf_reg_state vf_reg_zip vf_reg_zip4 vf_reg_cass_address_full vf_reg_cass_city vf_reg_cass_state vf_reg_cass_zip vf_reg_cass_zip4 vf_reg_cass_street_num vf_reg_cass_pre_directional vf_reg_cass_street_name vf_reg_cass_street_suffix vf_reg_cass_post_directional van_precinctid election_administration_id_x external_identifier_type_x external_identifier_othertype_x external_identifier_value_x name_x polling_location_ids_x state_id_x type_x other_type_x id_x merge_key election_administration_id_y external_identifier_type_y external_identifier_othertype_y external_identifier_value_y name_y polling_location_ids_y state_id_y type_y other_type_y grouped_index id_y'.split(' ')
   # print s



    #ballot_style_id electoral_district_ids	external_identifier_type	external_identifier_othertype	external_identifier_value	is_mail_only	locality_id	name	number	polling_location_ids	precinct_split_name	ward	id


    main()


    # TODO: merge voter file with locality using ocd id as key. filter df into subset


#    state_file = 'sd_early_voting_info.csv'

    # early_voting_file = "/Users/danielgilberg/Development/hand-collection-to-vip/polling_location/polling_location_input/" + state_file
#    early_voting_file = "/home/acg/democracyworks/hand-collection-to-vip/south_dakota/early_voting_input/" + state_file

#    colnames = ['ocd_division', 'email', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
#                'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
#                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change']

#    ev_df = pd.read_csv(early_voting_file, names=colnames, encoding='utf-8', skiprows=1)
#    ev_df['index'] = ev_df.index + 1

    #pl = PollingLocationTxt(ev_df)
    #ev_df = pl.export_for_schedule_and_locality()

    #lt = LocalityTxt(ev_df, config.state)
    #lt_df = lt.export_for_precinct()
    #print lt_df

#    m = MergeLocalityVF()
#    m.create_df()
