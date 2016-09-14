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
from sd_precinct_dask import PrecinctTxt

f = 'tsmart_google_geo_20160817_SD_20150914.txt'
#f = 'tsmart_google_geo_20160817_OH_20160708.txt'
#f = 'tsmart_google_geo_20160817_ME_20160712.txt'

voter_file = "/home/acg/democracyworks/hand-collection-to-vip/dev_scripts/source/" + f


class GenerateStreetSegDependencies(object):

    def __init__(self, pre_street_seg_df):
        self.pre_street_seg_df = pre_street_seg_df


    def groupby_street(self):
        grouped_by_street = self.pre_street_seg_df.groupby(
            ['vf_reg_cass_city', 'vf_reg_cass_street_name', 'van_precinctid']).agg(lambda x: ', '.join(x)).reset_index()
        # print grouped_by_street
        # grouped_by_street.to_csv(config.output + 'check_street_group2.txt', index=False, encoding='utf-8')

        grouped_by_street2 = grouped_by_street.groupby(
            ['vf_reg_cass_city', 'vf_reg_cass_street_name', ]).agg(lambda x: ', '.join(x)).reset_index()
        del grouped_by_street

        grouped_by_street2.rename({'van_precinctid': 'includes_all_boolean_data'})
        #print grouped_by_street2
        grouped_by_street2.to_csv(config.output + 'check_street_group2.txt', index=False, encoding='utf-8')
        # Todo: groupby again -- street name and city
        return grouped_by_street2

    def groupby_precinct(self):
        """Merge with Dataframe from the precinct scripts"""

        #group = self.pre_street_seg_df.groupby(['van_precinctid', 'vf_reg_cass_street_name',
        #                                        'vf_reg_cass_street_num'])

        grouped_by_precinct = self.pre_street_seg_df.groupby(
        #grouped_by_precinct = group.groupby(
            ['van_precinctid', 'vf_reg_cass_street_name', 'vf_reg_cass_street_num']).agg(
            lambda x: ', '.join(x)).reset_index()
        # TODO: handle fractions in house number here, and any other validation
        # by joining with a comma that can later be split fractions can be handled further in the process
        del self.pre_street_seg_df

        grouped_by_precinct2 = grouped_by_precinct.groupby(['van_precinctid', 'vf_reg_cass_street_name']).agg(
            lambda x: ', '.join(x)).reset_index()
        del grouped_by_precinct
        # print grouped2
        grouped_by_precinct2.to_csv(config.output + 'check_precinct_group.txt', index=False, encoding='utf-8')

        grouped_by_precinct2['merge_key'] = 'pre' + grouped_by_precinct2['van_precinctid']
        #print grouped_by_precinct2
        return grouped_by_precinct2


class StreetSegmentsTxt(object):
    """#"""

    def __init__(self, merged_df):
        self.base_df = merged_df
        #print self.base_df

    def get_address_direction(self, vf_reg_cass_pre_directional):
        """#"""
        return vf_reg_cass_pre_directional


    def get_city(self, vf_reg_cass_city):
        """#"""
        return vf_reg_cass_city

    def includes_all_addresses(self):
        """#"""
        # TODO: for includes_all_addresses - groupby street and city. if set(precinct_ids) >=2: includes_all... = false
        return ''

    def includes_all_streets(self):
        """#"""
        return ''

    def odd_even_both(self, index, vf_reg_cass_street_num_x):
        """#"""

        if len(vf_reg_cass_street_num_x) >=1:

            print "row" + ' ' + str(index)
            oel = vf_reg_cass_street_num_x.split(',')  # create list from grouped street number string
            #oel = [i.isdigit() for i in oel]
            oel = [x for x in oel if any(c.isdigit() for c in x)]  # keep any strings containing numbers
            #oel = [x for x in oel if x.isdigit()]
            #print oel

            clean_oel = [re.sub('[^0-9]', '', x) for x in oel]  # remove any non numeric values from strings
            #re.sub('[^0-9]', '', a)
            #print clean_oel

            odd_even_list = set(["even" if int(i) % 2 == 0 else "odd" for i in clean_oel])
            #print odd_even_list

            print odd_even_list
            # if dealing with floats instead of integers
            #odd_even_list = set(["even" if int(i * 10) % 2 == 0 else "odd" for i in vf_reg_cass_street_num_x.split(' ')])

            if 'even' in odd_even_list and 'odd' in odd_even_list:
                return 'both'

            elif len(odd_even_list) == 1 and 'even' in odd_even_list:
                return 'even'

            elif len(odd_even_list) == 1 and 'odd' in odd_even_list:
                return 'odd'
            else:
                print 'Non-numeric or missing street number value at row' + ' ' + str(index) + '.'
            #raise ValueError()
        else:
            raise ValueError()


    def get_precinct_id(self, van_precinctid_y):
        """#"""
        return van_precinctid_y

    def get_start_house_number(self, vf_reg_cass_street_num_x):
        """#"""

        # TODO: handle fractions, actually just get the first element, they're already ordered, add conditional, raise valueerror
        #return min([int(i) for i in vf_reg_cass_street_num_x.split(' ')])

        return [i for i in vf_reg_cass_street_num_x.split(',')][0]

    def get_end_house_number(self, vf_reg_cass_street_num_x):
        """#"""
        #return max([int(i) for i in vf_reg_cass_street_num_x.split(' ')])

        return [i for i in vf_reg_cass_street_num_x.split(',')][-1]

    def get_state(self, vf_reg_cass_state):
        """#"""
        return vf_reg_cass_state

    def get_street_direction(self, vf_reg_cass_pre_directional):
        """#"""
        return vf_reg_cass_pre_directional

    def get_street_name(self, vf_reg_cass_street_name):
        return vf_reg_cass_street_name

    def get_street_suffix(self, vf_reg_cass_street_suffix):
        """#"""
        return vf_reg_cass_street_suffix

    def get_unit_number(self):
        """#"""
        return ''

    def get_zip(self, vf_reg_cass_zip):
        """#"""
        return vf_reg_cass_zip

    def create_id(self, index):
        """Creates a sequential id by concatenating 'pre' with an 'index_str' based on the Dataframe's row index.
        '0s' are added, if necesary, to maintain a consistent id length.
        """

        # TODO: or use 'van_precinctid',

        if index <=9:
            index_str = '0000' + str(index)
            return 'ss' + index_str

        elif index in range(10,100):
            index_str = '000' + str(index)
            return 'ss' + index_str

        elif index in range(100, 1000):
            index_str = '00' + str(index)
            return 'ss' + index_str

        elif index:
            index_str = str(index)
            return 'ss' + index_str

        else:
            return ''

    def build_precinct_txt(self):
        """
        New columns that match the 'locality.txt' template are inserted into the DataFrame, apply() is
        used to run methods that generate the values for each row of the new columns.
        """

        self.base_df['address_direction'] = self.base_df.apply(
            lambda row: self.get_address_direction(row['vf_reg_cass_pre_directional']), axis=1)

        self.base_df['city'] = self.base_df.apply(
            lambda row: self.get_city(row['vf_reg_cass_city']), axis=1)

        self.base_df['includes_all_addresses'] = self.base_df.apply(
            lambda row: self.includes_all_addresses(), axis=1)

        self.base_df['includes_all_streets'] = self.base_df.apply(
            lambda row: self.includes_all_streets(), axis=1)

        self.base_df['odd_even_both'] = self.base_df.apply(
            lambda row: self.odd_even_both(row['index'], row['vf_reg_cass_street_num_x']), axis=1)

        self.base_df['precinct_id'] = self.base_df.apply(
            lambda row: self.get_precinct_id(row['van_precinctid_y']), axis=1)  # could also use 'merge_key"

        self.base_df['start_house_number'] = self.base_df.apply(
            lambda row: self.get_start_house_number(row['vf_reg_cass_street_num_x']), axis=1)

        self.base_df['end_house_number'] = self.base_df.apply(
            lambda row: self.get_end_house_number(row['vf_reg_cass_street_num_x']), axis=1)

        self.base_df['state'] = self.base_df.apply(
            lambda row: self.get_state(row['vf_reg_cass_state']), axis=1)

        self.base_df['street_direction'] = self.base_df.apply(
            lambda row: self.get_street_direction(row['vf_reg_cass_pre_directional']), axis=1)

        self.base_df['street_name'] = self.base_df.apply(
            lambda row: self.get_street_name(row['vf_reg_cass_street_name']), axis=1)

        self.base_df['street_suffix'] = self.base_df.apply(
            lambda row: self.get_street_suffix(row['vf_reg_cass_street_suffix']), axis=1)

        self.base_df['unit_number'] = self.base_df.apply(
            lambda row: self.get_unit_number(), axis=1)

        self.base_df['zip'] = self.base_df.apply(
            lambda row: self.get_zip(row['vf_reg_cass_zip']), axis=1)

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

    def counter(self):
        """#"""
        pass

    def write(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
        #print type(pt)
        #print pt
        # TODO: reindex

#        prt = pt.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
#                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
#                 'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4',
#                 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name',
#                 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
#                 'vf_reg_cass_apt_num', 'van_precinctid'], axis=1).compute()



        #pt.drop(pt.columns[[0, 1, 2, 3]], axis=1)
        #pt.set_index([])
        print pt

        pt.to_csv(config.output + 'street_segments5.txt', index=False, encoding='utf-8')  # send to txt file
        #pt.to_csv(config.output + 'precinct.csv', index=False, encoding='utf-8')  # send to csv file



def main():

    # create locality dataframe
    state_file = 'sd_early_voting_info.csv'

    colnames = ['ocd_division', 'email', 'county', 'location_name', 'address_1', 'address_2', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'start_date', 'end_date', 'is_only_by_appointment',
                'is_or_by_appointment', 'appointment_phone', 'is_subject_to_change']

    ev_df = pd.read_csv(config.input + state_file, names=colnames, encoding='utf-8', skiprows=1)
    ev_df['index'] = ev_df.index + 1
    #print  ev_df

    pl = PollingLocationTxt(ev_df)
    ev_df = pl.export_for_schedule_and_locality()

    lt = LocalityTxt(ev_df, config.state)
    locality_df = lt.export_for_precinct()

    # create state voter file dataframe from selected columns
    voter_file_ddf = dd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
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
                     dtype={'vf_county_code': str, 'vf_county_name': str, 'vf_reg_cass_zip': str}
                     #dtype='str'
                     )

    #print 'Building voter file dataframe.'

    # Add index and merge_key columns
    #voter_file_ddf['index'] = voter_file_ddf.index + 1
    voter_file_ddf['merge_key'] = voter_file_ddf['vf_county_name']

    # create dataframe passed to precinct.txt script
    merged_df = dd.merge(voter_file_ddf, locality_df, on='merge_key', how='outer')

    # Calls export_for_street_segments() from precinct script creating a dataframe containing relevant columns
    pr = PrecinctTxt(merged_df)
    precinct_text_ddf = pr.export_for_street_segments()

    #TODO: don't use dask's groupby, work on merge with a pre_street_seg file/df


    # Creates a dataframe from selected voter file columns. The df is grouped and merged
    pre_street_seg_df = pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns,
                     usecols=[ 'vf_reg_cass_street_num',
                               'vf_reg_cass_street_name',
                               #'vf_reg_cass_street_suffix',
                               'vf_reg_cass_apt_num',
                               'vf_reg_cass_city',
                              'van_precinctid'
                               # TODO:add full addressm, unit #
                               ],
                     encoding='ISO-8859-1',
                     skiprows=1,
                     #iterator=True,
                     #chunksize=1000,
                     dtype='str'
                     )

    # merge with precinct dataframe
    group_by_precinct = GenerateStreetSegDependencies(pre_street_seg_df).groupby_precinct()

    #TODO: MERGE ON MULTIPLE KEYS

    #street_seg_merged_df = precinct_text_ddf.merge(group_by_precinct, left_on=['van_precinctid', 'vf_reg_cass_street_name'], right_on=['merge_key', 'vf_reg_cass_street_name'], how='outer')
    street_seg_merged_df = group_by_precinct.merge(precinct_text_ddf, left_on=['merge_key', 'vf_reg_cass_street_name'], right_on=['van_precinctid', 'vf_reg_cass_street_name'], how='outer')
    #street_seg_merged_df = group_by_precinct.merge(precinct_text_ddf, left_on=['merge_key', 'vf_reg_cass_street_name'],
    #                                               right_on=['van_precinctid', 'vf_reg_cass_street_name'])

    street_seg_merged_df = street_seg_merged_df[street_seg_merged_df.van_precinctid_x.notnull()]  # drops rows without precinct_ids, no match during merge

    sseg_m = street_seg_merged_df.drop_duplicates(['van_precinctid_x', 'vf_reg_cass_street_name', 'vf_reg_cass_street_num_x'])
    del group_by_precinct
    #TODO: add index
    sseg_m['index'] = sseg_m.index + 1

    #sseg_m.to_csv(config.output + 'street_seg_merge_deduped7.txt', index=False, encoding='utf-8')

    # create StreetSegments instance
    st = StreetSegmentsTxt(sseg_m)
    st.write()

if __name__ == '__main__':

    main()