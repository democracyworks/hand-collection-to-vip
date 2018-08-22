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
import config
from tx_locality import LocalityTxt
from tx_polling_location import PollingLocationTxt

# voter file


voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\texas_pp\source\\" + config.voter_file


def main():

    # build locality dataframe at run time
    colnames = ['county', 'ocd_division', 'directions', 'precinct', 'name', 'address1', 'city', 'state',
                'zip_code', 'start_time', 'end_time', 'index']

    #early_voting_df = pd.read_csv(config.input_path + config.state_file,
    polling_place_df = pd.read_csv(config.input_path + config.state_file,
                                  names=colnames, sep=',',
                                  encoding='ISO-8859-1',
                                  skiprows=1,
                                  #dtype={'merge_key': 'object'}
                                  #dtype={'zip_code': 'str'}
                                  )

    polling_place_df = polling_place_df[polling_place_df.address1.notnull()]
    polling_place_df = polling_place_df[polling_place_df.city.notnull()]
    polling_place_df['index'] = polling_place_df.index + 1
    #print early_voting_df

    pl = PollingLocationTxt(polling_place_df)
    ev_df = pl.export_for_locality_precinct()
    #print '###', type(ev_df)
    del polling_place_df

    lt = LocalityTxt(ev_df, config.state)
    lt_df = lt.export_for_precinct()
    print 'from locality', lt_df.merge_key
    #print len(lt_df)

    #print lt_df.merge_key
    #print '!!!', lt_df.dtypes
    del ev_df

    use = ['vf_source_state', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township', 'vf_ward', 'vf_precinct_id',
           'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full', 'vf_reg_cass_city', 'vf_reg_cass_state',
           'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num', 'vf_reg_cass_pre_directional',
           'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
           'vf_reg_cass_apt_num', 'van_precinctid']



    # state voter file import
    df = pd.read_csv(voter_file, sep=',', names=use,
                                 encoding='ISO-8859-1', skiprows=1, iterator=True,
                                 chunksize=1000,
                                 #dtype={'van_precinctid': 'str'},
                                 #nrows=999999
                                 #nrows=499999
                                 dtype='str'
                                )

    voter_file_df = pd.concat(df, ignore_index=True)
    voter_file_df['merge_key'] = voter_file_df['vf_county_name'] + '-' + voter_file_df['vf_precinct_name']
    #voter_file_df['merge_key'] = voter_file_df['vf_county_name'] + '-' + voter_file_df['vf_reg_cass_zip']
    print voter_file_df.merge_key
    del df

    # create dataframe from voter file and locality file/date
    merged_df = pd.merge(voter_file_df, lt_df, how='outer', on=['merge_key'])
    #print merged_df

    #merged_df = merged_df[merged_df.zip_code.notnull()]  # drop rows where zip code is missing
    merged_df = merged_df[merged_df['vf_reg_cass_street_name'].isin(['UOCAVA', 'CONFIDENTIAL', 'TEMPORARY ABSENCE']) == False]

    merged_df.to_csv(config.output + 'tx_merge_check.txt', index=False, encoding='utf-8')
    merged_df['index'] = merged_df.index + 1
    print merged_df

    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()
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

    def create_precinct_split_name(self, van_precinctid):
        """#"""
        return ''
        #return 'split' + str(van_precinctid) + '-' + str(index)

    def ward(self, vf_ward):
        """#"""
        return vf_ward

    def create_id(self, van_precinctid):
        """Uses id from voter file."""
        #print 'precinct id', van_precinctid

        return 'pre' + str(van_precinctid)

        # TODO: use 'van_precinctid',
        # TODO: use 'van_precinctid' for groupby in street_segments, separate export method

    def create_merge_key(self, van_precinctid):
        """#"""

        return 'pre' + str(van_precinctid)

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
            lambda row: self.create_precinct_split_name(row['van_precinctid']), axis=1)

        self.base_df['ward'] = self.base_df.apply(
            lambda row: self.ward(row['vf_ward']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['van_precinctid']), axis=1)

        self.base_df['sseg_merge_key'] = self.base_df.apply(
            lambda row: self.create_id(row['van_precinctid']), axis=1)

        return self.base_df


    def export_for_street_segments(self):
        test = self.build_precinct_txt()

        print 'here', test.columns

        # TODO: some of the columns should have been dropped earlier
        tp = test.drop(['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                 'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                 'vf_reg_cass_zip4', 'vf_reg_cass_post_directional', 'vf_reg_cass_unit_designator',
                 'vf_reg_cass_apt_num', 'merge_key', 'state_id', 'other_type', 'grouped_index',
                 'external_identifier_type', 'external_identifier_othertype', 'polling_location_ids', 'ballot_style_id',
                 'electoral_district_ids', 'is_mail_only', 'locality_id', 'precinct_split_name', 'ward'
                #'van_precinctid'
                 ], axis=1)
        del test

        #tp.to_csv(config.output + 'for_street_seg.txt', index=False, encoding='utf-8')

        return tp

    def final_build(self):
        # TODO: reset index on dataframe from build_precinct, add precinct split (precinct id + index)
        pass


    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
        print type(pt)
        #pt = pt[pt.external_identifier_value.notnull()]
#        print pt
        # TODO: reindex

        #prt = pt.drop_duplicates(subset=['polling_location_ids'])
        prt = pt.drop_duplicates(subset=['id'])

        cols = ['ballot_style_id', 'electoral_district_ids', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'is_mail_only', 'locality_id', 'name', 'number', 'polling_location_ids',
                'precinct_split_name', 'ward', 'id']

        prt = prt.reindex(columns=cols)
        #print prt

        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to csv file



if __name__ == '__main__':



    s = 'vf_source_state,vf_county_name,vf_cd,vf_sd,vf_hd,vf_township,vf_ward,vf_precinct_id,vf_precinct_name,vf_county_council,vf_reg_cass_address_full,vf_reg_cass_city,vf_reg_cass_state,vf_reg_cass_zip,vf_reg_cass_zip4,vf_reg_cass_street_num,vf_reg_cass_pre_directional,vf_reg_cass_street_name,vf_reg_cass_street_suffix,vf_reg_cass_post_directional,vf_reg_cass_unit_designator,vf_reg_cass_apt_num,van_precinctid'.split(',')
    print s


    main()