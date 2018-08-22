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
from pima_county_locality import LocalityTxt
from pima_county_polling_location import PollingLocationTxt

# voter file
f = 'pima_county_vf.txt'

voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\arizona-pp\source\\" + f

def main2():
    m = MergePollingAndVF()
    merged_df = m.merge()

    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()

class MergePollingAndVF(object):

    def __init__(self):
        pass

    def create_polling_place_df(self):
        # build locality dataframe at run time
        colnames = ['county', 'official_title', 'types', 'ocd_division', 'division_description', 'homepage_url',
                    'phone',
                    'email', 'directions', 'precinct', 'address', 'city', 'state', 'zip_code', 'start_time', 'end_time',
                    'notes']

        polling_place_df = pd.read_csv(config.input_path + config.state_file,
                                      names=colnames, sep=',',
                                      encoding='ISO-8859-1',
                                      skiprows=1,
                                      dtype='str'
                                      )
        polling_place_df['index'] = polling_place_df.index + 1
        # print early_voting_df

        pl = PollingLocationTxt(polling_place_df)
        pp_df = pl.export_for_locality_precinct()
        # print '###', type(ev_df)
        del polling_place_df

        lt = LocalityTxt(pp_df, config.state)
        lt_df = lt.export_for_precinct()
        del pp_df

        lt_df['merge_key'] = lt_df.apply(
            lambda row: self.create_merge_key(row['county'], row['precinct']), axis=1)
        print lt_df
        return lt_df

    def create_voter_file_df(self):
        colnames = ['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
                    'vf_ward',
                    'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
                    'vf_reg_cass_city',
                    'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num',
                    'vf_reg_cass_pre_directional',
                    'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional',
                    'vf_reg_cass_unit_designator',
                    'vf_reg_cass_apt_num', 'van_precinctid']

        # state voter file import
        df = pd.read_csv(voter_file, sep=',', names=colnames, encoding='ISO-8859-1', skiprows=1, iterator=True,
                         chunksize=1000, dtype={'van_precinctid': 'str'})

        voter_file_df = pd.concat(df, ignore_index=True)

        # Drops duplicate rows based on county name and precinct name leaving a smaller dataframe with the necessary data for precinct.txt.
        voter_file_df = voter_file_df.drop_duplicates(subset=['vf_county_name', 'vf_precinct_name'])

        print voter_file_df.columns
        print 'from vf', voter_file_df.vf_precinct_name
        # print 'precinct id', pima_voter_file_df.van_precinctid
        del df
        # tn_voter_file_df = df
        return voter_file_df

    def create_merge_key(self, county, precinct):
        """For use when a version of the output is exported as a
        dataframe to the precinct script.

        The conditionals modify hand collected precinct names on a per county basis
        to match the equivalent name in the voter file.

        """

        return precinct

    def merge(self):
        pp = self.create_polling_place_df()
        vf = self.create_voter_file_df()

        merged_df = pd.merge(vf, pp, how='outer', left_on=['vf_precinct_name'], right_on=['merge_key'])

        merged_df = merged_df[
            merged_df['vf_reg_cass_street_name'].isin(['UOCAVA', 'CONFIDENTIAL', 'TEMPORARY ABSENCE']) == False]

        merged_df.to_csv(config.output + 'merge_test2.txt', index=False, encoding='utf-8')
        merged_df['index'] = merged_df.index + 1

        print merged_df
        return merged_df


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
        return 'loc0001'

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
        print polling_location_ids
        return polling_location_ids

    def create_precinct_split_name(self, van_precinctid):
        """#"""
        return ''

    def ward(self, vf_ward):
        """#"""
        return vf_ward

    def create_id(self, van_precinctid):
        """#"""

        return 'pre' + str(van_precinctid)

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

    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
#        print pt

        #prt = pt.drop_duplicates(subset=['polling_location_ids'])
        prt = pt.groupby(['id', 'name']).agg(lambda x: ' '.join(set(x))).reset_index()
        #print prt

        cols = ['ballot_style_id', 'electoral_district_ids', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'is_mail_only', 'locality_id', 'name', 'number', 'polling_location_ids',
                'precinct_split_name', 'ward', 'id']

        prt = prt.reindex(columns=cols)
        print prt

        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to txt file
        prt.to_csv(config.output + 'precinct.csv', index=False, encoding='utf-8')  # send to csv file



if __name__ == '__main__':

    main2()