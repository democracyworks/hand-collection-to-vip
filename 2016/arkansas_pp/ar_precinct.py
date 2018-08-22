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
from ar_locality import LocalityTxt
from ar_polling_location import PollingLocationTxt

# voter file
f = 'tsmart_google_geo_20160817_AR_20160206.txt'

voter_file = "C:\Users\Aaron Garris\democracyworks\hand-collection-to-vip\\arkansas_pp\source\\" + f

class MergePollingPlacesVF(object):
    """
    """

    def __init__(self):
        pass

    def create_polling_place_df(self):

        # build locality dataframe at run time
        colnames = ['county', 'ocd_division', 'phone', 'email', 'precinct', 'directions', 'name', 'address1',
                    'address2', 'city', 'state', 'zip_code', 'start_time', 'end_time']

        # early_voting_df = pd.read_csv(config.input_path + config.state_file,
        polling_place_df = pd.read_csv(config.input_path + config.state_file,
                                       names=colnames, sep=',',
                                       encoding='ISO-8859-1',
                                       skiprows=1,
                                       dtype='str'
                                       )

        polling_place_df = polling_place_df[polling_place_df.address1.notnull()]
        polling_place_df = polling_place_df[polling_place_df.city.notnull()]
        polling_place_df['index'] = polling_place_df.index + 1

        pl = PollingLocationTxt(polling_place_df)
        pp_df = pl.export_for_locality_precinct()
        del polling_place_df

        lt = LocalityTxt(pp_df, config.state)
        lt_df = lt.export_for_precinct()

        lt_df['merge_key'] = lt_df.apply(
            lambda row: self.create_merge_key(row['county'], row['precinct']), axis=1)

        del pp_df
        return lt_df

    def create_voter_file_df(self):
        use = ['vf_source_state', 'vf_county_code', 'vf_county_name', 'vf_cd', 'vf_sd', 'vf_hd', 'vf_township',
               'vf_ward', 'vf_precinct_id', 'vf_precinct_name', 'vf_county_council', 'vf_reg_cass_address_full',
               'vf_reg_cass_city', 'vf_reg_cass_state', 'vf_reg_cass_zip', 'vf_reg_cass_zip4', 'vf_reg_cass_street_num',
               'vf_reg_cass_pre_directional', 'vf_reg_cass_street_name', 'vf_reg_cass_street_suffix', 'vf_reg_cass_post_directional',
               'vf_reg_cass_unit_designator', 'vf_reg_cass_apt_num', 'van_precinctid']

        # state voter file import
        df = pd.read_csv(voter_file, sep='\t', names=config.voter_file_columns, usecols=use,
                         encoding='ISO-8859-1', skiprows=1, iterator=True,
                         chunksize=1000,
                         dtype={'van_precinctid': 'str'},
                         )

        voter_file_df = pd.concat(df, ignore_index=True)

        # Drops duplicate rows based on county name and precinct name leaving a smaller dataframe with the necessary data for precinct.txt.
        voter_file_df = voter_file_df.drop_duplicates(subset=['vf_county_name', 'vf_precinct_name'])

        # TODO: This was an attempt to deal with the vote center issue. Not resolved.
        #    voter_file_df.loc[voter_file_df['vf_county_name'].isin(['GARLAND', 'BENTON', 'BOONE', 'COLUMBIA', 'FAULKNER',
        #                                                                  'MONTGOMERY']), 'vf_precinct_name'] = 'VOTE CENTER'
        #    voter_file_df = voter_file_df[voter_file_df['vf_county_name'].isin(['BENTON'])]
        #    print tn_voter_file_df.vf_county_name
        #    print 'HERE', tn_voter_file_df.vf_precinct_name

        voter_file_df['merge_key'] = voter_file_df['vf_county_name'] + '-' + voter_file_df['vf_precinct_name']

        return voter_file_df

    def create_merge_key(self, county, precinct):
        """Creates the value used to merge the polling place dataframe with the voter file dataframe.

        The conditionals modify hand collected precinct names on a per county basis, as needed,
        to match the equivalent name in the voter file.

        """

        county = county.upper().replace(' COUNTY', '')
        print precinct
        try:
            if county == 'BAXTER':
                mk = county + '-' + precinct.replace('-0', '-')
                print mk
                return mk
            elif county.upper() == 'VAN BUREN':
                mk = county + '-' + precinct.upper().replace(' - ', ' ').strip()
                print mk
                return mk
            elif county == 'PULASKI':
                return precinct
            elif county == 'JEFFERSON':
                if len(precinct) == 2:
                    return county + '-' + precinct
                else:
                    return precinct
            elif county == 'SALINE':
                if len(precinct) == 1:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'RANDOLPH':
                return county + '-' + precinct.replace('(', '').replace(')', ' -')
            elif county == 'SEBASTIAN':
                return county + '-PRECINCT ' + precinct.upper()
            elif county == 'GREENE':
                return county + '-' + precinct.replace('Ward ', '')
            elif county == 'JOHNSON':
                return county + '-' + precinct.upper()
            elif county == 'LOGAN':
                return county + '-' + [" - ".join(c.split(" ", 1)) for c in precinct]
            elif county == 'HOWARD':
                precinct = precinct.split(' - ')
                precinct = precinct[0] + ' - ' + precinct[1]
                return county + '-' + precinct
            elif county == 'MILLER':
                return county + '-' + precinct.title()
            elif county == 'CRAIGHEAD':
                if len(precinct) == 1:
                    return county + '-0' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'MARION':
                if len(precinct) == 1:
                    return county + '-Precinct ' + precinct
                else:
                    return county + '-' + precinct
            elif county == 'LINCOLN':
                return county + '-' + precinct
            elif county == 'MILLER':
                return county + '-' + precinct.title()
            elif county == 'MARION':
                return county + '-Precinct ' + precinct
            elif county == 'ST. FRANCIS':
                return precinct
            elif county == 'MISSISSIPPI':
                return precinct
            elif county == 'IZARD':
                return precinct
            elif county == 'POINSETT':
                return precinct
            elif county == 'OUACHITA':
                return precinct
            elif county == 'WASHINGTON':
                return precinct
            elif county == 'NEWTON':
                return precinct
            elif county == 'PRAIRIE':
                return precinct

            else:
                mk = county + '-' + precinct
                return mk
        except:
            pass

    def merge(self):
        pp = self.create_polling_place_df()
        vf = self.create_voter_file_df()

        # create dataframe from voter file and locality file/date
        merged_df = pd.merge(vf, pp, how='outer', on=['merge_key'])
        # print merged_df

        merged_df.to_csv(config.output + 'ark_merge_test.txt', index=False, encoding='utf-8')
        merged_df['index'] = merged_df.index + 1
        return merged_df


class PrecinctTxt(object):
    """#
    """

    def __init__(self, merged_df):
        self.base_df = merged_df
        #print self.base_df

    def ballot_style_id(self):
        """Not required. No data available."""
        return ''

    def electoral_district_ids(self):
        """Not required. No data available."""
        return ''

    def get_external_identifier_type(self):
        """Sets external_identifier_type value."""
        return "ocd-id"

    def get_external_identifier_othertype(self):
        """n/a"""
        return ''

    def get_external_identifier_value(self, external_identifier_value):
        """Extracts external identifier (ocd-division)."""
        return external_identifier_value

    def is_mail_only(self):
        """n/a"""
        return ''

    def locality_id(self, locality_id):
        """Sets locality_id value."""
        return locality_id

    def name(self, vf_precinct_name):
        """Sets name value."""
        return vf_precinct_name

    def number(self):
        """n/a"""
        return ''

    def get_other_type(self):
        """n/a"""
        return ''

    def polling_location_ids(self, polling_location_ids):
        return polling_location_ids

    def create_precinct_split_name(self, van_precinctid):
        """#"""
        return ''

    def ward(self, vf_ward):
        """Sets ward value"""
        return vf_ward

    def create_id(self, van_precinctid):
        """Uses id from voter file."""
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
            lambda row: self.polling_location_ids(row['polling_location_ids_for_precincts']), axis=1)

        self.base_df['precinct_split_name'] = self.base_df.apply(
            lambda row: self.create_precinct_split_name(row['van_precinctid']), axis=1)

        self.base_df['ward'] = self.base_df.apply(
            lambda row: self.ward(row['vf_ward']), axis=1)

        self.base_df['id'] = self.base_df.apply(
            lambda row: self.create_id(row['van_precinctid']), axis=1)

        return self.base_df

    def write_precinct_txt(self):
        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

        pt = self.build_precinct_txt()
        print pt

        pt = pt[pt['id'].isin(['prenan']) == False]

        prt = pt.drop_duplicates(subset=['id'])

        cols = ['ballot_style_id', 'electoral_district_ids', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'is_mail_only', 'locality_id', 'name', 'number', 'polling_location_ids',
                'precinct_split_name', 'ward', 'id']

        prt = prt.reindex(columns=cols)
        prt.to_csv(config.output + 'precinct.txt', index=False, encoding='utf-8')  # send to csv file


def main():
    m = MergePollingPlacesVF()
    merged_df = m.merge()

    pr = PrecinctTxt(merged_df)
    pr.write_precinct_txt()

if __name__ == '__main__':

    main()