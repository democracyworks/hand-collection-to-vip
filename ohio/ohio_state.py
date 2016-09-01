"""

election_administration_id,
external_identifier_type,
external_identifier_othertype,
external_identifier_value,
name,
polling_location_ids,
id #fips

Creates a list of of all polling_location_ids from locality.csv

"""

import csv
import config
import pandas as pd


def pl():
    # state_feed_file = 'state_feed_info.csv'
    early_voting_file = 'locality.csv'

    early_voting_path = "/home/acg/democracyworks/hand-collection-to-vip/ohio/output/" + early_voting_file
    colnames = ['source_election_administration_id', 'external_identifier_type', 'external_identifier_othertype',
                'external_identifier_value', 'name', 'source_polling_location_ids', 'state_id', 'type',
                'other_type', 'source_id']

    df = pd.read_csv(early_voting_path, names=colnames, encoding='utf-8', skiprows=1)
    # df['index'] = early_voting_df.index + 1

    polling_location_list = df['source_polling_location_ids'].tolist()
    return polling_location_list


# print polling_location_list


class StateTxt(object):

    def __init__(self):
        pass


    def election_administration_id(self):
        """#"""
        #st = config.state

        #for key, value in config.fips_dict.iteritems():
        #    if key == st.lower():
        #        state_num = value
        #       print state_num
        #       return 'ea' + str(state_num)
        return ''

    def external_identifier_type(self):
        """#"""
        return 'ocd-id'

    def external_identifier_othertype(self):
        """#"""
        return ''

    def get_external_identifier_value(self):
        """#"""
        return 'ocd-division/country:us/state:' + config.state_abbreviation.lower()

    def get_name(self):
        """#"""
        return config.state_upper

    def polling_location_ids(self):
        """#"""
        p = pl()
        p = [i.split(' ') for i in p]
        p =[item for sublist in p for item in sublist]
        return ' '.join(p)

    def create_state_id(self):
        """Creates state_id from fips code."""
        state = config.state

        for key, value in config.fips_dict.iteritems():
            if key == state.lower():
                state_num = value
                return 'st' + str(state_num)

    def id(self):
        pass

    def write_state_txt(self):
        # This method is going to populate the source CSV with all the necessary data

        with open(config.output + 'state.txt', 'ab') as f:
            fieldnames = ["election_administration_id", 'external_identifier_type',
                          'external_identifier_othertype', 'external_identifier_value',
                          'name', 'polling_location_ids', 'id']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'election_administration_id': self.election_administration_id(),
                             'external_identifier_type': self.external_identifier_type(),
                             'external_identifier_othertype': self.external_identifier_othertype(),
                             'external_identifier_value': self.get_external_identifier_value(),
                             'name': self.get_name(),
                             'polling_location_ids': self.polling_location_ids(),
                             'id': self.create_state_id()
                             })

if __name__ == '__main__':


    st = StateTxt()
    st.write_state_txt()