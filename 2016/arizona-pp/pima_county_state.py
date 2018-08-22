"""

election_administration_id,
external_identifier_type,
external_identifier_othertype,
external_identifier_value,
name,
polling_location_ids,
id #fips


"""

import csv
import config
import pandas as pd



class StateTxt(object):
    """Creates VIP 5.1 state.txt document.

    """

    def __init__(self):
        pass


    def election_administration_id(self):
        """Not a required field. No data available."""
        return ''

    def external_identifier_type(self):
        """Sets external_identifier_type."""
        return 'ocd-id'

    def external_identifier_othertype(self):
        """N/A"""
        return ''

    def get_external_identifier_value(self):
        """Sets external_identifier_value (ocd-id)."""
        return 'ocd-division/country:us/state:' + config.state_abbreviation.lower() + '/county:pima'

    def get_name(self):
        """Sets state or county name."""
        return 'Pima County Arizona'

    def polling_location_ids(self):
        """Not a required field."""
        return ''

    def create_state_id(self):
        """Returns Pima County's fips code."""
        return 'st04019'

    def write_state_txt(self):
        """Populates source.txt with necessary data."""

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