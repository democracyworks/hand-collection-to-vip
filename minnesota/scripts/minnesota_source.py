import csv
import time
from datetime import date

import config


class SourceTxt(object):

    def __init__(self, state):
        self.state = state

    def get_date_time(self):
        # generate date-time, import date_time module--follows the format provided in the documentation
        date_string = date.today().strftime("%Y-%m-%d")
        time_string = time.strftime("%H:%M:%S")
        return date_string + "T" + time_string


    def get_vip_id(self):
        #the vip_id is the FIPS code for the state, which is available in the dictionary below
        if self.state in config.fips_dict:
            p = config.fips_dict[self.state]
            print p
            return p
        else:
            return ''

    def get_version(self):
        #current version was 5.1 for all instances
        return "5.1"

    def get_description(self):
        return ''

    def get_organization_uri(self):
        #was instructed to leave this field blank
        return ''

    def get_terms_of_use_uri(self):
        #was instructed to leave this field blank
        return ''

    def get_name(self):
        #name is Democracy works for all instances
        return "Democracy Works"

    def get_id(self):
        return 'source01'


    def write_source_txt(self):
        # This method is going to populate the source CSV with all the necessary data

        with open(config.source_output, 'ab') as f:
            fieldnames = ["date_time", 'description', 'name', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version',
                          'id']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'date_time': self.get_date_time(),
                             'description': self.get_description(),
                             'name': self.get_name(),
                             'organization_uri': self.get_organization_uri(),
                             'terms_of_use_uri': self.get_terms_of_use_uri(),
                             'vip_id': self.get_vip_id(),
                             'version': self.get_version(),
                             'id': self.get_id()
                             })

if __name__ == '__main__':

    state = config.state

    SourceTxt(state).write_source_txt()
