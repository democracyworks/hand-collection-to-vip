import pandas as pd
import time
from datetime import date
import csv



class SourceTxt(object):

    def __init__(self, state):
        self.state = state
        # self.base_df = base_df
        x = 1

    def get_date_time(self):
        # generate date-time, import date_time module
        date_string = date.today().strftime("%Y-%m-%d")
        time_string = time.strftime("%H:%M:%S")
        return date_string + "T" + time_string

    def get_vip_id(self):
        fips_dict = {"New York": 36, "New Jersey": 34, "Pennsylvania": 42}
        if self.state in fips_dict:
            return fips_dict[self.state]
        else:
            return ''

    def get_version(self):
        return "5.1"

    def get_name(self):
        return "Democracy Works"

#    def write_sourcetxt(self):
#        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

#        loc = self.build_sourcetxt_method()
        #print loc

        # Drop base_df columns.
#        loc.drop(['date_time', 'description', 'names', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version',



    def write_source_txt(self):
        output_path = "/home/acg/democracyworks/hand-collection-to-vip/test.csv"

        try:
            f = open(output_path, 'ab')
            fieldnames = ['name', 'vip_id', 'date_time']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({'name': self.get_name(), # TODO: call methods
                             'vip_id': self.get_vip_id(),
                             'date_time':  self.get_date_time()
                            })
        finally:
            f.close()

if __name__ == '__main__':

    arr = ["new york"]

    for i in arr:
        lt = SourceTxt(i)
        lt.write_source_txt()