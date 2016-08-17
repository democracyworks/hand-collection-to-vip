import pandas as pd
import time
from datetime import date
import csv



class SourceTxt(object):

    def __init__(self, state):
        self.state = state

    @classmethod
    def write_source_txt(cls, arr):
        #This method is going to populate the source CSV with all the necessary data
        output_path = "/Users/danielgilberg/Development/hand-collection-to-vip/test.csv"

        with open(output_path, 'ab') as f:
            fieldnames = ["date_time", 'description', 'name', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version', 'id']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in arr:
                writer.writerow({'date_time': item.get_date_time(),
                             'description': item.get_description(),
                             'name':  item.get_name(),
                             'organization_uri': item.get_organization_uri(),
                             'terms_of_use_uri': item.get_terms_of_use_uri(),
                             'vip_id': item.get_vip_id(),
                             'version': item.get_version(),
                             'id': item.get_id()
                            })



    def get_date_time(self):
        # generate date-time, import date_time module--follows the format provided in the documentation
        date_string = date.today().strftime("%Y-%m-%d")
        time_string = time.strftime("%H:%M:%S")
        return date_string + "T" + time_string


    def get_vip_id(self):
        #the vip_id is the FIPS code for the state, which is available in the dictionary below
        fips_dict = {'wyoming': '56', 'colorado': '08', 'washington': '53', 'hawaii': '15', 'tennessee': '47', 'iowa': '19',
         'nevada': '32', 'maine': '23', 'north dakota': '38', 'mississippi': '28', 'south dakota': '46',
         'new jersey': '34', 'oklahoma': '40', 'delaware': '10', 'minnesota': '27', 'north carolina': '37',
         'illinois': '17', 'new york': '36', 'arkansas': '05', 'indiana': '18', 'maryland': '24', 'louisiana': '22',
         'texas': '48', 'district of columbia': '11', 'arizona': '04', 'wisconsin': '55', 'michigan': '26',
         'kansas': '20', 'utah': '49', 'virginia': '51', 'oregon': '41', 'connecticut': '09', 'montana': '30',
         'california': '06', 'new mexico': '35', 'rhode island': '44', 'vermont': '50', 'georgia': '13',
         'pennsylvania': '42', 'florida': '12', 'alaska': '02', 'kentucky': '21', 'missouri': '29', 'nebraska': '31',
         'new hampshire': '33', 'idaho': '16', 'west virginia': '54', 'south carolina': '45', 'ohio': '39',
         'alabama': '01', 'massachusetts': '25'}

        if self.state in fips_dict:
            return fips_dict[self.state]
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
        return ''

#    def write_sourcetxt(self):
#        """Drops base DataFrame columns then writes final dataframe to text or csv file"""

#        loc = self.build_sourcetxt_method()
        #print loc

        # Drop base_df columns.
#        loc.drop(['date_time', 'description', 'names', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version',

    #date_time, description, name, organization_uri, terms_of_use_uri, vip_id, version, id
    # def write_source_txt(cls, arr):
    #     output_path = "/Users/danielgilberg/Development/hand-collection-to-vip/test.csv"
    #
    #     try:
    #         f = open(output_path, 'ab')
    #         fieldnames = ["date_time", 'description', 'name', 'organization_uri', 'terms_of_use_uri', 'vip_id', 'version', 'id']
    #         writer = csv.DictWriter(f, fieldnames=fieldnames)
    #         writer.writeheader()
    #         for item in arr:
    #             writer.writerow({'date_time': item.get_date_time(),
    #                          'description': item.get_vip_id(),
    #                          'name':  item.get_name(),
    #                          'organization_uri': item.get_organization_uri(),
    #                          'terms_of_use_uri': item.get_terms_of_use_uri(),
    #                          'vip_id': item.get_vip_id(),
    #                          'version': item.get_version(),
    #                          'id': item.get_id()
    #                         })
    #     finally:
    #         f.close()

if __name__ == '__main__':

    arr = ["Alabama"]
    sources = []
    for i in arr:
        lt = SourceTxt(i)
        sources.append(lt)


    SourceTxt.write_source_txt(sources)