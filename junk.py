def export_for_schedule(self):
    # intermediate_doc = self.build_polling_location_txt()

    # print intermediate_doc
    # intermediate_doc = self.dedupe(intermediate_doc)

    sch_intermediate_doc = self.build_polling_location_txt().drop_duplicates(
        subset=['start_time', 'end_time', 'start_date',
                'end_date', 'address_line'])
    sch_intermediate_doc.to_csv(config.output + 'sch_intermediate_doc.csv', index=False, encoding='utf-8')
    return sch_intermediate_doc


    # intermediate_doc.rename(columns={'id': 'polling_location_id'}, inplace=True)


def export_for_locality(self):
    loc_intermediate_doc = self.build_polling_location_txt()

    loci = loc_intermediate_doc.drop_duplicates(subset=['address_line'])

    # print intermediate_doc



    loci.to_csv(config.output + 'loc_intermediate_doc.csv', index=False, encoding='utf-8')
    return loci