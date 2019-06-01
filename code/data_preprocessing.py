#!/usr/bin/env python
########################################################################
# This script performs the first stage data merging/preprocessing
# we first need to merge the patent data (identified by unique patent id)
# with NBER (National Bureau of Economics Research) subcategory industry 
# data by the unique NBER subcategory id.

# main idea would be parsing both tables  grouped by nber_subcategory id
# then sort and use a for-loop to broadcast the "industry" name in table 
# 2 ont table 1 within each group in the reducer.
########################################################################

import mrjob
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
 
class NBER_merge(MRJob):
    
    #INPUT_PROTOCOL = RawValueProtocol
    #OUTPUT_PROTOCOL = RawValueProtocol
    #MRJob.SORT_VALUES = True

    #def __init__(self, *args, **kwargs):
        #super(NBER_merge, self).__init__(*args, **kwargs)
        #self.__init__()

    def configure_args(self):
        super(NBER_merge, self).configure_args()
        # defining arguments regarding how two tables are merged
        # Possible values would be:
        #    1. "local": running on local machines
        #    2. "GCS": running on google cloud services
        #    3. "cluster": running on self-configured clusters
        self.add_passthru_arg('--merger_type', default = "left_join",
            help = "Type of merging")
        # defining arguments regarding where the code will be running
        # Possible values would be:
        #    1. "left": keep all main table records
        #    2. "outer": Keep records from both sides
        #    3. "inner": Keep only overlapping args
        self.add_passthru_arg('--runner_type', default = "local",
            help = "Whether the mapreducer is running on local or cloud")
        #self.add_file_arg('--table1', help = "Main table to be joined")
        #self.add_file_arg('--table2', help = "Secondary table to be joined")

    def jobconf(self):
        orig_jobconf = super(NBER_merge, self).jobconf()
        custom_jobconf = {
            'mapreduce.job.reduces': 1
        }
        return mrjob.conf.combine_dicts(orig_jobconf, custom_jobconf)

    def mapper(self, _, line):
        '''
        Mapper will either get a record from main (nber) or join table (nber_category)

        Then parse each line of them into the same key, value pair structure
        '''

        try: # See if it is main table record
            if 'uuid' == line[:4]:    # skip header
                pass
            uuid, patent_id, category_id, subcategory_id = line.split('\t')
            table_id = 1    # nber.tsv
            yield subcategory_id, (table_id, patent_id, category_id, '')
        
        except ValueError:
            try: # See if it is a join table record
                if 'id' == line[:2]:
                    pass
                subcategory_id, title = line.split('\t')
                table_id = 2    # nber_subcategory.tsv
                yield subcategory_id, (table_id, '', '', title)
            except ValueError:
                pass # Record did not match either so skip the record
 

    def reducer(self, key, values):
        #########################
        # A testing code to debug the discrepancy between 
        # output on local and on GCS
        # If we run the following code on 
        #
        # max_id = 0
        # for table_id, patent_id, category_id, title in values:
        #     if table_id > max_id:
        #        max_id = table_id
        # yield key, max_id
        #########################
        
        tit = None    # initialize the subcategory title
        # print(self.options.runner_type)
        if (str(self.options.runner_type) == "GCS" or
            str(self.options.runner_type) == "cluster"):
            #################
            # This sorting process is necessary only on GCS
            # or multi-instance clusters because mapper would 
            # automatically sort the reducer input on 1-instance 
            # local runner. 
            # While reducer input associated with the same key
            # coming from GCS would not be sorted. 
            #################
            values = sorted(values, 
                key=lambda x: x[0], reverse = True)
        #else:
        #    s_values = values
        

        for table_id, patent_id, category_id, title in values:
            if (str(self.options.merger_type) == "left"):
                # case for left joiner
                if table_id == 2:
                    tit = title
                else: yield patent_id, tit

            elif (str(self.options.merger_type) == "outer"):
                # case for outer joiner
                if table_id == 2:
                    tit = title
                yield patent_id, tit

            elif (str(self.options.merger_type) == "outer"):
                #case for inner joiner
                if table_id == 2:
                    tit = title
                elif tit: yield patent_id, tit
            

if __name__ == '__main__':
    NBER_merge.run()