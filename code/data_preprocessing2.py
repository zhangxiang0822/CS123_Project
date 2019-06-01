#!/usr/bin/env python
########################################################################
# This script performs the second stage data merging/preprocessing
# we  merge the patent citation  (identified by unique citation id)
# with (patent_id, nber_industry) from the first merging script.

# main idea would be parsing both tables  grouped by patent id
# then sort and use a for-loop to broadcast the "industry" name in table 
# 2 ont table 1 (but this time we need to use multi-step because for each
# citation record, we have industry name for both citing and cited patents)
# within each group in the reducer.
########################################################################
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

class citation_merge(MRJob):
    
    # OUTPUT_PROTOCOL =  mrjob.protocol.TextValueProtocol
    #MRJob.SORT_VALUES = True

#    def __init__(self, *args, **kwargs):
#        super(citation_merge, self).__init__(*args, **kwargs)
    
    def configure_args(self):
        super(citation_merge, self).configure_args()
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
        orig_jobconf = super(citation_merge, self).jobconf()
        custom_jobconf = {
            'mapreduce.job.reduces': 1
        }
        return mrjob.conf.combine_dicts(orig_jobconf, custom_jobconf)


    def mapper(self, _, line):
        '''
        Mapper will either get a record from main (citation) or join table (nber industry pair)

        Then parse each line of them into the same key, value pair structure
        '''

        try: # See if it is main table record
            if 'uuid' == line[:4]:    # skip header
                pass
            uuid, patent_id, citation_id, date, name, \
                kind, country,  category, sequence = line.split('\t')
            table_id = 1    # nber.tsv
            # the second element in the values is the citing order
            # 1: citing patent
            # 2: cited patent
            yield str(patent_id), (table_id, 1, uuid,  date, sequence, "", "")    # second last item is industry of citing patent
            yield str(citation_id), (table_id, 2, uuid,  date, sequence, "", "")   # last item is industry of cited patent
        
        except ValueError:
            try: # See if it is a join table record
                patent_id, title = line.split('\t')
                patent_id = patent_id.strip('""')
                title = title.strip('""')
                table_id = 2
                yield patent_id, (table_id, 0, "", "", "", title, title)     # maintain the same structure
            except ValueError:
                pass # Record did not match either so skip the record


    def broadcast(self, key, values):
        '''
        This acts as the first reducer
        This will broadcast the corresponding nber industry name onto 
        BOTH citing patent and cited patent

        '''
        tit = None
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
                key=lambda x: x[0], reverse = True)    # sort based on table_id

        for table_id, citing_order, uuid,  date, \
            sequence, title_citing, title_cited in values:
            
            if (str(self.options.merger_type) == "left"):
                # case for left joiner
                if table_id == 2:
                    tit = title_citing
                elif citing_order == 1:
                    # our final desired key value structure:
                    #   uuid, citing_id, cited_id, citing_tit, cited_tit, date
                    yield uuid, (citing_order, key, "", tit, "", "")    # citing patent
                else:
                    yield uuid, (citing_order, "", key, "", tit, date)    # cited patent

            elif (str(self.options.merger_type) == "inner"):
                # case for outer joiner
                if table_id == 2:
                    tit = title_citing

                elif citing_order == 1 and tit:
                    # our final desired key value structure:
                    #   uuid, citing_id, cited_id, citing_tit, cited_tit, date
                    yield uuid, (citing_order, key, "", tit, "", "")    # citing patent
                elif citing_order == 2 and tit:
                    yield uuid, (citing_order, "", key, "", tit, date)    # cited patent


    def aggregator(self, key, values):
        '''
        This acts as the second reducer
        This will aggregate the citing patent and cited patent into 
        on citation record. maintain the same structure as input with 
        only two additional columns
        '''
        id_, tit, date_ = None, None, None
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
                key=lambda x: x[0], reverse = True)    # sort based on table_id

        for citing_order, citing_id, cited_id, \
            citing_tit, cited_tit, date in values:
            if (str(self.options.merger_type) == "left"):
                if citing_order == 2:
                    id_, tit, date_ = cited_id,  cited_tit, date
                else: yield key, (citing_id, id_, citing_tit, tit, date_)

            elif (str(self.options.merger_type) == "inner"):
                if citing_order == 2:
                    id_, tit, date_ = cited_id,  cited_tit, date
                elif tit: yield key, (citing_id, id_, citing_tit, tit, date_)


    def steps(self):
        return [MRStep(mapper=self.mapper,
                    reducer=self.broadcast),
            MRStep(reducer=self.aggregator)]

if __name__ == '__main__':
    citation_merge.run()