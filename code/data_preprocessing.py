# This is a testing script for merging nber data with nber_subcategory dataset
#!/usr/bin/env python
import mrjob
from mrjob.job import MRJob
 
class NBER_merge(MRJob):
    
    # OUTPUT_PROTOCOL =  mrjob.protocol.TextValueProtocol
    #MRJob.SORT_VALUES = True

    def __init__(self, *args, **kwargs):
        super(NBER_merge, self).__init__(*args, **kwargs)
    

    def mapper(self, _, line):
        # Mapper will either get a record from main (nber) or join table (nber_category)
        try: # See if it is main table record
            if 'uuid' == line[:4]:    # skip header
                pass
            uuid, patent_id, category_id, subcategory_id = line.split('\t')
            table_id = 1    # nber.tsv
            yield subcategory_id, (table_id, patent_id, category_id, '')
        # testing
        #except ValueError:
        #pass
        except ValueError:
            try: # See if it is a join table record
                if 'id' == line[:2]:
                    pass
                subcategory_id, title = line.split('\t')
                table_id = 2
                yield subcategory_id, (table_id, '', '', title)
            except ValueError:
                pass # Record did not match either so skip the record
 
    def reducer(self, key, values):
        # testing
        # max_id = 0
        # for table_id, patent_id, category_id, title in values:
        #    if table_id > max_id:
        #        max_id = table_id
        # yield key, max_id
        
        tit = None
        
        s_values = sorted(values, 
            key=lambda x: x[0], reverse = True) 

        for table_id, patent_id, category_id, title in s_values:
            if table_id == 2:
                tit = title
            else: yield patent_id, tit

if __name__ == '__main__':
    NBER_merge.run()