# This script will add the column of industry classification for both citing and cited patents
#!/usr/bin/env python
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class citation_merge(MRJob):
    
    # OUTPUT_PROTOCOL =  mrjob.protocol.TextValueProtocol
    #MRJob.SORT_VALUES = True

    def __init__(self, *args, **kwargs):
        super(citation_merge, self).__init__(*args, **kwargs)
    

    def mapper(self, _, line):
        # Mapper will either get a record from main (citation) or join table (nber_category)
        try: # See if it is main table record
            if 'uuid' == line[:4]:    # skip header
                pass
            uuid, patent_id, citation_id, date, name, \
                kind, country,  category, sequence = line.split('\t')
            table_id = 1    # nber.tsv
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


    def reducer1(self, key, values):
        tit = None
        s_values = sorted(values, 
            key=lambda x: x[0], reverse = True)
        for table_id, citing_order, uuid,  date, \
            sequence, title_citing, title_cited in s_values:
            #print("you!!!!")
            if table_id == 2:
                tit = title_citing
            elif citing_order == 1:
                # our final desired key value structure:
                #   uuid, citing_id, cited_id, citing_tit, cited_tit, date
                yield uuid, (citing_order, key, "", tit, "", "")    # citing patent
            else:
                yield uuid, (citing_order, "", key, "", tit, date)    # cited patent


    def reducer2(self, key, values):
        id_, tit, date_ = None, None, None
        s_values = sorted(values, 
            key=lambda x: x[0], reverse = True)
        for citing_order, citing_id, cited_id, \
            citing_tit, cited_tit, date in s_values:
            if citing_order == 2:
                id_, tit, date_ = cited_id,  cited_tit, date
            else: yield key, (citing_id, id_, citing_tit, tit, date_)


    def steps(self):
        return [MRStep(mapper=self.mapper,
                    reducer=self.reducer1),
            MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    citation_merge.run()