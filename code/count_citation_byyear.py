# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 05:57:38 2019

@author: Xiang Zhang
"""

from mrjob.job import MRJob

class count_citation_byyear(MRJob):
    def mapper(self, _, line):
        reader = line.split('\t')
        if reader[0] != "uuid":
            year = reader[3].split('-')[0].strip()
            
            if int(year) <= 2018 and int(year) >= 1850:
                yield year, 1
            
    def combiner(self, year, count):
        yield year, sum(count)
          
    def reducer(self, year, count):
        yield year, sum(count)
        
if __name__ == '__main__':
    count_citation_byyear.run()