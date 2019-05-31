# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 06:40:33 2019

@author: Xiang Zhang
"""

from mrjob.job import MRJob

class count_citation_flow(MRJob):
    def mapper(self, _, line):
        reader = line.split('\t')
        
        industry1 = reader[1].split(',')[2].strip("\"")
        industry2 = reader[1].split(',')[3].strip("\"")
        
        yield (industry1, industry2), 1
            
    def combiner(self, industry_pair, count):
        yield industry_pair, sum(count)
          
    def reducer(self, industry_pair, count):
        yield industry_pair, sum(count)
        
if __name__ == '__main__':
    count_citation_flow.run()