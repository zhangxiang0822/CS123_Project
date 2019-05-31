# -*- coding: utf-8 -*-
"""
Created on Sat Jun  1 06:08:33 2019

@author: Xiang Zhang
"""

from mrjob.job import MRJob

class count_citation_byindustry(MRJob):
    def mapper(self, _, line):
        reader = line.split('\t')
        
        industry = reader[1].split(',')[2].strip("\"")
            
        yield industry, 1
            
    def combiner(self, industry, count):
        yield industry, sum(count)
          
    def reducer(self, industry, count):
        yield industry, sum(count)
        
if __name__ == '__main__':
    count_citation_byindustry.run()