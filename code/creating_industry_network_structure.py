##################################################
# This script wll create the industry2industry citation
# count matrix for final result visualization
##################################################

import mrjob
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class ind2ind(MRJob):
	'''
    This class will create an industry to industry matrix of count of patent citation
	'''

	def mapper(self, _, line):
		reader = line.split('\t')#.strip("]")#.strip("[")
		citing_ind = reader[1].split(',')[2].strip(" \"").strip("\"").strip("\" ")
		cited_ind = reader[1].split(',')[3].strip(" \"").strip("\"").strip("\" ")
		date_ = reader[1].split(',')[4].strip("]").strip(" \"").strip("\"").strip("\" ")
		year_ = date_.split("-")[0]
		try: 
			year_ = int(year_)
		except:
			year_ = 0
		# print(year_)
		# print(citing_ind)

		decade_ = None
		if year_ >= 1970 and year_ < 1980:
			decade_ = "1970s"
		elif year_ >= 1980 and year_ < 1990:
			decade_ = "1980s"
		elif year_ >= 1990 and year_ < 2000:
			decade_ = "1990s"
		elif year_ >= 2000 and year_ < 2010:
			decade_ = "2000s"
		elif year_ >= 2010 and year_ < 2020:
			decade_ = "2010s"

		if decade_ != None: yield (citing_ind, cited_ind, decade_), 1

	def combiner(self, key, value):
		yield key, sum(value)

	def reducer(self, key, value):
		yield key, sum(value)


if __name__ == "__main__":
	ind2ind.run()