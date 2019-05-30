# -*- coding: utf-8 -*-
"""
Created on Tue May 28 10:50:01 2019

@author: Xiang Zhang
"""

import pandas as pd
import sys

df = pd.read_csv('../data/sample_data.tsv', delimiter = "\t")
df = df.loc[0:100, :]

list_unique_patent_id = pd.unique(df[['patent_id', 'citation_id']].values.ravel('k'))

dict_unique_patent_id = {}
for index, item in enumerate(list_unique_patent_id):
    dict_unique_patent_id[item] = index

graph_dict = {}
for index, item in df.iterrows():
    patent_id   = dict_unique_patent_id[item["patent_id"]]
    citation_id = dict_unique_patent_id[item["citation_id"]]
    
    if patent_id not in graph_dict:
        graph_dict[patent_id] = str(citation_id)
    else:
        graph_dict[patent_id] += "," + str(citation_id)
        
df_graph = pd.DataFrame(list(graph_dict.items()), columns = ["Origin", "Destination"])
df_graph["Visited"] = "White"
df_graph["Distance"] = sys.maxsize

output_file = "../data/sample_data_formated.csv"
df_graph.to_csv(output_file, sep='|', encoding = 'utf-8', index = False, header = False)

