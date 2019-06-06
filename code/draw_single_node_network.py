# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 10:49:24 2019

@author: zhang
"""

import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

def plot_network(i):
    input_file = '../../output/results' + i + '.csv'
    df = pd.read_csv(input_file, delimiter = "|")
    df.columns = ["ID", "Neighbor", "Color", "Distance"]
    df = df.loc[df.Color == "Black", :]
    
    x_graph_from = []
    x_graph_to   = []
    unique_id = []
    x_graph_distance = []
    for index, item in df.iterrows():
        if pd.notnull(item["Neighbor"]):
            neighbor_list = item["Neighbor"].split(",")
            if neighbor_list:
                for neighbor in neighbor_list:
                    if neighbor:
                        x_graph_from.append(str(item["ID"]))
                        x_graph_to.append(neighbor.strip())
                        
                        if str(item["ID"]) not in unique_id:
                            x_graph_distance.append(item["Distance"])
                            unique_id.append(str(item["ID"]))
                               
    df_graph = pd.DataFrame({'from':x_graph_from, 'to':x_graph_to})
        
    carac = pd.DataFrame({'ID':unique_id, 'value': x_graph_distance})
    
    G = nx.from_pandas_edgelist(df_graph, 'from', 'to',  create_using=nx.Graph())
    
    carac = carac.set_index('ID')
    carac = carac.reindex(G.nodes)
    
    # And I need to transform my categorical column in a numerical value: group1->1, group2->2...
    carac['value']=pd.Categorical(carac['value'])
    carac['value'].cat.codes
     
    if df["ID"].size < 50:
        node_size = 300
    else:
        node_size = 50
        
    nx.draw(G, with_labels= False, node_color=carac['value'].cat.codes, \
            cmap = plt.cm.seismic, node_size = node_size)

    output_file = '../../output/sample_identified_edge' + i + '.png'
    plt.savefig(output_file, dpi=1000)

if __name__ == '__main__': 
    for i in range(1, 4):
        plot_network(str(i))