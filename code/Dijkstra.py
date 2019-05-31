# -*- coding: utf-8 -*-
"""
Created on Tue May 14 10:01:26 2019

@author: Xiang Zhang
"""

import pandas as pd
import sys
import matplotlib.pyplot as plt

class Graph():
    """
    Graph class represents a graph structure
    
    Data Structure:
        - nodes (set): set of all nodes (using set instead of list could simplfy
                        edits to it)
        - edge (dictionary of list): a dictionary of list, the key is node ID, 
                                     the value is the list of its neighbor nodes
                                     
    Methods:
        - add_node: add a node to nodes attribute
        - add_edge: add a edge to edge structure
        - find_minDistance: a function return the unvisited node with the minimum
                            distance to the "from_node" argument
        - dijkstra: implement Dijkstra algorithm
    """
    def __init__(self):
        self.nodes = set()
        self.edge = {}
    
    def add_node(self, index):
        self.nodes.add(index)
    
    def add_edge(self, from_node, to_node):
        self.add_node(from_node)
        self.add_node(to_node)
        if from_node not in self.edge:
            self.edge[from_node] = [to_node]
        else:
            self.edge[from_node].append(to_node)

    def find_minDistance(self, from_node, visited, dist): 
        """
        Return the unvisited node with the shotest distance to the given node
        
        Inputs:
            - from_node: ID of the given node (our origin)
            - visited (list): list indicating whether a node has been fully 
                              iterated over (visited)
            - dist (list): distance list to the given node (the origin)
            
        Output:
            - min_node: ID of the found node
        """
        min = sys.maxsize
        min_node = -1
        
        for index, item in enumerate(visited):
            if item == True:
                continue
            else:
                min_temp = dist[index]
                if min_temp < min:
                    min = min_temp
                    min_node = index
                    
        return min_node
        
    def dijsktra(self, origin):
        """
        Main function implementing dijkstra algorithm
        
        Inputs:
            - origin: ID of the given node
            
        Output:
            - dist: list of distance to the origin. If they are not connected, the
            distance value would be sys.maxsize
        """
        visited = [False] * len(self.nodes)
        nodes_copy = self.nodes.copy()
        
        dist = [sys.maxsize] * len(self.nodes)
        dist[origin] = 0
    
        while True:
            min_node = self.find_minDistance(origin, visited, dist)
            if min_node == -1:
                break
            
            if min_node == 94599:
                print(min_node)
            nodes_copy.remove(min_node)
            
            # Mark the node as visited
            visited[min_node] = True
            
            if min_node not in self.edge:
                continue
                
            for to_node in self.edge[min_node]:
                if visited[to_node] == True:
                    continue
                
                temp_dist = dist[min_node] + 1
                if temp_dist < dist[to_node]:
                    dist[to_node] = temp_dist
                    # return dist
            if len(nodes_copy) == 0:
                break
            
        return dist   

if __name__ == '__main__': 
    # Read in sample node
    df = pd.read_csv('../data/sample_data.tsv', delimiter = "\t")
    
    # As the first step, we assign an unique ID to each node. In later implementation,
    # this ID would corresponds to their locations in the list structure.
    list_unique_patent_id = pd.unique(df[['patent_id', 'citation_id']].values.ravel('k'))

    dict_unique_patent_id = {}
    for index, item in enumerate(list_unique_patent_id):
        dict_unique_patent_id[item] = index
        
    # Generate graph structure
    graph_citation = Graph()
    for index, item in df.iterrows():
        patent_id   = dict_unique_patent_id[item["patent_id"]]
        citation_id = dict_unique_patent_id[item["citation_id"]]
        graph_citation.add_edge(patent_id, citation_id)
    
    """
    As a simple implementation, I compute the number of connected nodes and corresponding
    distance for each node in the graph.
    """
    count_list = []
    avg_dist_list = []
    for i in range(10000):
        print("The origion is", i)
        hh = graph_citation.dijsktra(i)
        count = 0
        dist_list = []
        for dist in hh:
            if dist != sys.maxsize:
                count += 1
                count_list.append(count)
                dist_list.append(dist)
        
        if len(dist_list) == 0:
            continue
        else:
            avg_dist = sum(dist_list) / len(dist_list)
            avg_dist_list.append(avg_dist)
            
    # Plot histogram of distance and number of nodes
    plt.hist(count_list, 10, density = True, facecolor='b', alpha = 0.75)
    
    plt.hist(avg_dist_list, 10, density = True, facecolor='b', alpha = 0.75)