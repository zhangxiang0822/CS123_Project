# -*- coding: utf-8 -*-
"""
Created on Tue May 14 10:01:26 2019

@author: Xiang Zhang
"""

import pandas as pd
import sys

class Graph():
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
        if from_node not in self.edge:
            return -1
        for to_node in self.edge[from_node]: 
            print("hahaha")
            if visited[to_node] == False: 
                min_temp = dist[to_node]
                if min_temp < min:
                    min = min_temp
                    min_node = to_node
                    print(min, min_node)
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

df = pd.read_csv('../data/sample_data.tsv', delimiter = "\t")
#df = df.loc[0:10000, :]

list_unique_patent_id = pd.unique(df[['patent_id', 'citation_id']].values.ravel('k'))

dict_unique_patent_id = {}
for index, item in enumerate(list_unique_patent_id):
    dict_unique_patent_id[item] = index

graph_citation = Graph()
for index, item in df.iterrows():
    patent_id   = dict_unique_patent_id[item["patent_id"]]
    citation_id = dict_unique_patent_id[item["citation_id"]]
    graph_citation.add_edge(patent_id, citation_id)

count_list = []
for i in range(10000):
    print("The origion is", i)
    hh = graph_citation.dijsktra(i)
    count = 0
    for j in hh:
        if j != sys.maxsize:
            count += 1
    count_list.append(count)
    
"""
Code by Others

class Graph():
    def __init__(self, vertices): 
        self.V = vertices 
        self.graph = [[0 for column in range(vertices)]  
                      for row in range(vertices)] 
    
    def printSolution(self, dist): 
        print("Vertex tDistance from Source")
        for node in range(self.V): 
            print(node, "t", dist[node])
            
    def minDistance(self, dist, sptSet): 
  
        # Initilaize minimum distance for next node 
        min = sys.maxsize
  
        # Search not nearest vertex not in the  
        # shortest path tree 
        for v in range(self.V): 
            if dist[v] < min and sptSet[v] == False: 
                min = dist[v] 
                min_index = v 
  
        return min_index 
    
    def dijkstra(self, origin): 
      
            dist = [sys.maxsize] * self.V 
            dist[origin] = 0
            sptSet = [False] * self.V 
      
            for cout in range(self.V): 
      
                # Pick the minimum distance vertex from  
                # the set of vertices not yet processed.  
                # u is always equal to src in first iteration 
                u = self.minDistance(dist, sptSet) 
      
                # Put the minimum distance vertex in the  
                # shotest path tree 
                sptSet[u] = True
      
                # Update dist value of the adjacent vertices  
                # of the picked vertex only if the current  
                # distance is greater than new distance and 
                # the vertex in not in the shotest path tree 
                for v in range(self.V): 
                    if self.graph[u][v] > 0 and sptSet[v] == False and dist[v] > dist[u] + self.graph[u][v]: 
                            dist[v] = dist[u] + self.graph[u][v] 
      
            self.printSolution(dist) 
            
g = Graph(9) 
g.graph = [[0, 1, 0, 0, 0, 0, 0, 1, 0], 
           [1, 0, 1, 0, 0, 0, 0, 1, 0], 
           [0, 1, 0, 1, 0, 1, 0, 0, 1], 
           [0, 0, 1, 0, 1, 1, 0, 0, 0], 
           [0, 0, 0, 1, 0, 1, 0, 0, 0], 
           [0, 0, 1, 1, 1, 0, 1, 0, 0], 
           [0, 0, 0, 0, 0, 1, 0, 1, 1], 
           [1, 1, 0, 0, 0, 0, 1, 0, 1], 
           [0, 0, 1, 0, 0, 0, 1, 1, 0] 
          ]; 

## Test
g1 = Graph() 
g1.add_edge(0, 1)
g1.add_edge(0, 7)
g1.add_edge(1, 0)
g1.add_edge(1, 2)
g1.add_edge(1, 7)
g1.add_edge(2, 1)
g1.add_edge(2, 3)
g1.add_edge(2, 5)
g1.add_edge(2, 8)
g1.add_edge(3, 2)
g1.add_edge(3, 4)
g1.add_edge(3, 5)
g1.add_edge(4, 3)
g1.add_edge(4, 5)
g1.add_edge(5, 2)
g1.add_edge(5, 3)
g1.add_edge(5, 4)
g1.add_edge(5, 6)
g1.add_edge(6, 5)
g1.add_edge(6, 7)
g1.add_edge(6, 8)
g1.add_edge(7, 0)
g1.add_edge(7, 1)
g1.add_edge(7, 6)
g1.add_edge(7, 8)
g1.add_edge(8, 2)
g1.add_edge(8, 6)
g1.add_edge(8, 7)
  
haha = g1.dijsktra(1)    
g.dijkstra(1);  
"""