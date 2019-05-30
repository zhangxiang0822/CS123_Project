# -*- coding: utf-8 -*-
"""
Created on Tue May 28 12:42:34 2019

@author: Xiang Zhang
"""

from Dijkstra_parallel import MRBFSIteration
import ast
import subprocess

INPUT_GRAPH = "../data/sample_data_formated.csv"
START_NODE = 1
END_NODE = 5

mr_job = MRBFSIteration(args=[INPUT_GRAPH, '--startnode', START_NODE, '--endnode', END_NODE,  '-r', 'hadoop', '--no-output', '--cleanup', 'NONE', '--no-check-input-paths', '--output-dir',  '../output' ])

num_interation = 0
endnode_reached = False

while (1):
    with mr_job.make_runner() as runner: 
        num_interation += 1
        print("Iteration: ", num_interation)
        subprocess.Popen(["hadoop", "fs", "-rm", "-r", "/output"], stdout=None)
        runner.run()
        f = open(INPUT_GRAPH, 'w+')
        #cat = subprocess.Popen(["hadoop", "fs", "-cat", "/tmp/msannat/temp-output/part-00000"], stdout=subprocess.PIPE)
        cat = subprocess.Popen(["hadoop", "fs", "-cat", "/output"], stdout=subprocess.PIPE)
        for line in cat.stdout:
        #for line in runner.stream_output():
            f.writelines(line)
            
            line = line.split('\t')
            node = line[0].strip('"')
            data = line[1].strip('"').split('|')
            edges = data[0]
            distance = data[1]
            path = data[2]
            status = data[3].replace('"', "")

            if status[0] == 'F' :
                min_distance = distance
                endnode_reached = True
                path = ast.literal_eval(path)
                shortest_path = ' -> '.join(path) + " -> " + node
            
        if endnode_reached:
            break
    f.close()

print("number of iterations {}".format(num_interation))
print("shortest distance from node {} to node {} is: {} with path: [{}]".format(START_NODE, END_NODE, min_distance, shortest_path))