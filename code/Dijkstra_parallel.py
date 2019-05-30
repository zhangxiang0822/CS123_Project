from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
import sys

class Node:
    """
    This is a class storing a data structure "node".
    
    Data Structure:
        - ID (can be int or str): ID of the node
        - neighbors (list of ID): list storing the ID of neighbors
        - color (str): can be 'White', 'Gray', or 'Black'. It's used to indicate
                       whether a node is to be iterate over (gray), haven't been
                       iterated over and not to be iterated over (white), or 
                       already have been iterated over (black). In the mapper, 
                       we only iteratre over those "gray" nodes.
        - distance (int): the distance to the starting node. Default value is 
                          sys.maxsize. 
    
    Methods:
        - fromline: function used to read in lines and edit node information. 
                    The formate of a line is:
                        ID|ID1, ID2, ID3, ...|color|distance
                    where ID1, ID2, ID3, ... stands for the ID of neighborhoods
        - get line: function used to transform a node class to single line. 
                    This will be used in reducer to generate inputs for next 
                    mapper.
    """
    
    def __init__(self):
        self.ID = ''
        self.neighbors = []
        self.color = 'White'
        self.distance = sys.maxsize
 
    def fromLine(self, line):
        fields = line.split('|')
        if len(fields) == 4:
            self.ID = fields[0]
            self.neighbors = fields[1].split(',')
            self.color = fields[2]
            self.distance = int(fields[3])
            
    def getLine(self):
        neighbors = ','.join(self.neighbors)
        return '|'.join((self.ID, neighbors, self.color, str(self.distance)))

class BFSIteration(MRJob):
    """
    This is the class for paralized breadth-first search.
    
    Methods:
        - mapper 
        - reducer: find smallest distance by node ID
        - configure_args(self): pass parameters passed to the mapper and reducer:
            Two arguments to be passed:
                - staring_point (str): ID of the starting point
                - iteration (str): number of map-reduce iterations.
        - 
    """
    # set the input / output protocol to write and read the file
    # Json is probably the most used
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self):
        super(BFSIteration, self).configure_args()
        self.add_passthru_arg('--start_point', default = "-1", \
                              help = "Starting point")
        self.add_passthru_arg('--iteration', default = "3", \
                              help = "Number of Map-reduce iterations")

    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)  
        
        # Pre-process the original starting point
        # This is a trick so that we don't have to modify our input file every time
        if (node.ID == self.options.start_point and node.color == 'White'):
            node.distance = 0
         
        # Look for grey nodes or starting nodes
        if (node.color == 'Gray') or (node.ID == self.options.start_point and node.color == 'White'):
            for neighbor in node.neighbors:
            
                # Create nodes for each neighbor and yield
                vnode = Node()
                vnode.ID = neighbor
                vnode.distance = int(node.distance) + 1
                
                # Set color to "gray" as we'll iterate over it in the next step
                vnode.color = 'Gray'
                 
                yield neighbor, vnode.getLine()

            # Mark the node black as we've already processed it
            node.color = 'Black'

        # Also emit the input node.
        yield node.ID, node.getLine()

    def reducer(self, key, values):
        # Base settings
        edges = []
        distance = sys.maxsize
        color = 'White'

        for value in values:
            node = Node()
            node.fromLine(value)

            # Extends the new array of neighbors
            if (len(node.neighbors) > 0):
                edges.extend(node.neighbors)

            # Set the new distance value if less than existing minimum distance
            if (node.distance < distance):
                distance = node.distance

            # Change color of nodes
            if (node.color == 'Black' ):
                color = 'Black'
                
            if (node.color == 'Gray' and color == 'White' ):
                color = 'Gray'
        
        # Generate new input lines for next map-reduce
        node = Node()
        node.ID = key
        node.distance = distance
        node.color = color
        
        # Set the max number of neighbors to 100000
        max_neighbors = 100000
        node.neighbors = edges[:max_neighbors]

        yield key, node.getLine()
        
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=None,
                       reducer=self.reducer)
                ] * int(self.options.iteration)
        
if __name__ == '__main__': 
    mrjob = BFSIteration 
    mrjob.run()