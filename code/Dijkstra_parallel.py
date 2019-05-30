from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

class Node:
    # Abstraction of Character

    # Constructor
    def __init__(self):
        self.ID = ''
        self.neighbors = []
        self.color = 'White'
        self.distance = 9999

    # Format: HEROID|EDGES|DISTANCE|COLOR
    def fromLine(self, line):
        fields = line.split('|')
        if len(fields) == 4:
            self.ID = fields[0]
            self.neighbors = fields[1].split(',')
            self.color = fields[2]
            self.distance = int(fields[3])
            
    # Return the line with the correct format
    def getLine(self):
        neighbors = ','.join(self.neighbors)
        return '|'.join((self.ID, neighbors, self.color, str(self.distance)))

class MRBFSIteration(MRJob):

    # set the input / output protocol to write and read the file
    # Json is probably the most used
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # Pass the parameter to everybody who needs it as input value
    # --num-iterations=12345
    """
    def configure_options(self):
        super(MRBFSIteration, self).configure_options()
        self.add_passthrough_option('--target', help="Insert the ID of the character we are looking for")
    """   
    def configure_args(self):
        super(MRBFSIteration, self).configure_args()
        self.add_passthru_arg('--target', default = "-1", \
                              help = "Insert the ID of the character we are looking for")
        self.add_passthru_arg('--start_point', default = "-1", \
                              help = "Starting point")
        self.add_passthru_arg('--iteration', default = "3", \
                              help = "Number of Map-reduce iterations")

    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)
        # Look for grey nodes
        # At first iteration for ID = 100
        # 100|5432,3554,3116,4125,1721,6187,1347|0|GRAY     

        if (node.ID == self.options.start_point and node.color == 'White'):
            node.distance = 0
               
        if (node.color == 'Gray') or (node.ID == self.options.start_point and node.color == 'White'):
            for neighbor in node.neighbors:
                # Create a node for each connections
                vnode = Node()
                vnode.ID = neighbor
                vnode.distance = int(node.distance) + 1
                vnode.color = 'Gray'
                
                """
                if (self.options.target == neighbor):
                    counterName = ("Target ID " + neighbor +
                        " was hit with distance " + str(vnode.distance))
                    self.increment_counter('Degrees of Separation',
                        counterName, 1)
                """    
                yield neighbor, vnode.getLine()

            # We've processed this node, so color it black
            node.color = 'Black'

        # Emit the input node so we don't lose it.
        yield node.ID, node.getLine()

    def reducer(self, key, values):
        # Base settings
        edges = []
        distance = 9999
        color = 'White'

        # Data is grouped by HeroID, so for each value
        for value in values:
            node = Node()
            node.fromLine(value)

            # Extends the new array of connections
            if (len(node.neighbors) > 0):
                edges.extend(node.neighbors)

            # Set the new distance value if less than MAX
            if (node.distance < distance):
                distance = node.distance

            # If the node was already Black -> be black again
            if (node.color == 'Black' ):
                color = 'Black'

            # If is not black -> become gray, will be checked on next iteration
            if (node.color == 'Gray' and color == 'White' ):
                color = 'Gray'

        # Prepare the new output line
        node = Node()
        node.ID = key
        node.distance = distance
        node.color = color
        node.neighbors = edges[:500] # just a memory control

        yield key, node.getLine()
        
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=None,
                       reducer=self.reducer)
                ] * int(self.options.iteration)
        
if __name__ == '__main__': 
    mrjob = MRBFSIteration 
    mrjob.run()