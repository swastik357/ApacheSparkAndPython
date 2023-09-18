#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
'''    
CAPTAIN AMERICA          1933  859
SPIDER-MAN/PETER PAR     1741  5306
IRON MAN/TONY STARK      1528  2664
THING/BENJAMIN J. GR     1426  5716
WOLVERINE/LOGAN          1394  6306
MR. FANTASTIC/REED R     1386  3805
HUMAN TORCH/JOHNNY S     1371  2557
SCARLET WITCH/WANDA      1345  4898
THOR/DR. DONALD BLAK     1289  5736
BEAST/HENRY &HANK& P     1280  403
VISION                   1263  6066
INVISIBLE WOMAN/SUE      1244  2650
HAWK                     1176  2399
CYCLOPS/SCOTT SUMMER     1104  1289
STORM/ORORO MUNROE S     1095  5467
ANGEL/WARREN KENNETH     1094  133
WASP/JANET VAN DYNE      1093  6148
ANT-MAN/DR. HENRY J.     1092  154
SHE-HULK/JENNIFER WA     1080  5046
DR. STRANGE/STEPHEN      1079  1602
'''
    


superHeroIds = [859,5306,2664,5716,6306,3805,2557,4898,5736,403,6066,2650,2399,1289,5467,133,6148,154,5046,1602]    
superHeros = ["CAPTAIN AMERICA","SPIDER-MAN/PETER PAR","IRON MAN/TONY STARK","THING/BENJAMIN J. GR","WOLVERINE/LOGAN","MR. FANTASTIC/REED R","HUMAN TORCH/JOHNNY S","SCARLET WITCH/WANDA","THOR/DR. DONALD BLAK","BEAST/HENRY &HANK& P","VISION","INVISIBLE WOMAN/SUE","HAWK","CYCLOPS/SCOTT SUMMER","STORM/ORORO MUNROE S","ANGEL/WARREN KENNETH","WASP/JANET VAN DYNE","ANT-MAN/DR. HENRY J.","SHE-HULK/JENNIFER WA", "DR. STRANGE/STEPHEN"]


for i in range(1,len(superHeroIds)):
    for j in range(i):
        
        startCharacterID = superHeroIds[i] #SpiderMan
        targetCharacterID = superHeroIds[j]  #ADAM 3,031 (who?)//14
        
        # Our accumulator, used to signal when we find the target character during
        # our BFS traversal.
        hitCounter = sc.accumulator(0)
        
        def convertToBFS(line):
            fields = line.split()
            heroID = int(fields[0])
            connections = []
            for connection in fields[1:]:
                connections.append(int(connection))
        
            color = 'WHITE'
            distance = 9999
        
            if (heroID == startCharacterID):
                color = 'GRAY'
                distance = 0
        
            return (heroID, (connections, distance, color))
        
        
        def createStartingRdd():
            inputFile = sc.textFile("/Users/swastik./SparkCourse/marvel-graph.txt")
            return inputFile.map(convertToBFS)
        
        def bfsMap(node):
            characterID = node[0]
            data = node[1]
            connections = data[0]
            distance = data[1]
            color = data[2]
        
            results = []
        
            #If this node needs to be expanded...
            if (color == 'GRAY'):
                for connection in connections:
                    newCharacterID = connection
                    newDistance = distance + 1
                    newColor = 'GRAY'
                    if (targetCharacterID == connection):
                        hitCounter.add(1)
        
                    newEntry = (newCharacterID, ([], newDistance, newColor))
                    results.append(newEntry)
        
                #We've processed this node, so color it black
                color = 'BLACK'
        
            #Emit the input node so we don't lose it.
            results.append( (characterID, (connections, distance, color)) )
            return results
        
        def bfsReduce(data1, data2):
            edges1 = data1[0]
            edges2 = data2[0]
            distance1 = data1[1]
            distance2 = data2[1]
            color1 = data1[2]
            color2 = data2[2]
        
            distance = 9999
            color = color1
            edges = []
        
            # See if one is the original node with its connections.
            # If so preserve them.
            if (len(edges1) > 0):
                edges.extend(edges1)
            if (len(edges2) > 0):
                edges.extend(edges2)
        
            # Preserve minimum distance
            if (distance1 < distance):
                distance = distance1
        
            if (distance2 < distance):
                distance = distance2
        
            # Preserve darkest color
            if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
                color = color2
        
            if (color1 == 'GRAY' and color2 == 'BLACK'):
                color = color2
        
            if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
                color = color1
        
            if (color2 == 'GRAY' and color1 == 'BLACK'):
                color = color1
        
            return (edges, distance, color)
        
        
        #Main program here:
        iterationRdd = createStartingRdd()
        
        for iteration in range(0, 10):
            print("Running BFS iteration# " + str(iteration+1))
        
            # Create new vertices as needed to darken or reduce distances in the
            # reduce stage. If we encounter the node we're looking for as a GRAY
            # node, increment our accumulator to signal that we're done.
            mapped = iterationRdd.flatMap(bfsMap)
        
            # Note that mapped.count() action here forces the RDD to be evaluated, and
            # that's the only reason our accumulator is actually updated.
            print("Processing " + str(mapped.count()) + " values.")
            print(mapped.collect())
            if (hitCounter.value > 0):
                print("degree of separation between " +superHeros[i] +" and "+ superHeros[j] +"  is " + str(hitCounter.value))
                '''
                print("Hit the target character! From " + str(hitCounter.value) \
                    + " different direction(s).")
                '''    
                break
            
            # Reducer combines data for each character ID, preserving the darkest
            # color and shortest path.
            iterationRdd = mapped.reduceByKey(bfsReduce)
