from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Spend")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    value = float(fields[2])
    return(customerID,value)

lines = sc.textFile('../../data/customer-orders.csv')
parsedLines = lines.map(parseLine)

valueSum = parsedLines.reduceByKey(lambda x, y: x + y)
rez = valueSum.collect()
rez.sort()

for order in rez:
    print (order)