import csv
import StringIO

def loadRecord(line):
	'''Parse a CSV line'''
	input = StringIO.StringIO(line)
	reader = csv.DictReader(input, fieldnames = ['date', 'time', 'sensorType', 'columnLoc', 'value'])
	return reader.next()
	
def toCSVLine(data):
	'''Concatenate to CSV with K,V'''
	k = data[0]
	v = ','.join(str(d) for d in data[1])
	return '{0}\t{1}'.format(k,v) 
	
inputFile = 'file:///local_path_to_input_dir/input.csv'
outputFile = 'file:///local_path_to_output_dir'

input = sc.textFile(inputFile).map(loadRecord)

pairs = input.map(lambda x: (x['date']+ '-' + x['columnLoc'], x['value']))

pairsGrp = pairs.groupByKey()
#pairsGrp = pairs.sortByKey(True,1).groupByKey()
pa#irsGrp = pairs.groupByKey().sortByKey(True,1)

lines = pairsGrp.map(toCSVLine)
lines.saveAsTextFile(outputFile)
