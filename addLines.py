import sys
import os



def file_len(filename):
	with open(filename) as f:
		numberOfLines = 0
		for i, l in enumerate(f):
			numberOfLines += 1
	print("File has "+ str(numberOfLines) +" lines before adding")
	return numberOfLines + 1





if len(sys.argv) != 3:
	print sys.exit("usage: addLines <file> <number off lines to add>")


fileExists = os.path.exists(sys.argv[1])
if fileExists:
	filePath = sys.argv[1] 
else:
	print sys.exit("File does not exist")


linesToAdd = int(sys.argv[2])
numberOfLines = file_len(filePath)

print("\nAdding "+str(linesToAdd)+" lines")

for i in range(numberOfLines, numberOfLines+linesToAdd):
	# just some unique triple with linenumber as 'id'
	tmpStr = "<http://dbpedia.org/resource/Queens_of_the_Stone_Age> <http://www.w3.org/2002/07/owl#sameAs>  <http://commons.dbpedia.org/resource/Queens_of_the_Stone_Age"+str(i-1)+"> .\n"
	#append newline and new triple
	with open(filePath, 'a') as file:
		file.write(tmpStr)
