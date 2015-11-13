print '''
##########################
start to generate hive sql
##########################
'''

testTuple = (1,2,3
	,4,5)
print testTuple

strList = ["and", "test"]
print "and" in strList

str = "afdafaf1,"
print str[0:-1]

strdict = {"hello1": ("test","test")}
value = strdict.get("hello")
print value
print value is None
print not value is None
if value:
	print 'true'
else:
	print 'false'


