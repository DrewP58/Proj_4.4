import time, os, sys, pymongo, numpy
from pymongo import MongoClient
from scipy import stats

cluster = MongoClient("mongodb+srv://savanp:bobzai@cluster0.lqldu.mongodb.net/test?retryWrites=true&w=majority")
db = cluster["test"]["new"]

def newUser(arr):
	post = {"firstName": arr[0], 
			"lastName": arr[1],
			"_id": arr[2],
			"password": arr[3],
			"streetAdd": arr[4],
			"city": arr[5],
			"zip": arr[6],
			"state": arr[7],
			"pH": [],
			"disOx": [],
			"turbid": [],
			"speCond": [],
			"index" : False
		}
	db.insert_one(post)
	return 1

def update(ele, arr):
	pH, disOx, turbid, speCond = [float(e) for e in arr]
	filter = { '_id': ele } 
	newvalues = { "$push": { 'pH': pH, 'disOx' : disOx, 'turbid': turbid, 'speCond': speCond} } 
	db.update_one(filter, newvalues)

	file = db.find_one(filter)
	zipC = file["zip"]
	boolean = runStat(ele, file)
	newvalues = { "$set": { 'index': boolean} } 
	db.update_one(filter, newvalues)
	return 1

def runStat(email, file):
	pH, disOx, turbid, speCond = [], [], [], []
	for post in db.find({"zip" : file["zip"]}):
		pH = numpy.concatenate((pH,post["pH"]))
		disOx = numpy.concatenate((disOx,post["disOx"]))
		turbid = numpy.concatenate((turbid,post["turbid"]))
		speCond = numpy.concatenate((speCond,post["speCond"]))

	data = [pH, disOx, turbid, speCond]
	compare = [file["pH"][-3:], file["disOx"][-3:], file["turbid"][-3:], file["speCond"][-3:]]

	for i in range(4):
		obj = stats.ttest_1samp(compare[i],numpy.mean(data[i]))
		t, p = obj.statistic, obj.pvalue
		if p < 0.05:
			return True
	return False

def pull(email):
	file = db.find_one({"_id": email})
	print(file["pH"])
	print(file["disOx"])
	print(file["turbid"])
	print(file["speCond"])
	print(file["index"])
	return 1

if __name__ == "__main__":
	if len(sys.argv) > 1:
		if (sys.argv[1] == "newUser"):
			print("hello")
			newUser(sys.argv[2:])
		elif (sys.argv[1] == "update"):
			update(sys.argv[2], sys.argv[3:])
		elif (sys.argv[1] == "pull"):
			pull(sys.argv[2])
		else:
			print("No clue what you're doing.")
	else:
		print("You didn't add a descriptor to your process call.")
