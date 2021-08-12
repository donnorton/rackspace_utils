#!/usr/bin/python3

import sys
import argparse
import os.path
import csv
import pprint
import inspect
 
pp = pprint.PrettyPrinter(indent=4)

# Global variables
line_count = 1
comment_line_count = 0
zero_poh = 0
zero_pcc = 0
max_years=12

# Useful constants
hrs_per_year = 24 * 365

# Debugging support
verbose = False
debug = {}
debug["data"] = False
debug["keys"] = False
debug["model"] = False
debug["pcc"] = False
debug["poh"] = False
debug["reports"] = False
debug["size"] = False

# Instantiate data dictionaries
drivedata = {}
driveage = {}
drivesize = {}
modeldict = {}
sizedict = {}

# Initialize data dictionaries
drivedata["global"] = {"count":0, "power_on_hours":0, "avg_power_on_hours":0, "power_cycle_count":0, "avg_power_cycle_count":0, "ignored_records":0, "zero_size":0}
driveage["global"] = {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0, 10:0, 11:0, 12:0}
drivesize["global"] = {1862.50:0, 2794.00:0, 2794.52:0, 3725.50:0, 3726.02:0, 5588.50:0, 7451.50:0, 7452.04:0, 9313.50:0 }
modeldict["global"] = {}
sizedict["global"] = {}

# Debugging support
def line_numb():
	return str(inspect.currentframe().f_back.f_lineno)

# Data file processing function
def process_data(datafile):
	global verbose
	global line_count
	global comment_line_count
	global zero_poh
	global zero_pcc

	if (verbose): print("Processing data file: ", datafile)

	# Open data file and read data line by line
	with open(datafile, mode='r') as csv_file:
		csv_reader = csv.DictReader(csv_file)
		for row in csv_reader:
			if line_count == 1:
				try:
					tmp = row["Hostname"]
					if (verbose): print(f'Line {line_count}: Column names are {", ".join(row)}')
					line_count += 1
				except:
					print(f"Error: Data file \"{datafile}\" does not contain proper column header line")
					exit(-1)

			if (debug["data"]): print(f'Line {line_count}: {row}')
			if (debug["data"]): print(f'Line {line_count}: {row["Hostname"]}, {row["Model"]}, {row["Serial"]}, {row["Size"]}, {row["UDMA_CRC_Error_Count"]}, {row["Raw_Read_Error_Rate"]}, {row["Power_Cycle_Count"]}, {row["Power_On_Hours"]}')

			if row["Hostname"].startswith("#"):
				comment_line_count += 1
				line_count += 1
			else:
				region = get_region_name(row)
				add_keys(region)
				# Increment drive count prior to processing the fields to prevent math errors
				drivedata[region]["count"] += 1
				drivedata["global"]["count"] += 1

				# Process each field appropriately
				process_power_on_hours(row, region)
				process_power_cycle_count(row, region)
				process_drive_model(row, region)
				process_drive_size(row, region)

				# Increment the data file line counter 
				line_count += 1

	# Subtract one from line_count for header row
	if (verbose): print(f'Processed {line_count-1} records')
	if (verbose): print(f'Ignored {comment_line_count} commented records')

def add_keys(region):
	# Add region keys to dicts if it hasn't been seen yet 
	if region not in drivedata:
		drivedata[region] = {"count":0, "power_on_hours":0, "avg_power_on_hours":0, "power_cycle_count":0, "avg_power_cycle_count":0, "ignored_records":0, "zero_size":0}
		driveage[region] = {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0, 10:0, 11:0, 12:0}
		modeldict[region] = {}
		sizedict[region] = {}
		if (debug["keys"]): 
			print("drivedata =", drivedata)
			print("driveage =", driveage)
			print("modeldict =", modeldict)
			print("sizedict = ", sizedict)

# Field processing functions

# Extract the region name from the fully qualified hostname
def get_region_name(row):
	# Split fqdn to pull out region name
	fqdn = row["Hostname"].split(".")
	#host = fqdn[0] 
	return(fqdn[1])

# Process the Power_On_Hours field
def process_power_on_hours(row, region):
	global zero_poh

	if (debug["poh"]): print(f'Line {line_count}: row[Power_On_Hours] = {row["Power_On_Hours"]}')
	if (debug["poh"]): print(f"Line {line_count}: row =", row)

	# Set Power_On_Hours to zero if the data is absent (some drives don't report all data)
	if row["Power_On_Hours"] == "": 
		row["Power_On_Hours"] = str(drivedata[region]["avg_power_on_hours"])
		#row["Power_On_Hours"] = "0"	# Use this if the running average isn't appropriate
		if (verbose): print(f'Line {line_count}: Warning: Power_On_Hours is null; setting to {row["Power_On_Hours"]}')
		if (debug["poh"]): print(f'Line {line_count}: Warning: Power_On_Hours for drive Model {row["Model"]}, with Serial {row["Serial"]}, is {row["Power_On_Hours"]}')
		zero_poh += 1

	# Strip off fractional hours to the right of the decimal point, since we're doing integer math
	if '.' in row["Power_On_Hours"]:
		# Remove any decimal point and trailing digits
		row["Power_On_Hours"] = row["Power_On_Hours"][:row["Power_On_Hours"].index('.')]
		if (debug["poh"]): print(f'Line {line_count}: modified Power_On_Hours = {row["Power_On_Hours"]}')

	# Update rolling count totals and calculate running averages 
	drivedata[region]["power_on_hours"] += int(row["Power_On_Hours"])
	drivedata["global"]["power_on_hours"] += int(row["Power_On_Hours"])
	drivedata[region]["avg_power_on_hours"] = drivedata[region]["power_on_hours"] / drivedata[region]["count"]
	drivedata["global"]["avg_power_on_hours"] = drivedata["global"]["power_on_hours"] / drivedata["global"]["count"]

	# Increment the "age bucket" tallying the operational "age" of the drive
	age = int(int(row["Power_On_Hours"])/hrs_per_year)
	driveage[region][age] += 1
	driveage["global"][age] += 1

	return(0)

# Process the Power_Cycle_Count field
def process_power_cycle_count(row, region):
	# Process the Power_Cycle_Count field
	global zero_pcc

	if (debug["pcc"]): print(f'Line {line_count}: row[size] = {row["Power_Cycle_Count"]}')
	if (debug["pcc"]): print(f"Line {line_count}: row =", row)

	if row["Power_Cycle_Count"] == "":
		row["Power_Cycle_Count"] = "1"
		zero_pcc += 1	# At least one power cycle required to put into operation

	try:
		drivedata[region]["power_cycle_count"] += int(row["Power_Cycle_Count"])
		drivedata["global"]["power_cycle_count"] += int(row["Power_Cycle_Count"])
	except:
		print(f"Invalid Power_Cycle_Count data for drive Model {row['Model']}, Serial {row['Serial']}, Power_Cycle_Count {row['Power_Cycle_Count']}")

	drivedata[region]["avg_power_cycle_count"] = drivedata[region]["power_cycle_count"] / drivedata[region]["count"]
	drivedata["global"]["avg_power_cycle_count"] = drivedata["global"]["power_cycle_count"] / drivedata["global"]["count"]

	return(0)

# Process the Size field
def process_drive_size(row, region):
	if (debug["size"]): print(f'Line {line_count}: row[size] = {row["Size"]}')
	if (debug["size"]): print("row =", row)

	sizedata = row["Size"].split(".")
	size = sizedata[0].strip()

	if (debug["size"]): print(f"Line {line_count}: size = {size}")
	# Very voluminous debugging output.  Uncomment if desired.
	#if (debug["size"]): print("sizedict =", sizedict)
	#if (debug["size"]): print("sizedict[global] =", sizedict["global"])
	#if (debug["size"]): print("sizedict["+region+"] =", sizedict[region])

	if (size == '' or size == "0"):
		# Do not add a null size entry to the dictionary, but keep a count of them
		if (verbose): print(f'Line {line_count}: Warning: Size for drive Model {row["Model"]}, with Serial {row["Serial"]}, is {row["Size"]}')
		if (debug["size"]): print(f'Line {line_count}: Size for drive Model {row["Model"]}, with Serial {row["Serial"]}, is {row["Size"]}')

		drivedata[region]["zero_size"] += 1
		drivedata["global"]["zero_size"] += 1
		#zero_size += 1
	else:
		# Add size entry to dictionary for the global (aggregate) "region"
		if size not in sizedict["global"]:
			# Add the new "size" key to the global dict
			#if (verbose): print(f"Line {line_count}: Adding new size {size} to sizedict for region global")
			tmpdict = {size:{"count":0, "power_on_hours":0, "avg_power_on_hours":0}}
			sizedict["global"].update(tmpdict)
			if (debug["size"]): print(f"Line {line_count}: sizedict = {sizedict}")

		# Add size entry to dictionary for the appropriate region
		if size not in sizedict[region]:
			# Add the new "size" key to the specific region dict
			if (verbose): print(f"Line {line_count}: Adding new size {size} to sizedict for region {region}")
			tmpdict = {size:{"count":0, "power_on_hours":0, "avg_power_on_hours":0}}
			sizedict[region].update(tmpdict)
			if (debug["size"]): print(f"Line {line_count}: sizedict = {sizedict}")

		# In theory, this should never trigger, since the Power_On_Hours field is processed
		# prior to the Size field, and if found to be null, is set to something else there.
		# But, for defensive programming purposes, we'll leave this code here.
		if (row["Power_On_Hours"] == ""):
			# Missing/bad Power_On_Hours data; set to the current running average
			row["Power_On_Hours"] = str(sizedict[region][size]["avg_power_on_hours"])
			if (debug["size"]): 
				print("Source line "+line_num()+" : ", end="")
				print(type(row["Power_On_Hours"]))

		# Increment/update the values for drives of that size in both global and the region
		if (debug["size"]): print(f"Line {line_count}: Incrementing size count for {size} in region {region}")
		sizedict[region][size]["count"] += 1
		sizedict[region][size]["power_on_hours"] += int(row["Power_On_Hours"])
		sizedict[region][size]["avg_power_on_hours"] = sizedict[region][size]["power_on_hours"] / sizedict[region][size]["count"]
		if (debug["size"]): print(f"Line {line_count}: Size data for region {region} : {sizedict[region]}")
		if (debug["size"]): print(f"Line {line_count}: Stats for {region}[{size}] : {sizedict[region][size]}")

		sizedict["global"][size]["count"] += 1
		sizedict["global"][size]["power_on_hours"] += int(row["Power_On_Hours"])
		sizedict["global"][size]["avg_power_on_hours"] = sizedict["global"][size]["power_on_hours"] / sizedict["global"][size]["count"]
		if (debug["size"]): print(f'Line {line_count}: Size data for global : {sizedict["global"]}')
		if (debug["size"]): print(f'Line {line_count}: Stats for global[{size}] : {sizedict["global"][size]}')

	return(0)

def process_drive_model(row, region):
	# Proocess the model field

	# If the Model field is empty, set it to "(NULL)" for easier interpretation in the report
	if row["Model"] == "":
		row["Model"] = "(NULL)"

	# Split model number field
	modeldata = row["Model"].split("-")
	model = modeldata[0].strip()
	if (debug["model"]): print(f"Line {line_count}: model = {model}")

	# Add model number key to dictionary for the appropriate region
	if model not in modeldict["global"]:
		if (verbose): print(f"Line {line_count}: Adding new model {model} to modeldict under region global")
		modeldict["global"][model] = 1
	if model not in modeldict[region]:
		if (verbose): print(f"Line {line_count}: Adding new model {model} to modeldict under region {region}")
		modeldict[region][model] = 1
	
	if (debug["model"]): print(f"Line {line_count}: Incrementing model count for {model} in region {region}")
	if (debug["model"]): print("Model data for region", region, ":", modeldict[region])
	modeldict["global"][model] = modeldict["global"][model] + 1
	modeldict[region][model] = modeldict[region][model] + 1

	return(0)

# Reporting functions
def report(drivedata, driveage, modeldict, sizedict, dc, poh, model, size, csv):
	if (csv):
		print_csv(drivedata, driveage, sizedict)
	else:
		if dc != "":
			#print("Reporting formatted output for dc:", dc)
			if dc in drivedata:
				if (verbose) : print("report(): dc =", dc, "; poh =", poh, "; model =", model, "; size =", size)
				if (poh == True): print_power_on_hours_data(drivedata, driveage, dc)
				if (model == True): print_model_data(modeldict[dc], dc)
				if (size == True): print_size_data(sizedict[dc], dc)
			else:
				print("Region \'" + dc + "\' not found")
				print("Specify one of the following:", end=" ")
				for region in drivedata:
					print(region, end=" ")
				print("")
				return
		else:
			for region in drivedata:
				if (poh == True): print_power_on_hours_data(drivedata, driveage, region)
			for region in modeldict:
				if (model == True): print_model_data(modeldict[region], region)
			for region in sizedict:
				if (size == True): print_size_data(sizedict[region], region)

# TODO:  This function still needs to be finished/debugged/tested
def print_csv(drivedata, driveage, sizedict):
	print("Region\Age, ", end=" ")
	for year in range(0, max_years):
		print("{}-{},".format(year, year+1), end=" ")
	print("")
	
	for region in driveage:
		print("{}, ".format(region), end=" ")
		for year in range(0, max_years):
			print("{},".format(driveage[region][year]), end=" ")
		print("")

	print("sizedict : ")
	pp.pprint(sizedict["global"])

	print(" ")
	print("Region\Size, ", end=" ")
# TODO:  This line causes a run-time failure.  Figure it out....
	for size in sorted(sizedict["global"], key=sizedict["global"].get, reverse=True):
		print(size, " ", end=" ")
	print(" ")

	for region in sizedict:
		print(region, end=" ")
		for size in sizedict[region]:
			print(", ", size, end=" ")
			#print("\t%8d" % sizedict[region][size], ":", size)
			#print(sizedict[region][size][count], " ", end=" ")
			#print(", ", end=" ")
		print(" ")
			
def print_power_on_hours_data(drivedata, driveage, region):
	global comment_line_count
	global zero_poh
	global zero_pcc

	print("REGION:", region, "===> Drive power-on hours")
	print("")
	print("\t  Years\t\tDrives\tPercent")
	for year in range(0, max_years):
		print("\t {:2d} - {:2d}\t{:6d}\t{:4.1f}%%".format(year, year+1, driveage[region][year], (driveage[region][year] / drivedata[region]["count"] * 100)))
	print("")
	print(region, "region summary:")
	print("\t%8.0f" % (drivedata[region]["avg_power_on_hours"]), "average drive power-on hours" )
	print("\t%8.2f" % (drivedata[region]["avg_power_on_hours"]/hrs_per_year), "average drive power-on years")
	print("\t%8.2f" % (drivedata[region]["avg_power_cycle_count"]), "average drive power cycle count")
	print("\t%8.0f" % drivedata[region]["count"], "drive records reported")
	if region == "global":
		print("\t%8.0f" % zero_poh, "drives reported zero Power On Hours (%4.1f %%)" % (zero_poh / drivedata["global"]["count"] * 100))
		print("\t%8.0f" % zero_pcc, "drives reported zero Power Cycle Count (%4.1f %%)" % (zero_pcc / drivedata["global"]["count"] * 100))
	print("-----------------------------------------------------------------")

def print_model_data(mdict, region):
	num_models = len(mdict)
	num_total = 0
	print("REGION:", region, "===> Drive model quantities")
	print("")
	print("\tQuantity : Model")
	for model in sorted(mdict, key=mdict.get, reverse=True):
		print("\t%8d" % mdict[model], ":", model)
		num_total += int(mdict[model])
	print("")
	print("\t%8.0f" % num_models, "Unique drive model(s)")
	if "(NULL)" in mdict:
		print("\t%8.0f" % mdict["(NULL)"], "Drive(s) reporting a null model number")
	print("\t%8.0f" % num_total, "Total drive record(s)")
	print("-----------------------------------------------------------------")

def print_size_data(sdict, region):
#	global zero_size
	num_sizes = len(sdict)
	num_total = 0
	if (verbose) : print("print_size_data()", )
	if (debug["reports"]) : print("region =", region)
	if (debug["reports"]) : print("sdict =", sdict)
	print("REGION:", region, "===> Drive size quantities")
	print("")
	print("\tSize (GB):      Quantity")
	if (debug["reports"]) : pp.pprint(sdict)
	#for size in sorted(sdict)
	for size in sorted(sdict, key=lambda a : int(a)):
		print("\t%8d" % int(size), ":\t%8d" % int(sdict[size]["count"]))
		num_total += int(sdict[size]["count"])
	print("")
	print("\t%8.0f" % num_sizes, "Unique drive size(s)")
	print("\t%8.0f" % drivedata[region]["zero_size"], "Drive(s) reporting size of zero or null")
	print("\t%8.0f" % num_total, "Total drive record(s)")
	print("-----------------------------------------------------------------")

def main():
	global verbose

	parser = argparse.ArgumentParser(description= "Parse hard drive S.M.A.R.T. data and summarize by region and by drive characteristics")
	parser.add_argument('-a', '--all',	action='store_true',	help="Report all data from all regions combined")
	parser.add_argument('-c', '--csv',	action='store_true',	help="Report data in CSV format")
	parser.add_argument('-d', '--debug',	nargs='?',		help="Report additional debug output (data|keys|size|model|pcc|poh|size|reports)")
	parser.add_argument('-m', '--model',	action='store_true',	help="Report drive model distribution")
	parser.add_argument('-p', '--power',	action='store_true',	help="Report drive power-on hours distribution")
	parser.add_argument('-s', '--size',	action='store_true',	help="Report drive size distribution")
	parser.add_argument('-v', '--verbose',	action='store_true',	help="Print verbose information")
	parser.add_argument('-f', '--file',	nargs='?',		help="Data file to process")
	parser.add_argument('-r', '--region',	nargs='?',		help="Report data from specified region")
	args = parser.parse_args()

	if args.verbose == True:
		verbose = True
		print("Setting verbose to ", verbose)

	if args.debug:
		debug[args.debug] = True
		print("Setting debug level to", args.debug)

	region = ""
	if args.region:
		region = args.region
		if (verbose): print("Setting region to", region)

	power = False
	if args.power:
		power = True

	model = False
	if args.model:
		model = True

	size = False
	if args.size:
		size = True

	csv = False
	if args.csv:
		csv = True

	if args.all == True:
		region = "global"
		power = True
		model = True
		size = True

	if model == False and power == False and size == False:
		print("Specify one of [-a], [-m], [-p], or [-s]")
		exit(-1)

	if args.file:
		input_file = args.file
		if os.path.isfile(input_file):
			process_data(input_file)
		else:
			print("Error: file not found:", input_file)
			exit(1)
	else:
		print("Error: No data file specified")
		exit(1)

	report(drivedata, driveage, modeldict, sizedict, dc=region, poh=power, model=model, size=size, csv=csv)

if __name__ == '__main__':
	main()
	exit(0)

