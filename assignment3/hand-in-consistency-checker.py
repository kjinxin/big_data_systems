import os
import subprocess

# Get the group number
group = os.getcwd().split("/")[-1].split("-")[-1]

# List of required files to be submitted
required_files = [
		"./Part-A/bigmatrixmultiplication.py",
		"./Part-A/synchronoussgd.py",
		"./Part-A/asyncsgd.py",
		"./Part-A/launch_asyncsgd.sh",
		"./Part-A/README",
		"./Part-A/group-" + group + "-part-a.pdf",
		"./Part-A/batchsynchronoussgd.py",
		"./Part-A/batchasyncsgd.py",
		"./Part-A/launch_batchasyncsgd.sh",
		"./Part-B/group-" + group + "-part-b.pdf",
		"./Part-B/PartBApplication1Question1.scala",
		"./Part-B/PartBApplication2Question1.scala",
		"./Part-B/PartBApplication2Question2.scala",
		"./Part-B/PartBApplication2Question3.scala",
		"./Part-B/PartBApplication2Question4.scala",
		"./Part-B/PartBApplication2Question5.scala",
		"./Part-B/PartBApplication2Question6.scala",
		"./Part-B/README"
		]

part_a_number = 0
part_b_number = 0

print "\n\033[1mHAND-IN CONSISTENCY CHECKER\033[0m\n"
# Check the presence of the required files for Part-A
output_str = '\033[1m' + "Checking Part-A Submission Folder ...." + '\033[0m' + "\n"
print output_str
for i in range(0,9):
	fname = required_files[i]
	output_str = '\t\033[1m' + str(i+1) + ". "
	output_str += fname.split("/")[-1] + ": " + '\033[0m'
	if os.path.isfile(fname):
		part_a_number += 1
		output_str += " File found :)"
	else:
		output_str += " FILE NOT FOUND :("
                if i > 5:
                        output_str += " This is for the extra-credit question."
	print output_str

# Check the presence of the extra files for Part-A
expected_files = [ ".",  "..", "launch_asyncsgd.sh","batchsynchronoussgd.py","batchasyncsgd.py","launch_batchasyncsgd.sh","README", "asyncsgd.py" ,"bigmatrixmultiplication.py", "group-" + group + "-part-a.pdf", "synchronoussgd.py"]
existing_files = subprocess.check_output('ls -a Part-A', shell=True, stderr=subprocess.STDOUT).split("\n")[:-1]
extra_files = []

for file in existing_files:
	if file not in expected_files:
		extra_files.append(file)

if len(extra_files) != 0:
	print "\nThere are \033[1mEXTRA-FILES\033[0m in your Part-A folder. Please remove these files:\n" 
	for i in range(0,len(extra_files)):
		print "\t\033[1m" + str(i) +". " + extra_files[i] +  "\033[0m"	

# Check the presence of the required files for Part-B
output_str = '\n\033[1m' + "Checking Part-B Submission Folder ...." + '\033[0m' + "\n"
print output_str
for i in range(9,18):
        fname = required_files[i]
        output_str = '\t\033[1m' + str(i-8) + ". "
        output_str += fname.split("/")[-1] + ": " + '\033[0m'
        if os.path.isfile(fname):
		part_b_number += 1
                output_str += " File found :)"
        else:
                output_str += " FILE NOT FOUND :("
		if i > 13 and i != 16:
			output_str += " This is for the extra-credit question."
        print output_str

# Check the presence of the extra files for Part-B
expected_files = [ ".",  "..", "README", "PartBApplication1Question1.scala" ,"PartBApplication2Question1.scala", "group-" + group + "-part-b.pdf", "PartBApplication2Question2.scala", "PartBApplication2Question3.scala", "PartBApplication2Question4.scala", "PartBApplication2Question5.scala", "PartBApplication2Question6.scala"]
existing_files = subprocess.check_output('ls -a Part-B', shell=True, stderr=subprocess.STDOUT).split("\n")[:-1]
extra_files_b = []

for file in existing_files:
        if file not in expected_files:
                extra_files_b.append(file)

if len(extra_files_b) != 0:
        print "\nThere are \033[1mEXTRA-FILES\033[0m in your Part-B folder. Please remove these files:\n"
        for i in range(0,len(extra_files_b)):
                print "\t\033[1m" + str(i) +". " + extra_files_b[i] +  "\033[0m"

print "\n\033[1mSUBMISSION SUMMARY\033[0m"
print "PART-A: " + str(part_a_number) +" files out of 9 have been submitted (3 files for extra-credit part)."
print "PART-B: " +str(part_b_number) + " files out of 9 have been submitted (3 files for extra-credit part)."
if extra_files:
	print "THERE ARE EXTRA FILES UNDER PART-A. Please DELETE them."
if extra_files_b:
	print "THERE ARE EXTRA FILES UNDER PART-B. Please DELETE them."
