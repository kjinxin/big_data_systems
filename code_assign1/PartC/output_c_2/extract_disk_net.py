import os

def solve(data):
	print data
	data = [int(f) for f in data]
	data = [sum(data[i:i + 5]) for i in range(0, len(data), 5)]
	data = [data[i] - data[i + 1] for i in range(0, 1)]    
	return data


osfiles = os.listdir(".")
osfiles.sort()
print osfiles
mr_disk = [f for f in osfiles if f.startswith("disk")]
mr_net = [f for f in osfiles if f.startswith("net")]

mr_disk_read = []
mr_disk_write = []
for file in mr_disk:
    with open(file) as f:
	lines = f.read().splitlines()
	line = lines[-1].split()
	mr_disk_read.append(line[5])
	mr_disk_write.append(line[9])

mr_disk_read = solve(mr_disk_read)
mr_disk_write = solve(mr_disk_write)


mr_net_rec = []
mr_net_trans = []
for file in mr_net:
    with open(file) as f:
	lines = f.read().splitlines()
	line = lines[len(lines) - 2].split()
	mr_net_rec.append(line[1])
	mr_net_trans.append(line[9])

mr_net_rec = solve(mr_net_rec)
mr_net_trans = solve(mr_net_trans)

print "disk_read_c2"
print mr_disk_read
print "mr_disk_write_c2"
print mr_disk_write

print "net_rec_c2"
print mr_net_rec
print "net_trans_c2"
print mr_net_trans
