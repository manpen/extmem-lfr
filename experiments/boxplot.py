#!/usr/bin/env python3
import fileinput

current_key = None
values = []

blocks = ["", ""]

def output(key, values):
    if key is None:
        return
    
    if len(values) < 5*5:
        print("# %s has only %d values; skip" % (key, len(values)))
        return
    
    values.sort(key=lambda x: x[0])
    
    for i in range(2):
        if i == 0:
            tvalues = [x[1] for x in values[:len(values)//5]]
        else:
            tvalues = [x[1] for x in values[len(values)//-5:]]
        
        tvalues.sort()
        
        line = str(key) + " " + str(len(tvalues))
        for x in range(5):
            line += " " + str(tvalues[round(x/4.0 * (len(tvalues)-1))])
            
        blocks[i] += line + "\n"

for line in fileinput.input():
    if '' == line.strip():
        continue
    
    key, cnt, value = [x.strip() for x in line.split(" ")]
    
    if key != current_key:
        output(current_key, values)
        current_key = key
        values = []
    
    values.append((int(cnt), float(value)))
    
output(current_key, values)    
        

print(blocks[0] + "\n\n\nT\n" + blocks[1])