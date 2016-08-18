import fileinput
import random

random.seed(1)

def print_med(current_grp):
    if len(current_grp) == 0: return
    r = random.randint(0, len(current_grp)-1)
    print(", ".join(current_grp[r]))
    return


    current_grp.sort(key=lambda x: float(x[-1]))
    print(", ".join(current_grp[int(len(current_grp) / 2)]))
    if (len(current_grp) > 5):
        print(", ".join(current_grp[1]))
        print(", ".join(current_grp[-2]))
    
keysize = 10

data = []
for line in fileinput.input():
    if "None" in line: continue
    values = [x.strip() for x in line.split(",")]
    jobid = int(values[12][1:].split("-")[0])
    if (jobid >= 3560659 or "Orig" != values[11]):
        print(line.strip())
    else:
        this_key = values[:keysize] + values[13:15]
        data.append(['-'.join(this_key)] + values.copy())
    
data.sort(key=lambda x: x[0])    
    
current_key = ""
current_grp = []
for values in data:
    this_key = values[0]
    if this_key != current_key:
        print_med(current_grp)
        current_grp = []
        current_key = this_key

    current_grp.append(values[1:])

        
print_med(current_grp)
            

        
    