import ROOT 
import re 

def ListToVector(list, type="string"):
    vec = ROOT.std.vector(f"{type}")() 
    for item in list:
        vec.push_back(item)
    return vec 

def GetReportInfo(fileName):
    eff_vec = []
    num_vec = []
    den_vec = []
    cumul_vec = []
    x_axes_vec = []
    with open(fileName) as f:
        lines = f.readlines()

    count = 0
    total_den_idx = 0
    tot_den =1
    for line in lines:
        count += 1
        
        lline = re.split('=|:| ',line)  
        num_idx = lline.index('pass')+1
        den_idx = lline.index('all')+1
        if(count == 1 ): 
            total_den_idx=den_idx
            tot_den = float(lline[total_den_idx]) 
        num = float(lline[num_idx])
        den = float(lline[den_idx])
        eff = num/den 
        cumul = num/tot_den
        num_vec.append(num)
        den_vec.append(den)
        eff_vec.append(eff)
        cumul_vec.append(cumul) 
        x_label= ' '.join(l for l in lline[0:num_idx-1])
        x_axes_vec.append(x_label)
    return num_vec, den_vec, cumul_vec, eff_vec,x_axes_vec



