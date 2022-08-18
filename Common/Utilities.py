import ROOT   

def ListToVector(list, type="string"):
    vec = ROOT.std.vector(type)() 
    for item in list: 
        vec.push_back(item)
    return vec 
 