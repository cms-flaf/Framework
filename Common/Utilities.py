import ROOT 
def ListToVector(list):
    vec = ROOT.std.vector("string")() 
    for item in list:
        vec.push_back(item)
    return vec 
