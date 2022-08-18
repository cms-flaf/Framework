import ROOT 
import re 
compression_algorithms= {
        "kInherit":-1,
        "kUseGlobal":0,
        "kZLIB":1,
        "kLZMA":2,
        "kOldCompressionAlgo":3,
        "kLZ4": 4, 
        "kZSTD": 5, 
        "kUndefined":6,
    }

def ListToVector(list, type="string"):
    vec = ROOT.std.vector(f"{type}")() 
    for item in list:
        #print(item)
        vec.push_back(item)
    return vec 
 