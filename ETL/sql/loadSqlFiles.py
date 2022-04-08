import os

def getSqlFiles():
    fileDictionary = {}
    directory = os.path.dirname(__file__) + '/'
    files = [f for f in os.listdir(directory) if os.path.isfile(directory + f) and f.endswith('.sql')]
    for f in files:
        fd = open(directory + f, 'r')
        sql = fd.read()
        fd.close()
        fileDictionary[f] = sql
    return fileDictionary