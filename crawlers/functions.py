

def GetDataFromField(d, field):
    if type(field) != list:
        field = [field]
    o = d

    for f in field:
        if type(o) != dict:
            return False, 'ERROR DICT'

        if f in o.keys():
            o = o[f]
        else:
            return False, o
    return True, o
