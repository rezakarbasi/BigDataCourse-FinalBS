

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


def GetHashtags(text):
    t=text.replace('\n',' ')
    idx = t.find('#')
    out = []
    while idx!=-1 :

        t = t[idx+1:]
        tag = t[:t.find(' ')]
        out.append(tag)
        idx = t.find('#')

    return out
