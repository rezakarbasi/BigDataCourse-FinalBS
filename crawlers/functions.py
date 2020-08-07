

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

def GetLinks(text):
    inputText = text
    inputText = inputText.replace('\n',' ')
    out = []

    a=inputText
    idx = a.find('http')
    while idx!= -1 :
        a=a[idx:]
        idx = a.find('"')
        url = a[:idx]
        a=a[idx:]
        idx = a.find('http')

        if not(url in out):
            out.append(url)

    a=inputText
    idx = a.find('@')
    while idx!= -1 :
        a=a[idx:]
        idx = a.find(' ')
        link = a[:idx]
        a=a[idx:]
        idx = a.find('@')

        if not(link in out):
            out.append(link)

    return out

def FindWords(syms,normText):
    out = []
    words = normText.split()

    for i in syms:
        if i in words:
            out.append(i)

    return out 

def ListToString(l:list):
    a=str(l)[1:-1]
    return a.replace("'",'').replace('"','').replace(' ','')