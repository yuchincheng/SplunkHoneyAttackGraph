import splunklib.client as client


def splunkconn(username, password):
    service = client.connect(username=username, password=password)
    return service

def indexcreate(service, indexname):
    print indexname
    if service.indexes.contains(indexname):
        print "Index '%s' already exists" % indexname
        return

    service.indexes.create(indexname)
    index = service.indexes[indexname]
    index.enable()
    
def indexclean(indexname):
    index = service.indexes[indexname]
    index.clean()

def indexattach(service, indexname, payload):
    cn = service.indexes[indexname].attach()
    cn.write(payload)
    print "splunkcli = %s" % payload
