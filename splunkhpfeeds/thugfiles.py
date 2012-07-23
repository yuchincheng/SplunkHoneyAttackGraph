import os
import sys
import time
import datetime
import json
import logging
import hpfeeds
import splunklib.client as client


HOST        = 'hpfeeds.honeycloud.net'
PORT        = 10000
CHANNELS    = ['thug.files',]
IDENT       = '6iyxt@hp1'
SECRET      = '3a1jiv3d30fn6avs'
OUTFILE     = '/projects/grab.log'
OUTDIR      = '/projects/thugfiles/'

log       = logging.getLogger("thug.files")
handler   = logging.FileHandler(OUTFILE)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

class ThugFiles:
    def __init__(self):
        if not os.path.exists(OUTDIR): 
            os.mkdir(OUTDIR)

    def run(self):
        def on_message(identifier, channel, payload):
            print "on_message: %s" % payload
            print identifier
            print channel
            print payload
            service = client.connect(username="admin", password="yuchin1234")
            cn = service.indexes["testing"].attach()
            try: 
                decoded = json.loads(str(payload))
            except: 
                decoded = {'raw': payload}

            if not 'md5' in decoded or not 'data' in decoded:
                log.info("Received message does not contain hash or data - Ignoring it")
                return
            
            csv    = ', '.join(['{0} = {1}'.format(i, decoded[i]) for i in ['url', 'md5', 'sha1', 'type']])
            outmsg = 'PUBLISH channel = %s, identifier = %s, %s' % (channel, identifier, csv)
            log.info(outmsg)
            cn.write(outmsg)
            
            filedata = decoded['data'].decode('base64') 
            fpath    = os.path.join(OUTDIR, decoded['md5'])

            with open(fpath, 'wb') as fd:
                fd.write(filedata)

        def on_error(payload):
            log.critical("Error message from server: %s" % (payload, ))
            self.hpc.stop()

        while True:
            try:
                self.hpc = hpfeeds.new(HOST, PORT, IDENT, SECRET)
                log.info("Connected to %s" % (self.hpc.brokername, ))
                print type(CHANNELS)
                self.hpc.subscribe(CHANNELS)
                print self.hpc.subscribe(CHANNELS)
            except hpfeeds.FeedException:
                break

            try:
                print "self.hpd.run : thigfiles"
                self.hpc.run(on_message, on_error)
                print "self.hpd.run2 : thugfiles"
            except:
                self.hpc.close()
                time.sleep(20)

if __name__ == '__main__':
    try: 
        f = ThugFiles()
        f.run()
    except KeyboardInterrupt:
        sys.exit(0)

