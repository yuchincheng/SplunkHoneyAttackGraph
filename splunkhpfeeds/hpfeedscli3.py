import os
import sys
import time
import datetime
import json
import logging
import hpfeeds
import splunklib.client as client
import splunkcli
import getConfig

from defineConfig import SPLUNK_ROOT

class ThugFiles:
    def __init__(self, channel, host, port, ident, secret):
        self.CHANNEL = channel
        self.HOST = host
        self.PORT = port
        self.IDENT = ident
        self.SECRET = secret
        self.LOGFILE = os.path.join(SPLUNK_ROOT, "data", channel, channel+".log")
        self.SAMPLEDIR = os.path.join(SPLUNK_ROOT, "data", channel, "files")
        
        if not os.path.exists(self.SAMPLEDIR): 
            os.mkdir(self.SAMPLEDIR)
            
        self.log       = logging.getLogger(self.CHANNEL)
        print "self.log = %s" % self.log
        self.handler   = logging.FileHandler(self.LOGFILE)
        print "self.handler = %s" % self.handler
        self.formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        print "self.formatter = %s" % self.formatter
        self.handler.setFormatter(self.formatter)
        print "self.formatter = %s" % self.handler.setFormatter
        self.log.addHandler(self.handler)
        print "self.log.addHandler = %s" % self.log.addHandler(self.handler)
        self.log.setLevel(logging.INFO)
        print "self.log.setLevel = %s" % self.log.setLevel(logging.INFO)
            
    #def on_error(self, payload):
    #        self.log.critical("Error message from server: %s" % (payload, ))
    #        self.hpc.stop()

    def run(self):
        
        def on_message(identifier, channel, payload):
            print "start to run on_message:"
            print "on_message: %s" % payload
            try: 
                decoded = json.loads(str(payload))
            except: 
                decoded = {'raw': payload}
                
            print "on_message_identifier=%s" % identifier
            print "on_message_channel=%s" % channel
            print "on_message_channel=%s" % payload
    
            if not 'md5' in decoded or not 'data' in decoded:
                self.log.info("Received message does not contain hash or data - Ignoring it")
                return
                
            csv    = ', '.join(['{0} = {1}'.format(i, decoded[i]) for i in ['url', 'md5', 'sha1', 'type']])
            outmsg = '%s PUBLISH channel = %s, identifier = %s, %s' % (datetime.datetime.now().ctime(), channel, identifier, csv)
            self.log.info(outmsg)
            print "self.log.info(outmsg)= %s" % self.log.info(outmsg)
            print "outmsg = %s" % outmsg
                
            filedata = decoded['data'].decode('base64') 
            fpath    = os.path.join(self.SAMPLEDIR, decoded['md5'])
    
            with open(fpath, 'wb') as fd:
                print "filedata=%s" % filedata
                fd.write(filedata)
                    
            indexname = channel.replace('.', '-')
            print "indexname = %s" % indexname
            P = getConfig.Config()
            splunkconf = P.confParser("splunk.conf")
            spk_curl = splunkcli.splunkconn(splunkconf['Splunk_setting_1']['user'],splunkconf['Splunk_setting_1']['password'])
            print "spk_curl=%s" % spk_curl 
            splunkcli.indexattach(spk_curl, indexname, payload)
        
        def on_error(payload):
            self.log.critical("Error message from server: %s" % (payload, ))
            hpc.stop()
        
        while True:
            try:
                self.hpc = hpfeeds.new(self.HOST, self.PORT, self.IDENT, self.SECRET)
                print "Connected to %s" % (self.hpc.brokername, )
                self.log.info("Connected to %s" % (self.hpc.brokername, ))
                CHANNELS = [self.CHANNEL]
                print CHANNELS
                print type(CHANNELS)
                self.hpc.subscribe(CHANNELS)
                print self.hpc.subscribe(CHANNELS)
                
                indexname = (self.CHANNEL).replace('.', '-')
                print "indexname = %s" % indexname
                P = getConfig.Config()
                splunkconf = P.confParser("splunk.conf")
                spk_curl = splunkcli.splunkconn(splunkconf['Splunk_setting_1']['user'],splunkconf['Splunk_setting_1']['password'])
                print "spk_curl=%s" % spk_curl 
                splunkcli.indexattach(spk_curl, indexname, "Connected to %s" % (self.hpc.brokername, ))
            
            except hpfeeds.FeedException:
                break

            try:
                print "self.hpd.run"
                #payload = "testing 123456"
                self.hpc.run(on_message, on_error)
                #self.hpc.run(self.on_message(self.IDENT, self.CHANNEL, payload), self.on_error)
                print "self.hpd.run2"
            except:
                self.hpc.close()
                time.sleep(20)
        
        
                



