import os
import ConfigParser

from defineConfig import SPLUNK_ROOT

class Config:
    """Hpfeeds Configuration file parser."""

    def __init__(self):
         pass
    
    def  confParser(self, configfile):
        """HPfeeds Conf parser process."""
        sectiondict = dict()
        cfg=os.path.join(SPLUNK_ROOT, "config", configfile)
        config = ConfigParser.ConfigParser()
        config.read(cfg)
        
        for section in config.sections():
            globals()[section] = dict()
            for name, raw_value in config.items(section):             
                try:
                     globals()[section][name] = config.getboolean(section, name)
                except ValueError:
                    try:
                        globals()[section][name] = config.getint(section, name)
                    except ValueError:
                        globals()[section][name] = config.get(section, name)
            
            
            sectiondict[section] = globals()[section] 
        return sectiondict


    def cfg_mkdirs(self, outdir, chan):
        outdir_path = os.path.join(SPLUNK_ROOT, outdir, chan) 
        if not os.path.exists (outdir_path):
                os.makedirs (outdir_path)
       
