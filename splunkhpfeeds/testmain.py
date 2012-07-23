import hpfeedscli3
import sys


if __name__ == '__main__':
    try: 
        print "Program start 1:"
        f = hpfeedscli3.ThugFiles("thug.files", "hpfeeds.honeycloud.net", 10000, "6iyxt@hp1", "3a1jiv3d30fn6avs")  
        f.run()
    except KeyboardInterrupt:
        sys.exit(0)
