import os
import json
import shutil
import bz2
import getopt
import networkx as nx
from collections import defaultdict
import sys
from datetime import datetime

def main(argv):
    # Valores por defecto
    directory = 'data'
    start_date = None
    end_date = None
    hashtag_file = None

    # print(argv)

    try:
        opts,args=getopt.getopt(argv,"-d:-fi:")
    except getopt.GetoptError as err:
        print(err)
    
    print(opts)

if __name__ == "__main__":
   main(sys.argv[1:])
