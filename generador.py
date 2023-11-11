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
        opts,args=getopt.getopt(argv,"d:h:",["fi=","ff="])
    except getopt.GetoptError as err:
        print(err)
        print('Uso: generador.py -d <path> --fi=<fecha inicial> --ff=<fecha final> -h <archivo de hashtags>')
        sys.exit(2)
    
    print(opts)

    for opt, arg in opts:
        print(opt)
        if opt == "-d":
            directory = arg
        elif opt in ["--fi"]:
            start_date = arg
        elif opt in ["--ff"]:
            end_date = arg
        elif opt == "-h":
            hashtag_file = arg


if __name__ == "__main__":
   main(sys.argv[1:])
