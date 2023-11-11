import os
import json
import shutil
import bz2
import getopt
import networkx as nx
from collections import defaultdict
from datetime import datetime

def main(argv):
    # Valores por defecto
    directory = 'data'
    start_date = None
    end_date = None
    hashtag_file = None

    # Analizar los argumentos de la línea de comandos
    try:
        opts, args = getopt.getopt(argv, "d:fi:ff:h:", ["directory=", "startdate=", "enddate=", "hashtagfile="])
    except getopt.GetoptError:
        print('Uso: generador.py -d <path> -fi <fecha inicial> -ff <fecha final> -h <archivo de hashtags>')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-d", "--directory"):
            directory = arg
        elif opt in ("-fi", "--startdate"):
            start_date = datetime.strptime(arg, "%d-%m-%y")
        elif opt in ("-ff", "--enddate"):
            end_date = datetime.strptime(arg, "%d-%m-%y")
        elif opt in ("-h", "--hashtagfile"):
            hashtag_file = arg

    # Aquí va la lógica para procesar los tweets

if __name__ == "__main__":
   main(sys.argv[1:])
