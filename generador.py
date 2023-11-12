import os
import json
import shutil
import bz2
import getopt
import networkx as nx
from collections import defaultdict
import sys
from datetime import datetime

def descomprimir_archivo(file_path, output_directory):
    with bz2.BZ2File(file_path, 'rb') as file:
        data = file.read()
        
        # Extraer el nombre del archivo sin la extensión .bz2
        base_name = os.path.basename(file_path)
        output_file_name = os.path.splitext(base_name)[0] + ".json"

        # Crear la ruta completa del archivo de salida
        output_file_path = os.path.join(output_directory, output_file_name)

        # Escribir los datos en un nuevo archivo
        with open(output_file_path, 'wb') as output_file:
            output_file.write(data)




def descomprimir_tweets(directory, start_date_str, end_date_str, output_base_directory):
    start_date = datetime.strptime(start_date_str, "%d-%m-%y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%y")

    for year in os.listdir(directory):
        year_path = os.path.join(directory, year)
        if os.path.isdir(year_path):
            for month in os.listdir(year_path):
                month_path = os.path.join(year_path, month)
                if os.path.isdir(month_path):
                    for day in os.listdir(month_path):
                        day_path = os.path.join(month_path, day)
                        if os.path.isdir(day_path):
                            for hour in os.listdir(day_path):
                                hour_path = os.path.join(day_path, hour)
                                if os.path.isdir(hour_path):
                                    current_date = datetime(year=int(year), month=int(month), day=int(day))
                                    if start_date <= current_date <= end_date:
                                        # Crear directorio de salida basado en la fecha y hora
                                        output_directory = os.path.join(output_base_directory, year, month, day, hour)
                                        if not os.path.exists(output_directory):
                                            os.makedirs(output_directory)

                                        for file in os.listdir(hour_path):
                                            if file.endswith('.bz2'):
                                                file_path = os.path.join(hour_path, file)
                                                descomprimir_archivo(file_path, output_directory)

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

    descomprimir_tweets(directory,start_date,end_date,"output")
    
    # Descomentar para eliminar carpeta de output al finalizar
    # try:
    #     if os.path.exists("output"):
    #         shutil.rmtree("output")
    # except Exception as e:
    #     print(f"Error al eliminar la carpeta: {e}")



if __name__ == "__main__":
   main(sys.argv[1:])