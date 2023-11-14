import os
import timeit
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

#--------------Retweets----------------------------------------------
def crear_grafo_retweets(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        # Suponiendo que "user" es el usuario que hace el retweet y "retweeted_status" es el tweet original
        if 'retweeted_status' in tweet:
            retweeter_id = tweet['user']['screen_name']
            retweeted_id = tweet['retweeted_status']['user']['screen_name']

            # Añadir nodos
            G.add_node(retweeter_id)
            G.add_node(retweeted_id)

            # Añadir arista
            G.add_edge(retweeter_id, retweeted_id)

    return G

def crear_json_retweets(tweets):
    retweets_info = defaultdict(lambda: {'receivedRetweets': 0, 'tweets': defaultdict(list)})

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            original_user = tweet['retweeted_status']['user']['screen_name']
            original_tweet_id = tweet['retweeted_status']['id_str']
            retweeter = tweet['user']['screen_name']

            # Almacenar la información del retweet
            retweets_info[original_user]['tweets'][original_tweet_id].append(retweeter)
            retweets_info[original_user]['receivedRetweets'] += 1

    # Ordenar por número total de retweets recibidos
    sorted_retweets = sorted(retweets_info.items(), key=lambda x: x[1]['receivedRetweets'], reverse=True)

    # Construir la estructura final del JSON
    json_structure = {'retweets': []}
    for user, data in sorted_retweets:
        tweets_data = [{'tweetId: ' + tweet_id: {'retweetedBy': retweeters} for tweet_id, retweeters in data['tweets'].items()}]
        json_structure['retweets'].append({
            'username': user,
            'receivedRetweets': data['receivedRetweets'],
            'tweets': tweets_data
        })

    return json_structure


#-------Menciones------------------------------------
def crear_grafo_menciones(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            mencionante_id = tweet['user']['id']
            G.add_node(mencionante_id)

            for mencionado in tweet['entities']['user_mentions']:
                mencionado_id = mencionado['id']
                G.add_node(mencionado_id)
                G.add_edge(mencionante_id, mencionado_id)

    return G

def crear_json_menciones(tweets):
    menciones_info = defaultdict(lambda: {'mentionBy': defaultdict(int), 'tweets': []})

    for tweet in tweets:
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            tweet_id = tweet['id_str']
            mencionante = tweet['user']['screen_name']

            for mencionado in tweet['entities']['user_mentions']:
                mencionado_nombre = mencionado['screen_name']
                menciones_info[mencionado_nombre]['mentionBy'][mencionante] += 1
                menciones_info[mencionado_nombre]['tweets'].append(tweet_id)

    # Preparar la estructura final del JSON
    final_structure = {'mentions': []}
    for username, data in menciones_info.items():
        mention_list = []
        for mentioner, count in data['mentionBy'].items():
            mention_list.append({'mentionBy': mentioner, 'tweets': list(set(data['tweets']))})

        final_structure['mentions'].append({
            'username': username,
            'receivedMentions': sum(data['mentionBy'].values()),
            'mentions': mention_list
        })

    # Ordenar por número total de menciones recibidas
    final_structure['mentions'].sort(key=lambda x: x['receivedMentions'], reverse=True)

    return final_structure

#-----Co-Retweet-----------------
def crear_grafo_corretweets(tweets):
    G = nx.Graph()  # Usar un grafo no dirigido

    retweet_pairs = defaultdict(set)  # Para almacenar pares de autores retuiteados por el mismo usuario

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            retweeter = tweet['user']['screen_name']
            original_author = tweet['retweeted_status']['user']['screen_name']

            # Agregar este autor al conjunto de autores retuiteados por el retweeter
            retweet_pairs[retweeter].add(original_author)

    # Crear aristas entre todos los pares de autores retuiteados por el mismo usuario
    for retweeter, authors in retweet_pairs.items():
        for author1 in authors:
            for author2 in authors:
                if author1 != author2:
                    G.add_edge(author1, author2)

    return G

def crear_json_corretweets(tweets):
    # Recopilar información sobre quién retwitteó a quién
    retweeters_por_autor = defaultdict(set)
    for tweet in tweets:
        if 'retweeted_status' in tweet:
            autor_original = tweet['retweeted_status']['user']['screen_name']
            retweeter = tweet['user']['screen_name']
            retweeters_por_autor[autor_original].add(retweeter)

    # Identificar co-retweets
    co_retweets = defaultdict(lambda: {'retweeters': set(), 'totalCoretweets': 0})
    autores = list(retweeters_por_autor.keys())
    for i in range(len(autores)):
        for j in range(i + 1, len(autores)):
            autor1, autor2 = autores[i], autores[j]
            co_retweeters = retweeters_por_autor[autor1].intersection(retweeters_por_autor[autor2])
            if co_retweeters:
                co_retweets[(autor1, autor2)]['retweeters'] = co_retweeters
                co_retweets[(autor1, autor2)]['totalCoretweets'] = len(co_retweeters)

    # Construir la estructura del JSON
    json_structure = {'coretweets': []}
    for authors, data in co_retweets.items():
        json_structure['coretweets'].append({
            'authors': {'u1': authors[0], 'u2': authors[1]},
            'totalCoretweets': data['totalCoretweets'],
            'retweeters': list(data['retweeters'])
        })

    return json_structure


#------Descomprimir archivos-------
def descomprimir_tweets(directory, start_date_str, end_date_str, output_base_directory):
    start_date = datetime.strptime(start_date_str, "%d-%m-%y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%y")

    tweets=[]

    # Crear directorio de salida basado en la fecha y hora
    output_directory = os.path.join(output_base_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

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
                                        for file in os.listdir(hour_path):
                                            if file.endswith('.bz2'):
                                                file_path = os.path.join(hour_path, file)
                                                #descomprimir_archivo(file_path, output_directory)
                                                with bz2.BZ2File(file_path, 'rb') as f:
                                                    for line in f:
                                                        tweet = json.loads(line)
                                                        tweets.append(tweet)
    #print(tweets[:3])
    grafo_retweets=crear_grafo_retweets(tweets)
    nx.write_gexf(grafo_retweets, 'rt.gexf')
    retweets_json = crear_json_retweets(tweets)
    with open('rt.json', 'w') as file:
        json.dump(retweets_json, file, indent=4)
    grafo_menciones = crear_grafo_menciones(tweets)
    nx.write_gexf(grafo_menciones, 'mencion.gexf')
    menciones_json = crear_json_menciones(tweets)
    with open('mencion.json', 'w') as file:
        json.dump(menciones_json, file, indent=4)
        grafo_co_retweet = crear_grafo_corretweets(tweets)
    nx.write_gexf(grafo_co_retweet, 'corrtw.gexf')
    co_retweet_json = crear_json_corretweets(tweets)
    with open('corrtw.json', 'w') as file:
        json.dump(co_retweet_json, file, indent=4)

def main(argv):
    # Valores por defecto
    directory = 'data'
    start_date = None
    end_date = None
    hashtag_file = None
    
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
   initialTime = timeit.default_timer()
   main(sys.argv[1:])
   finalTime = timeit.default_timer()
   print(f"Total execution time: {finalTime - initialTime} seconds")