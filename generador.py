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



def procesar_tweets(tweets):
    # Inicialización de grafos y estructuras de datos para JSON
    grafo_retweets = nx.DiGraph()
    retweets_info = defaultdict(lambda: {'receivedRetweets': 0, 'tweets': defaultdict(list)})

    grafo_menciones = nx.DiGraph()
    menciones_info = defaultdict(lambda: {'mentionBy': defaultdict(int), 'tweets': []})

    grafo_corretweets = nx.Graph()
    retweeters_por_autor = defaultdict(set)

    # Procesar cada tweet
    for tweet in tweets:
        # Procesamiento para retweets
        if 'retweeted_status' in tweet:
            retweeter_screen_name = tweet['user']['screen_name']
            retweeted_screen_name = tweet['retweeted_status']['user']['screen_name']

            grafo_retweets.add_node(retweeter_screen_name)
            grafo_retweets.add_node(retweeted_screen_name)
            grafo_retweets.add_edge(retweeter_screen_name, retweeted_screen_name)

            retweets_info[retweeted_screen_name]['tweets'][tweet['retweeted_status']['id_str']].append(retweeter_screen_name)
            retweets_info[retweeted_screen_name]['receivedRetweets'] += 1

        # Procesamiento para menciones
        if 'entities' in tweet and 'user_mentions' in tweet['entities']:
            mencionante_screen_name = tweet['user']['screen_name']
            mencionante_id = tweet['user']['id']

            grafo_menciones.add_node(mencionante_id)

            for mencionado in tweet['entities']['user_mentions']:
                mencionado_screen_name = mencionado['screen_name']
                mencionado_id = mencionado['id']

                grafo_menciones.add_node(mencionado_id)
                grafo_menciones.add_edge(mencionante_id, mencionado_id)

                menciones_info[mencionado_screen_name]['mentionBy'][mencionante_screen_name] += 1
                menciones_info[mencionado_screen_name]['tweets'].append(tweet['id_str'])

        # Procesamiento para co-retweets
        if 'retweeted_status' in tweet:
            retweeter_screen_name = tweet['user']['screen_name']
            retweeted_screen_name = tweet['retweeted_status']['user']['screen_name']

            retweeters_por_autor[retweeted_screen_name].add(retweeter_screen_name)
    
    for autor in retweeters_por_autor:
        for otro_autor in retweeters_por_autor:
            if autor != otro_autor:
                co_retweeters = retweeters_por_autor[autor].intersection(retweeters_por_autor[otro_autor])
                if co_retweeters:
                    grafo_corretweets.add_edge(autor, otro_autor, weight=len(co_retweeters))


    # Finalizar JSON de retweets
    retweets_json = finalizar_json_retweets(retweets_info)

    # Finalizar JSON de menciones
    menciones_json = finalizar_json_menciones(menciones_info)

    # Finalizar JSON de co-retweets
    corretweets_json = finalizar_json_corretweets(retweeters_por_autor)

    return grafo_retweets, retweets_json, grafo_menciones, menciones_json, grafo_corretweets, corretweets_json

def finalizar_json_retweets(retweets_info):
    sorted_retweets = sorted(retweets_info.items(), key=lambda x: x[1]['receivedRetweets'], reverse=True)
    json_structure = {'retweets': []}
    for user, data in sorted_retweets:
        tweets_data = [{'tweetId: ' + tweet_id: {'retweetedBy': retweeters} for tweet_id, retweeters in data['tweets'].items()}]
        json_structure['retweets'].append({
            'username': user,
            'receivedRetweets': data['receivedRetweets'],
            'tweets': tweets_data
        })
    return json_structure

def finalizar_json_menciones(menciones_info):
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
    return final_structure

def finalizar_json_corretweets(retweeters_por_autor):
    json_structure = {'coretweets': []}
    for autor, retweeters in retweeters_por_autor.items():
        for autor2, retweeters2 in retweeters_por_autor.items():
            if autor != autor2 and retweeters.intersection(retweeters2):
                co_retweeters = retweeters.intersection(retweeters2)
                json_structure['coretweets'].append({
                    'authors': {'u1': autor, 'u2': autor2},
                    'totalCoretweets': len(co_retweeters),
                    'retweeters': list(co_retweeters)
                })
    return json_structure

#------Descomprimir archivos-------
def descomprimir_tweets(directory, start_date_str, end_date_str,hashtag_file=None):
    start_date = datetime.strptime(start_date_str, "%d-%m-%y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%y")

    hashtags = set()
    if hashtag_file:
        with open(hashtag_file, 'r') as file:
            for line in file:
                hashtags.add(line.strip().lower())

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
                                                with bz2.BZ2File(file_path, 'rb') as f:
                                                    for line in f:
                                                        tweet = json.loads(line)
                                                        if not hashtags or any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', [])):
                                                            yield tweet


#--------------------Main-------------------------------------------------
def main(argv):

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

    for opt, arg in opts:
        if opt == "-d":
            directory = arg
        elif opt in ["--fi"]:
            start_date = arg
        elif opt in ["--ff"]:
            end_date = arg
        elif opt == "-h":
            hashtag_file = arg

    tweets=descomprimir_tweets(directory,start_date,end_date,hashtag_file)

    grafo_retweets, retweets_json, grafo_menciones, menciones_json, grafo_corretweets, corretweets_json=procesar_tweets(tweets)

    nx.write_gexf(grafo_retweets, 'rt.gexf')
    with open('rt.json', 'w') as file:
        json.dump(retweets_json, file, indent=4)

    nx.write_gexf(grafo_menciones, 'mencion.gexf')
    with open('mencion.json', 'w') as file:
        json.dump(menciones_json, file, indent=4)

    nx.write_gexf(grafo_corretweets, 'corrtw.gexf')
    with open('corrtw.json', 'w') as file:
        json.dump(corretweets_json, file, indent=4)



if __name__ == "__main__":
   initialTime = timeit.default_timer()
   main(sys.argv[1:])
   finalTime = timeit.default_timer()
   print(f"Tiempo total de ejecucion: {finalTime - initialTime} seconds")