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
from mpi4py import MPI

def combinarGrafo(grafo_original,grafo_nuevo):
    grafo_original.add_nodes_from(grafo_nuevo.nodes(data=True))
    grafo_original.add_edges_from(grafo_nuevo.edges(data=True))

def combinar_json_cortweets(json_maestro, json_nuevo):
    for coretweet_nuevo in json_nuevo["coretweets"]:
        autores = (coretweet_nuevo["authors"]["u1"], coretweet_nuevo["authors"]["u2"])
        autores_key = tuple(sorted(autores))  # Crear una clave ordenada

        if autores_key not in json_maestro:
            json_maestro[autores_key] = coretweet_nuevo
        else:
            # Fusionar la lista de retweeters, evitando duplicados
            retweeters_existentes = set(json_maestro[autores_key]["retweeters"])
            retweeters_nuevos = set(coretweet_nuevo["retweeters"])
            json_maestro[autores_key]["retweeters"] = list(retweeters_existentes | retweeters_nuevos)
            json_maestro[autores_key]["totalCoretweets"] = len(json_maestro[autores_key]["retweeters"])

    return json_maestro

def combinar_json_menciones(json_maestro, json_nuevo):
    for usuario_nuevo in json_nuevo["mentions"]:
        username = usuario_nuevo["username"]

        # Si el usuario no existe en el maestro, añádelo
        if username not in json_maestro:
            json_maestro[username] = usuario_nuevo
        else:
            # Si el usuario ya existe, fusiona la información de menciones
            usuario_maestro = json_maestro[username]
            for mencion_nueva in usuario_nuevo["mentions"]:
                mentionBy_nueva = mencion_nueva["mentionBy"]
                tweets_nuevos = set(mencion_nueva["tweets"])

                # Buscar si ya existe la misma mención por el mismo usuario
                mencion_existente = next((m for m in usuario_maestro["mentions"] if m["mentionBy"] == mentionBy_nueva), None)
                if mencion_existente:
                    # Actualizar la lista de tweets, evitando duplicados
                    mencion_existente["tweets"] = list(set(mencion_existente["tweets"]) | tweets_nuevos)
                else:
                    # Agregar la nueva mención
                    usuario_maestro["mentions"].append(mencion_nueva)

    return json_maestro

def combinar_json_retweet(json_maestro, json_nuevo):
    # Recorrer cada usuario en el JSON nuevo
    for usuario in json_nuevo["retweets"]:
        username = usuario["username"]
        if username not in json_maestro:
            # Si el usuario no existe en el maestro, añádelo
            json_maestro[username] = usuario
        else:
            # Si el usuario ya existe, fusiona la información de retweets
            for tweet_id, retweet_info in usuario["tweets"].items():
                if tweet_id in json_maestro[username]["tweets"]:
                    # Fusionar las listas de retweetedBy si el tweet ya existe
                    json_maestro[username]["tweets"][tweet_id]["retweetedBy"].extend(retweet_info["retweetedBy"])
                    # Eliminar duplicados si es necesario
                    json_maestro[username]["tweets"][tweet_id]["retweetedBy"] = list(set(json_maestro[username]["tweets"][tweet_id]["retweetedBy"]))
                else:
                    # Si el tweet no existe, añádelo
                    json_maestro[username]["tweets"][tweet_id] = retweet_info

    return json_maestro

def procesar_tweets(tweets,grt, jrt, gm, jm, gcrt, jcrt):
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
        if grt or jrt:
            if 'retweeted_status' in tweet:
                retweeter_screen_name = tweet['user']['screen_name']
                retweeted_screen_name = tweet['retweeted_status']['user']['screen_name']

                grafo_retweets.add_node(retweeter_screen_name)
                grafo_retweets.add_node(retweeted_screen_name)
                grafo_retweets.add_edge(retweeter_screen_name, retweeted_screen_name)

                retweets_info[retweeted_screen_name]['tweets'][tweet['retweeted_status']['id_str']].append(retweeter_screen_name)
                retweets_info[retweeted_screen_name]['receivedRetweets'] += 1

        # Procesamiento para menciones
        if gm or jm:
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
        if gcrt or jcrt:
            if 'retweeted_status' in tweet:
                retweeter_screen_name = tweet['user']['screen_name']
                retweeted_screen_name = tweet['retweeted_status']['user']['screen_name']

                retweeters_por_autor[retweeted_screen_name].add(retweeter_screen_name)
    
    if gcrt or jcrt:
        for autor in retweeters_por_autor:
            for otro_autor in retweeters_por_autor:
                if autor != otro_autor:
                    co_retweeters = retweeters_por_autor[autor].intersection(retweeters_por_autor[otro_autor])
                    if co_retweeters:
                        grafo_corretweets.add_edge(autor, otro_autor, weight=len(co_retweeters))


    resultados = {}
    if grt:
        resultados['grafo_retweets'] = grafo_retweets
    if jrt:
        resultados['retweets_json'] = finalizar_json_retweets(retweets_info)
    if gm:
        resultados['grafo_menciones'] = grafo_menciones
    if jm:
        resultados['menciones_json'] = finalizar_json_menciones(menciones_info)
    if gcrt:
        resultados['grafo_corretweets'] = grafo_corretweets
    if jcrt:
        resultados['corretweets_json'] = finalizar_json_corretweets(retweeters_por_autor)

    return resultados

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


def listar_archivos(directory, start_date, end_date):
    archivos=[]

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
                                                archivos.append(file_path)
    
    return archivos

def procesar_archivo(archivo,hashtag_file=None):
    tweets=[]
    hashtags = set()
    if hashtag_file:
        with open(hashtag_file, 'r') as file:
            for line in file:
                hashtags.add(line.strip().lower())

    with bz2.BZ2File(archivo, 'rb') as f:
        for line in f:
            tweet = json.loads(line)
            if not hashtags or any(hashtag['text'] in hashtags for hashtag in tweet.get('entities', {}).get('hashtags', [])):
                tweets.append(tweet)
    
    return tweets

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def main(argv):
    ti = timeit.default_timer()
    directory = 'data'
    start_date = None
    end_date = None
    hashtag_file = None
    opts = []
    grt, jrt, gm, jm, gcrt, jcrt = False, False, False, False, False, False
    
    i = 0
    while i < len(argv):
        argumento = argv[i]
        valor = argv[i + 1] if i + 1 < len(argv) else ''
        if argumento.startswith('--'):
            opts.append((argumento, ''))
        else:
            if argumento.startswith('-') and not valor.startswith('-'):
                opts.append((argumento, valor))
                i += 2
                continue
            elif argumento.startswith('-') and valor.startswith('-') and not valor.startswith('--'):
                pass
        i += 1
    
    for opt, arg in opts:
        if opt == '-d':
            input_directory = arg
        if opt == '-ff':
            end_date = datetime.strptime(arg, "%d-%m-%y")
        if opt == '-fi':
            start_date = datetime.strptime(arg, "%d-%m-%y")
        if opt == '-h':
            hashtag_file = arg
        if opt == "--grt":
            grt = True
        if opt == "--jrt":
            jrt = True
        if opt == "--gm":
            gm = True
        if opt == "--jm":
            jm = True
        if opt == "--gcrt":
            gcrt = True
        if opt == "--jcrt":
            jcrt = True

    if (rank == 0):

        grafo_retweets = nx.DiGraph()
        retweets_info = {}

        grafo_menciones = nx.DiGraph()
        menciones_info = {}

        grafo_corretweets = nx.Graph()
        coretweets_info = {}

        tweets=[]
        archivos=listar_archivos(directory,start_date,end_date)
        status = MPI.Status()
        num_workers = size - 1
        workers_done = 0

        # Enviar el primer lote de archivos
        for i in range(1, size):
            if archivos:
                archivo = archivos.pop(0)
                comm.send(archivo, dest=i, tag=0)
            else:
                comm.send(None, dest=i, tag=1)
                workers_done += 1

        # Manejar el resto del proceso
        while workers_done < num_workers:
            tweets.extend(comm.recv(source=MPI.ANY_SOURCE, status=status))
            worker_rank = status.Get_source()
            if archivos:
                archivo = archivos.pop(0)
                comm.send(archivo, dest=worker_rank, tag=0)
            else:
                comm.send(None, dest=worker_rank, tag=1)
                workers_done += 1   

        #procesar tweets
        num_workers = size - 1
        workers_done = 0

        lotes = [tweets[i:i + 10] for i in range(0, len(tweets), 10)]

        resultados_raiz = procesar_tweets(lotes.pop(0), grt, jrt, gm, jm, gcrt, jcrt)
        resultados_totales = [resultados_raiz]

        for i in range(1, size):
            if lotes:
                comm.send(lotes.pop(0), dest=i, tag=0)
            else:
                comm.send(None, dest=i, tag=1)
                workers_done += 1

        # Recibir resultados y enviar más trabajo
        while workers_done < num_workers:
            resultado = comm.recv(source=MPI.ANY_SOURCE, status=status)
            resultados_totales.append(resultado)
            trabajador = status.Get_source()  # Almacenar el rango del trabajador
            if lotes:
                comm.send(lotes.pop(0), dest=trabajador, tag=0)  # Usar el rango almacenado aquí
            else:
                comm.send(None, dest=trabajador, tag=1)
                workers_done += 1
        
        for resultado in resultados_totales:
            for key, value in resultado.items():
                if key=='grafo_retweets':
                    combinarGrafo(grafo_retweets,value)
                if key=='retweets_json':
                    retweets_info=combinar_json_retweet(retweets_info,value)
                if key=='grafo_menciones':
                    combinarGrafo(grafo_menciones,value)
                if key=='menciones_json':
                    menciones_info=combinar_json_menciones(menciones_info,value)
                if key=='grafo_corretweets':
                    combinarGrafo(grafo_corretweets,value)
                if key=='corretweets_json':
                    coretweets_info=combinar_json_cortweets(coretweets_info,value)
        
        if grt:
            nx.write_gexf(grafo_retweets, 'rtp.gexf')
        if jrt:
            with open('rtp.json', 'w') as file:
                json.dump(retweets_info, file, indent=4)
        if gm:
            nx.write_gexf(grafo_menciones, 'mencionp.gexf')
        if jm:
            with open('mencionp.json', 'w') as file:
                json.dump(menciones_info, file, indent=4)
        if gcrt:
            nx.write_gexf(grafo_corretweets, 'corrtwp.gexf')
        if jcrt:
            with open('corrtwp.json', 'w') as file:
                json.dump(coretweets_info, file, indent=4)

    else:
        while True:
            archivo = comm.recv(source=0, tag=MPI.ANY_TAG, status=MPI.Status())
            if archivo is None:
                break
            resultado = procesar_archivo(archivo)
            comm.send(resultado, dest=0)

        while True:
            lote = comm.recv(source=0, tag=MPI.ANY_TAG, status=MPI.Status())
            if lote is None:
                break
            resultado = procesar_tweets(lote, grt, jrt, gm, jm, gcrt, jcrt)
            comm.send(resultado, dest=0)

    if rank==0:
        tf = timeit.default_timer()
        print(f"Tiempo total de ejecucion: {tf - ti} seconds")
    

if __name__ == "__main__":
    main(sys.argv[1:])