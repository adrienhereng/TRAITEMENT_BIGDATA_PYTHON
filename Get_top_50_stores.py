import csv
import os
import multiprocessing as mp
import time
import collections

# fonction permettant de transformer un fichier PSV en dictionnaire generateur
def Generateur_Dict(file_name, start,end) :
    i = 0
    col_name=[]

    for row in open(file_name, 'r'):
        if i == 0:
            col_name.append(row.replace('prix\n','ca').split(sep ='|'))
        if i> start:
            yield dict(zip(col_name[0], row.replace('\n','').split(sep ='|')))
        if i >= end:
            break
        i=i+1

# fonction permettant d'effectuer l'aggregation sum() sur une liste de dictionnaire en parallele
def parallel_process(file_name, start,end, group_by_key, sum_value_keys):
    dataset = Generateur_Dict(file_name, start, end)

    return group_and_sum_dataset(dataset, group_by_key, sum_value_keys)

# fonction permettant d'effectuer une aggregation sum() sur une liste de dictionnaire
def group_and_sum_dataset(dataset, group_by_key, sum_value_keys):
    container = collections.defaultdict(collections.Counter)

    for item in dataset:
        key = str([item[i] for i in group_by_key]).replace('[','').replace(']','').replace('"','').replace("'",'')
        values = {sum_value_keys:float(item[sum_value_keys])}
        container[key].update(values)

    new_dataset = [
        dict(**dict(dict(zip(group_by_key,item[0].split(sep=',')))),**dict(item[1].items()))
            for item in container.items()
    ]

    del (container)
    return new_dataset

# fonction callback
def get_ca_magasin(arg):
    global ca_par_magasin

    for i in arg:
        ca_par_magasin.append(i)
    del(arg)

# fonction permettant de transformer une liste de dictionnaire en fichier CSV
def list_dict_to_csv(file_name, dataset,keys):
    with open('top-50-stores.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dataset)

if __name__=='__main__':
    print('Process started...')
    start = time.time()
    file_name='randomized-transactions-202009.psv'
    file_nb_row = 162413730
    ca_par_magasin=[]
    nb_process=os.cpu_count()-1
    chunk_size = int(round(file_nb_row/nb_process))


    pool = mp.Pool(nb_process)

    for i in range(nb_process):
        b = i*chunk_size
        pool.apply_async(parallel_process,args= (file_name, b, b+chunk_size, ['code_magasin'], 'ca'),callback=get_ca_magasin)

    pool.close()
    pool.join()


    dataset_res=group_and_sum_dataset(ca_par_magasin,['code_magasin'], 'ca')
    dataset_res.sort(key=lambda item: [item['ca']],reverse=True)
    dataset_res = dataset_res[:50]
    keys = dataset_res[0].keys()

    list_dict_to_csv(file_name, dataset_res,keys)

    end = time.time()
    print('The process took: ' + str(end - start)+' seconds')
