import numpy as np 
import pandas as pd
import tqdm
import matplotlib.pyplot as plt
from util.DataLoader import DataLoader
from util.Simulator import FaasSimulator
from multiprocessing import Process, Pool
import ipdb
from util.Manager import SystemAnalyzer

def proc(i):
    loader = DataLoader(path="dataset")
    loader.load_dataset(i)
    
    simulator = FaasSimulator(loader)
    simulator.prepare(i)
    
    intv_lst = [10, 20, 50, 100, 150, 200]
    # intv_lst = [10, 20]
    cold_rate_lst = [[] for _ in range(len(intv_lst))]
    mem_rate_lst = [[] for _ in range(len(intv_lst))]
    for idx, intv in enumerate(intv_lst):
        [cold_rate, mem_rate] = simulator.run(intv)
        cold_rate_lst[idx] = cold_rate
        mem_rate_lst[idx] = mem_rate
    return [cold_rate_lst, mem_rate_lst]
    # return True


print(".....................Starting.............................")

if __name__ == '__main__': 
    # p_lst = []
    # for i in range(12):
    #     p_lst.append(Process(target=proc, args=(i+1,)))
    #     p_lst[i].start()
        
    # [p_lst[i].join for i in range(12)]
    
    result_lst = []
    pool = Pool(24)
    result_lst = pool.map(proc, list(range(1, 13)))
        # result_lst.append(result)
        
    pool.close()
    pool.join()
    
    # print(result_lst)
    print("Simulation Finish.")
    
    itv_len = 6

    cold_rate_data = [[] for _ in range(itv_len)]
    mem_rate_data = [[] for _ in range(itv_len)]
    for j in range(itv_len): # itv
        for i in range(len(result_lst)): # day 
            # ipdb.set_trace()
            cold_rate_data[j] += result_lst[i][0][j]  # [day][cold/mem][intv]
            mem_rate_data[j] += result_lst[i][1][j]

    plt.figure(1)
    for i in range(itv_len): 
        SystemAnalyzer.draw_cold_rate(cold_rate_data[i])
    plt.show()
        
    plt.figure(2)
    for i in range(6): 
        SystemAnalyzer.draw_mem_rate(mem_rate_data[i], cold_rate_data[i])
    plt.show()        