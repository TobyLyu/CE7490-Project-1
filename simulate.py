import numpy as np 
import pandas as pd
import tqdm
import matplotlib.pyplot as plt
from util.DataLoader import DataLoader
from util.Simulator import FaasSimulator
from multiprocessing import Process, Pool
import ipdb
from util.Analyzer import SystemAnalyzer

arg_lst = [i*8*1024 for i in [4, 8, 18, 32, 64, 128]]

# arg_lst = [5, 10, 20, 30, 45, 60, 90, 120, 1440]
# intv_lst = [1, 2, 4, 5, 6, 8, 10, 20, 30, 45, 60, 90, 120, 1440]
# intv_lst = [5, 10, 20, 30]
max_day = 12
# max_day = 1


# process for fixed-interval strategy
def proc(i):
    global arg_lst
    loader = DataLoader(path="dataset")
    loader.load_dataset(i)
    
    simulator = FaasSimulator(loader)
    simulator.prepare()
    
    cold_rate_lst = [[] for _ in range(len(arg_lst))]
    mem_rate_lst = [[] for _ in range(len(arg_lst))]
    for idx, arg in enumerate(arg_lst):
        [cold_rate, mem_rate] = simulator.run_sys(arg)
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
    result_lst = pool.map(proc, list(range(1, max_day+1)))
        # result_lst.append(result)
        
    pool.close()
    pool.join()
    
    # print(result_lst)
    print("..................Simulation Finish.....................")
    
    itv_len = len(intv_lst)

    # unpack result from multi-processing list
    cold_rate_data = [[] for _ in range(itv_len)]
    mem_rate_data = [[] for _ in range(itv_len)]
    for j in range(itv_len): # itv
        for i in range(len(result_lst)): # day 
            # ipdb.set_trace()
            cold_rate_data[j] += result_lst[i][0][j]  # [day][cold/mem][intv]
            mem_rate_data[j] += result_lst[i][1][j]

    plt.figure(1)
    SystemAnalyzer.draw_cold_rate(cold_rate_data, legend=intv_lst)
        
    plt.figure(2)
    SystemAnalyzer.draw_mem_rate(mem_rate_data, cold_rate_data, legend=intv_lst) 
    
    SystemAnalyzer.save_result(cold_rate_data, "result", "cold_rate_result.csv")
    SystemAnalyzer.save_result(mem_rate_data, "result", "mem_rate_result.csv")