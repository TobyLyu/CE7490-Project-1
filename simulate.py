import numpy as np 
import pandas as pd
import pdb
import matplotlib.pyplot as plt
from util.DataLoader import DataLoader
from util.Simulator import FaasSimulator

loader = DataLoader(path="dataset")
simulator = FaasSimulator(loader)

def proc(i):
    loader.load_dataset(i)
    simulator.prepare()
    return simulator.run()



        # plt.figure(1)

        # for i in range(6): 
        #     SystemAnalyzer.draw_cold_rate(cold_start_rate_lst[i])
        # plt.show()
        
        plt.figure(2)
        for i in range(6): 
            SystemAnalyzer.draw_mem_rate(wasted_mem_rate_lst[i], cold_start_rate_lst[i])
        plt.show()        