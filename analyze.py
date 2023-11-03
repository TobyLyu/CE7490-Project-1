from util.Analyzer import SystemAnalyzer
from matplotlib import pyplot as plt
import numpy as np
import os

path = "result"

filename = os.path.join(path, "cold_rate_result.csv")
cold_rate_data = np.loadtxt(filename, delimiter=',')

filename = os.path.join(path, "mem_rate_result.csv")
mem_rate_data = np.loadtxt(filename, delimiter=',')

intv_lst = [5, 10, 20, 30, 45, 60, 90, 120, 1440]

plt.figure(1)
SystemAnalyzer.draw_cold_rate(cold_rate_data, legend=intv_lst)
    
plt.figure(2)
SystemAnalyzer.draw_mem_rate(mem_rate_data, cold_rate_data, legend=intv_lst) 

SystemAnalyzer.save_result(cold_rate_data, "result", "cold_rate_result.csv")
SystemAnalyzer.save_result(mem_rate_data, "result", "mem_rate_result.csv")