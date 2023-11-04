import copy
import matplotlib.pyplot as plt
import os
import numpy as np
from tqdm import tqdm, trange

class SystemAnalyzer():
    def __init__(self) -> None:
        pass
    
    @classmethod
    def draw_cold_rate(cls, cold_rate_in, legend):
        """draw cold rate vs cdf figure

        Args:
            cold_rate (list): a list cold rates for all apps
        """
        
        cold_rate = copy.deepcopy(cold_rate_in)
        rates = [x * 0.01 for x in range(101)] # cold rate percentage
        for i in range(len(cold_rate)):
            cold_rate[i].sort() # ascending order
            cdf_list = [0 for _ in range(101)]
            ptr = 0
            for idx, rate in enumerate(rates):
                while ptr < len(cold_rate[i]) and cold_rate[i][ptr] <= rate: 
                    ptr += 1
                    
                cdf_list[idx] = ptr * 1.0 / len(cold_rate[i]) # cdf
        
            plt.plot([x*100 for x in rates], cdf_list)
        
        plt.axhline(y=0.75, color='darkgray', linestyle='-') 
        plt.xlabel("App Cold Start (%)")
        plt.ylabel("CDF")
        plt.xlim = [0, 105]
        plt.ylim = [0, 1.05]
        plt.grid()
        plt.legend([str(x)+"-min" for x in legend])
        plt.show()

    @classmethod
    def draw_mem_rate(cls, mem_rate_in, cold_rate_in, legend):
        """draw memory wasted rate at 3rd quartile app cold start 

        Args:
            mem_rate (list): a list of wasted memory for all apps
            cold_rate (list): a list cold rates for all apps
        """
        mem_idle_rate = []
        cold_start_rate = []
        assert len(mem_rate_in) == len(cold_rate_in)
        assert len(mem_rate_in) > 1
        
        mem_rate = copy.deepcopy(mem_rate_in)
        cold_rate = copy.deepcopy(cold_rate_in)
        for i in range(len(mem_rate)):
            sort_idx = np.argsort(np.array(cold_rate[i]))
            cold_rate[i] = np.array(cold_rate[i])[sort_idx]
            mem_rate[i] = np.array(mem_rate[i])[sort_idx]
            
            idx_75_pert = int(len(cold_rate[i]) * 0.75)
            cold_start_rate.append(cold_rate[i][idx_75_pert])
            mem_idle_rate.append(mem_rate[i][idx_75_pert])
            
        # normalized wasted memory time
        mem_idle_rate = [x/mem_idle_rate[1]*100 for x in mem_idle_rate]
        # mem_idle_rate = [x*100 for x in mem_idle_rate]
        cold_start_rate = [x*100 for x in cold_start_rate]
        # plt.scatter(cold_start_rate, mem_idle_rate,color='fuchsia')
        for i in range(len(cold_start_rate)):
            plt.scatter(cold_start_rate[i], mem_idle_rate[i])
        plt.plot(cold_start_rate, mem_idle_rate, color='fuchsia')
        plt.axhline(y=100, color='darkgray', linestyle='-')
        plt.xlabel("3rd Quartile App Cold Start (%)")
        plt.ylabel("Normalized Wasted Memory Time (%)")
        plt.xlim = [0, 100]
        plt.ylim = [90, 130]
        plt.legend([str(x)+"-min" for x in legend])
        plt.show()

    @classmethod
    def save_result(cls, data_in, path, name):
        data = copy.deepcopy(data_in)
        data = np.array(data)
        filename = os.path.join(path, name)
        np.savetxt(filename, data, delimiter=',')
        
    @classmethod
    def draw_idle_busy_time(cls, exe_raw):
        zero_ave = np.array([])
        one_ave = np.array([])
        for app_exe_raw in exe_raw:
            app_exe_raw = app_exe_raw.groupby(["HashOwner", "HashApp"]).max().sort_index().iloc[:, 1:].values.astype(bool)
            zero_sec = []
            zero_cot = 0
            one_sec = []
            one_cot = 0
            for i in trange(app_exe_raw.shape[0]):
                zeros = False
                ones = False
                for j in range (app_exe_raw.shape[1]):
                    if not app_exe_raw[i][j]:
                        zero_cot += 1
                    else:
                        one_cot += 1
                    if not app_exe_raw[i][j] and not zeros:
                        zeros = True
                        ones = False
                        one_sec.append(one_cot)
                        one_cot = 0
                    elif app_exe_raw[i][j] and not ones:
                        ones = True
                        zeros = False
                        zero_sec.append(zero_cot)
                        zero_cot = 0
                        
            # useful_sec_z = zero_sec > 0
            # zero_sec = zero_sec[useful_sec_z]
            # zero_cot = zero_cot[useful_sec_z]
            
            # useful_sec_o = one_sec > 0
            # one_sec = one_sec[useful_sec_o]
            # one_cot = one_cot[useful_sec_o]
            
            # zero_ave = np.append(zero_ave, np.divide(zero_cot, zero_sec))
            # zero_ave = np.append(zero_ave, np.zeros(sum(~useful_sec_z)))
            # one_ave = np.append(one_ave, np.divide(one_cot, one_sec))
            # one_ave = np.append(one_ave, np.zeros(sum(~useful_sec_o)))
        
        zero_ave = zero_sec
        one_ave = one_sec
            
        cls.save_result(np.array(zero_ave).T, "result", "idle_sec.csv")
        cls.save_result(np.array(one_ave).T, "result", "busy_sec.csv")
            
        plt.figure(1)
        hist_, bin_edge = np.histogram(zero_ave, bins=np.arange(1441))
        plt.hist(hist_, bins=bin_edge)
        
        plt.figure(2)
        hist_, bin_edge = np.histogram(one_ave, bins=np.arange(1441))
        plt.hist(hist_, bins=bin_edge)        