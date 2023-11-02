import pandas as pd
import numpy as np
import random
import os
import copy
from time import time
import threading 
import functools
import matplotlib.pyplot as plt

import ipdb
from tqdm import tqdm, trange
from util.Manager import BaselineManager, FixIntervalsimApp, SystemAnalyzer


def call_it(instance, name, arg):
    "indirect caller for instance methods and multiprocessing"
    return getattr(instance, name)(arg)

class FaasSimulator():
    def __init__(self, data_loader) -> None:
        self.data_loader = data_loader
        self.data_info = data_loader.data_info
        self.max_day = data_loader.max_day
        self.data_path = data_loader.data_path
        self.inv_raw = [[] for _ in range(self.max_day)]
        self.exe_raw = [[] for _ in range(self.max_day)]
        self.system_clock = [0, 0] # [day, sec]
        self.system_monitor = dict()
        self.owner_dict = dict()
        self.day_len = 1440
        self.invc_flag = [2, 1]
        
        self.baseM = BaselineManager(data_info=self.data_info, owner_dict=self.owner_dict)
    
    def __generate_func_dur(self, day, ID_arr):
        """generate randomly function runtime usage based on dataset information

        Args:
            day (str): day of running
            ID_arr (ndarray): ID in a array ([OwnerID, AppID, FunctionID])

        Returns:
            float: randomly allocated run time in minute
        """
        
        [owner_id, app_id, func_id] = ID_arr
        # func_properties = self.data_info.loc[day].loc[owner_id].loc[app_id].loc[func_id]
        # func_properties = self.data_info.loc[day, owner_id, app_id, func_id]
        DurAve = self.data_info.loc[(day, owner_id, app_id, func_id), "DurAve"]
        DurProb = self.data_info.loc[(day, owner_id, app_id, func_id), "DurProb"]
        rand_dur = random.choices(DurAve, weights=DurProb, k=1)[0] / 60000.0
        
        return rand_dur

        
    def __generate_app_mem(self, day, ID_arr):
        """generate randomly app memory allocation based on dataset information

        Args:
            day (str): day of running
            ID_arr (ndarray): ID in a array ([OwnerID, AppID, FunctionID])

        Returns:
            int: randomly allocated memory
        """
        
        [owner_id, app_id, func_id] = ID_arr
        func_properties = self.data_info.loc[day, owner_id, app_id, func_id]
        
        MemAve = func_properties["MemAve"]
        MemProb = func_properties["MemProb"]
        rand_mem_alloc = random.choices(MemAve, weights=MemProb, k=1)[0]
        
        return int(rand_mem_alloc)
    
    def __gen_invoc_series(self, arg):
        i = arg[0]
        save = arg[1]
        invoc_info_col = ["HashOwner", "HashApp", "HashFunction"]

        inv_tmp = copy.deepcopy(self.inv_raw[i]).set_index(invoc_info_col).sort_index()
        
        # func_name = inv_tmp[invoc_info_col].values
        func_name = np.vstack([[inv_tmp.index.get_level_values(0).values, 
                                inv_tmp.index.get_level_values(1).values, 
                                inv_tmp.index.get_level_values(2).values]]).T 
        # ipdb.set_trace()        
        
        for j in trange(self.day_len):
            exec_func_name = func_name[inv_tmp.loc[:, str(j+1)].values.astype(bool)]
            # with tqdm(total=len(exec_func_name)) as pbar:
            for func_ in exec_func_name:
                # pbar.update(1)
                try:
                    dur = self.__generate_func_dur(day=str(i+1), ID_arr=func_)
                except KeyError:
                    inv_tmp.loc[(func_[0], func_[1], func_[2]), str(j+1)] = 0
                    continue # we do not have info for this func
                func_start_time = max(0.01, j + 1 - dur)
                for idx, tick in enumerate(np.ceil(np.arange(func_start_time, j+1, 1)).astype(int)):
                    inv_tmp.loc[(func_[0], func_[1], func_[2]), str(tick)] = self.invc_flag[int(bool(idx))]
        
        self.exe_raw[i] = inv_tmp.sort_index()
        if save == True:
            output_path = os.path.join(self.data_path, "execution_series_{}.csv".format(i+1))
            inv_tmp.to_csv(output_path)

    
    def gen_invoc_series(self, save=True):
        print("Generating function invocation series...")
        
        
        thread_lst = []
        for i in range(self.max_day):
            print("Processing Day{} data...".format(i+1))
            self.__gen_invoc_series([i, save])
        
        

    
    # def exec(self):
    #     for func in self.func_lst:
    #         ID_arr = func["HashOwner", "HashApp", "HashFunction"].values
    #         rand_mem = self.__generate_app_mem(self.day, ID_arr)
    #         self.baseM.register_func(ID_arr, rand_mem)
    #         # self.owner_dict[ID_arr[0]].app_dict[ID_arr[1]].func_dict[ID_arr[2]].step()
            
    # def step(self):
    #     for owner in self.owner_dict:
    #         owner.step()
            
    # def update(self):
    #     for owner in self.owner_dict:
    #         owner.update()

    def prepare(self):
        print("Loading execution series...")
        if self.data_loader.exec_:
            print("!!!Lucky AGAIN we found execution files from CSV! Loading it now!")
            for i in trange(self.max_day):
                self.exe_raw[i] = pd.read_csv(os.path.join(self.data_path, self.data_loader.exec_files[i])).set_index(["HashOwner", "HashApp", "HashFunction"]).sort_index()
        else:
            print("Oh no! Seems we need to generate a execution file for dataset!")
            print("It can be PRETTY slow...But no worries we will save the result after finishing!")
            for i in trange(self.max_day):
                self.inv_raw[i] = pd.read_csv(os.path.join(self.data_path, self.data_loader.inv_files[i]))
            self.gen_invoc_series()
        print("SUCCESS!")
        
        
    def run(self):
        cold_start_rate_lst = []
        wasted_mem_rate_lst = []
        for intv in [10, 20, 50, 100, 150, 200]:
        # for intv in [10, 20]:
            cold_rate = []
            mem_rate = []
            for i in range(self.max_day):
                owner_lst = self.exe_raw[i].index.get_level_values(0).unique().values
                with tqdm(total=len(owner_lst)) as pbar:
                    for owner in owner_lst:
                        pbar.update(1)
                        
                        app_lst = self.exe_raw[i].loc[owner].index.get_level_values(0).unique().values
                        for app in app_lst:
                            # ipdb.set_trace()
                            exec_series = np.any(self.exe_raw[i].loc[owner, app].iloc[:, 1:].values, axis=0)
                            simApp = FixIntervalsimApp(intv, exec_series)
                            cold_rate.append(simApp.cold_start_rate)
                            mem_rate.append(simApp.mem_waste_rate)
            cold_start_rate_lst.append(cold_rate)
            wasted_mem_rate_lst.append(mem_rate)

        # ipdb.set_trace()

        for i in range(6): 
            SystemAnalyzer.draw_cold_rate(cold_start_rate_lst[i])
        plt.show()