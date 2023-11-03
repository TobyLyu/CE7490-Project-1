import pandas as pd
import numpy as np
import random
import os
import copy
import math
from time import time
import threading 
import functools
import matplotlib.pyplot as plt

import ipdb
from tqdm import tqdm, trange
from util.Manager import FixIntervalsimApp, FixIntervalsimSys


def call_it(instance, name, arg):
    "indirect caller for instance methods and multiprocessing"
    return getattr(instance, name)(arg)

class FaasSimulator():
    def __init__(self, data_loader) -> None:
        self.data_loader = data_loader
        self.data_info = data_loader.data_info
        self.max_day = data_loader.max_day
        self.data_path = data_loader.data_path
        self.inv_raw = []
        self.exe_raw = []
        self.system_clock = [0, 0] # [day, sec]
        self.system_monitor = dict()
        self.owner_dict = dict()
        self.day_len = 1440
        self.invc_flag = [2, 1]
        
        # self.baseM = BaselineManager(data_info=self.data_info, owner_dict=self.owner_dict)
    
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
    
    def __gen_invoc_series(self, i, save=True):
        # i = arg[0]
        # save = arg[1]
        invoc_info_col = ["HashOwner", "HashApp", "HashFunction"]

        inv_tmp = copy.deepcopy(self.inv_raw).set_index(invoc_info_col).iloc[:, 1:].sort_index()
        
        # ipdb.set_trace()
        
        # func_name = inv_tmp[invoc_info_col].values
        func_name = np.vstack([[inv_tmp.index.get_level_values(0).values, 
                                inv_tmp.index.get_level_values(1).values, 
                                inv_tmp.index.get_level_values(2).values]]).T 
        # ipdb.set_trace()        
        with tqdm(total=len(func_name)) as pbar:
            for func_ in func_name:
                pbar.update(1)
                func_invc_series = inv_tmp.loc[(func_[0], func_[1], func_[2])].values
                # print(func_invc_series)
                # ipdb.set_trace()
                for t, state in enumerate(func_invc_series):
                    if state:
                        try:
                            dur = self.__generate_func_dur(day=str(i), ID_arr=func_)
                            if not dur: # some duration is unrepresentative in the dataset (dur = 0). we set it to 0.01min
                                dur = 0.01
                                # continue
                        except KeyError:
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), :] = 0
                            continue # we do not have info for this func
                        # ipdb.set_trace()   
                        func_start_time = max(0.01, t + 1 - dur)
                        exec_time = np.ceil(np.arange(func_start_time, t + 1, 1)).astype(int).astype(str) # the duration of this exec
                        if dur <= 1: # within 1min: num of invocation+1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), exec_time[0]] = state + 1
                        else:   # func has some duration: first time slot: num of invocation+1; following time slot: 1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), exec_time[0]] = state + 1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), (x for x in exec_time[1:])] = 1
        
        # for j in trange(self.day_len):
        #     exec_func_name = func_name[inv_tmp.loc[:, str(j+1)].values.astype(bool)]
        #     # with tqdm(total=len(exec_func_name)) as pbar:
        #     for func_ in exec_func_name:
        #         # pbar.update(1)
        #         try:
        #             dur = self.__generate_func_dur(day=str(i+1), ID_arr=func_)
        #         except KeyError:
        #             inv_tmp.loc[(func_[0], func_[1], func_[2]), str(j+1)] = 0
        #             continue # we do not have info for this func
        #         func_start_time = max(0.01, j + 1 - dur)
        #         for idx, tick in enumerate(np.ceil(np.arange(func_start_time, j+1, 1)).astype(int)):
        #             inv_tmp.loc[(func_[0], func_[1], func_[2]), str(tick)] = self.invc_flag[int(bool(idx))]
        
        self.exe_raw = inv_tmp.sort_index()
        if save == True:
            output_path = os.path.join(self.data_path, "execution_series_{}.csv".format(i))
            inv_tmp.to_csv(output_path)

            

    
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

    def prepare(self, i):
        print("[P_{}] Getting execution series...".format(i))
        # exec_ = f"invocations_per_function_md.anon.d{i:02}.csv"
        # if exec_ in self.data_loader.inv_files:
        exec_ = "execution_series_{}.csv".format(i)
        if exec_ in self.data_loader.exec_files:
            print("[P_{}] Loading execution series from CSV...".format(i))
            self.exe_raw = pd.read_csv(os.path.join(self.data_path, self.data_loader.exec_files[i-1])).set_index(["HashOwner", "HashApp", "HashFunction"]).sort_index()
            # self.exe_raw[:] = self.exe_raw[:].values.astype(bool) # for raw dataset
        else:
            print("[P_{}] Generating series from dataset...".format(i))
            self.inv_raw = pd.read_csv(os.path.join(self.data_path, self.data_loader.inv_files[i-1]))
            self.__gen_invoc_series(i)
        print("[P_{}] Series getting SUCCESS!".format(i))
        
        
    def run(self, intv):
        # [intv, i] = arg
        # cold_start_rate_lst = []
        # wasted_mem_rate_lst = []
        # for intv in [10, 20, 50, 100, 150, 200]:
        # for intv in [10, 20]:
        cold_rate = []
        mem_rate = []
        # for k in range(self.max_day):
        owner_lst = self.exe_raw.index.get_level_values(0).unique().values
        with tqdm(total=len(owner_lst)) as pbar:
            for owner in owner_lst:
                pbar.update(1)
                
                app_lst = self.exe_raw.loc[owner].index.get_level_values(0).unique().values
                for app in app_lst:
                    # ipdb.set_trace()
                    # func_exec_series = self.exe_raw[i].loc[owner, app].iloc[:, 1:].values
                    exec_series = np.max(self.exe_raw.loc[owner, app].iloc[:, 1:].values, axis=0)
                    
                    simApp = FixIntervalsimApp(intv, exec_series)
                    if not simApp.never_launch:
                        cold_rate.append(simApp.cold_start_rate)
                        mem_rate.append(simApp.mem_waste_rate)
        # cold_start_rate_lst.append(cold_rate)
        # wasted_mem_rate_lst.append(mem_rate)

        return [cold_rate, mem_rate]
        # ipdb.set_trace()

    def run_sys(self, intv):
        app_exe_raw = self.exe_raw.reset_index().groupby(["HashOwner", "HashApp"]).max().sort_index().iloc[:, 1:]
        app_name_list = app_exe_raw.reset_index().iloc[:, :2].values
        # app_name_list = np.vstack([[self.exe_raw.index.get_level_values(0).values, 
        #                             self.exe_raw.index.get_level_values(1).values, 
        #                             self.exe_raw.index.get_level_values(2).values]]).T 
        
        # ipdb.set_trace()
        simSys = FixIntervalsimSys(app_name_list)
        
        for i in trange(self.day_len):
            t = i + 1
            exec_now = app_exe_raw[str(t)].values
            simSys.update(exec_now, intv)

        return [simSys.cal_cold_rate(), simSys.cal_mem_waste()]