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
from util.Manager import FixIntervalsimApp, FixIntervalsimSys, GreedysimSys


def call_it(instance, name, arg):
    "indirect caller for instance methods and multiprocessing"
    return getattr(instance, name)(arg)

class FaasSimulator():
    def __init__(self, data_loader) -> None:
        self.data_loader = data_loader
        # self.data_info = data_loader.data_info
        self.dur_info = data_loader.data_info.sort_index()[["DurAve", "DurProb"]]
        self.mem_info = data_loader.data_info.groupby(["Day", "HashOwner", "HashApp"]).max().sort_index()[["MemAve", "MemProb"]]
        self.max_day = data_loader.max_day
        self.data_path = data_loader.data_path
        self.day_id = data_loader.day_id
        self.inv_raw = []
        self.exe_raw = []
        self.system_clock = [0, 0] # [day, sec]
        self.system_monitor = dict()
        self.owner_dict = dict()
        self.day_len = 1440
        self.invc_flag = [2, 1]
        
        # self.baseM = BaselineManager(data_info=self.data_info, owner_dict=self.owner_dict)
    
    def __generate_func_dur(self, ID_arr):
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
        DurAve = self.dur_info.loc[(str(self.day_id), owner_id, app_id, func_id), "DurAve"]
        DurProb = self.dur_info.loc[(str(self.day_id), owner_id, app_id, func_id), "DurProb"]
        rand_dur = random.choices(DurAve, weights=DurProb, k=1)[0] / 60000.0
        
        return rand_dur

        
    def __generate_app_mem(self, ID_arr):
        """generate randomly app memory allocation based on dataset information

        Args:
            ID_arr (ndarray): ID in a array ([OwnerID, AppID])

        Returns:
            int: randomly allocated memory
        """
        
        [owner_id, app_id] = ID_arr

            
        MemAve = self.mem_info.loc[(str(self.day_id), owner_id, app_id), "MemAve"]
        MemProb = self.mem_info.loc[(str(self.day_id), owner_id, app_id), "MemProb"]
        
        rand_mem_alloc = random.choices(MemAve, weights=MemProb, k=1)[0]
        
        if type(rand_mem_alloc) == dict:
            ipdb.set_trace()
        return int(rand_mem_alloc)
    
    def __gen_invoc_series(self, save=True):
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
                
                try: # skip rows without memory/duration
                    dur = self.__generate_func_dur(ID_arr=func_)
                    mem = self.__generate_app_mem(ID_arr=func_[:2])
                except KeyError:
                    inv_tmp = inv_tmp.drop(index=(func_[0], func_[1], func_[2]))
                    continue # we do not have info for this func               
                
                func_invc_series = inv_tmp.loc[(func_[0], func_[1], func_[2])].values
                for t, state in enumerate(func_invc_series):
                    if state:
                        dur = self.__generate_func_dur(ID_arr=func_)
                        if not dur:
                            dur = 0.01                        
                        func_start_time = max(0.01, t + 1 - dur)
                        exec_time = np.ceil(np.arange(func_start_time, t + 1, 1)).astype(int).astype(str) # the duration of this exec
                        if dur <= 1: # within 1min: num of invocation+1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), exec_time[0]] = state + 1
                        else:   # func has some duration: first time slot: num of invocation+1; following time slot: 1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), exec_time[0]] = state + 1
                            inv_tmp.loc[(func_[0], func_[1], func_[2]), (x for x in exec_time[1:])] = 1
        
        
        self.exe_raw = inv_tmp.sort_index()
        if save == True:
            output_path = os.path.join(self.data_path, "execution_series_{}.csv".format(self.day_id))
            inv_tmp.to_csv(output_path)



    def prepare(self):
        print("[P_{}] Getting execution series...".format(self.day_id))
        # exec_ = f"invocations_per_function_md.anon.d{i:02}.csv"
        # if exec_ in self.data_loader.inv_files:
        exec_ = "execution_series_{}.csv".format(self.day_id)
        if exec_ in self.data_loader.exec_files:
            print("[P_{}] Loading execution series from CSV...".format(self.day_id))
            self.exe_raw = pd.read_csv(os.path.join(self.data_path, exec_)).set_index(["HashOwner", "HashApp", "HashFunction"]).sort_index()
            # self.exe_raw[:] = self.exe_raw[:].values.astype(bool) # for raw dataset
        else:
            print("[P_{}] Generating series from dataset...".format(self.day_id))
            self.inv_raw = pd.read_csv(os.path.join(self.data_path, self.data_loader.inv_files[self.day_id-1]))
            self.__gen_invoc_series()
        print("[P_{}] Series getting SUCCESS!".format(self.day_id))
        
        
    def run_app(self, intv):
        cold_rate = []
        mem_rate = []

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

    def run_sys(self, arg_lst):
        app_exe_raw = self.exe_raw.reset_index().groupby(["HashOwner", "HashApp"]).max().sort_index().iloc[:, 1:]
        app_name_list = app_exe_raw.reset_index().iloc[:, :2].values
        app_mem_list = np.array([self.__generate_app_mem(app) for app in app_name_list])


        cold_rate_lst = []
        mem_rate_lst = []
        for idx, arg in enumerate(arg_lst):
            simSys = GreedysimSys(app_name_list, app_mem_list)
            simSys.total_mem = arg
            
            # simSys = FixIntervalsimSys(app_name_list)
            # simSys.keep_alive_interval = arg
            for i in trange(self.day_len):
                t = i + 1
                exec_now = app_exe_raw[str(t)].values
                if not simSys.update(exec_now):
                    print("System Memory Overflow!")
                    break
            cold_rate_lst.append(simSys.cal_cold_rate())
            mem_rate_lst.append(simSys.cal_mem_waste())
        
        return [cold_rate_lst, mem_rate_lst]