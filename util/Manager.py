import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import copy
import os
import ipdb

class simFunction():
    def __init__(self, ID, times) -> None:
        self.ID = ID
        self.times = times

class simApp():
    def __init__(self, ID) -> None:
        """_summary_

        Args:
            ID (str): app ID
        """
            
        self.ID = ID
        self.func_dict = dict()
        self.mem_allocated = 0
        self.idle_time = 0.0
        self.state = True # idle = False vs occupied = True
    
    def update(self):
        self.state = True
        if len(self.func_dict) == 0:
            self.idle_time += 1.0
            self.state = False
            return
        for func in self.func_dict:
            if func.dur_left < 0:
                self.func_dict.pop(func.ID)
                
    def step(self):
        for func in self.func_dict:
            func.step()
        
class simOwner():
    def __init__(self, ID) -> None:
        self.ID = ID
        self.app_dict = dict()
        
    def update(self) -> None: #key management strategy
        for app in self.app_dict:
            if app.state == False: # idle app
                self.app_dict.pop(app.ID)
            else:
                app.update()
                
    def step(self):
        for app in self.app_dict:
            app.step()
        
class FixIntervalsimOwner(simOwner):
    def __init__(self, ID, timeout) -> None:
        super().__init__(ID)
        self.timeout = timeout
        
    def update(self):
        for app in self.app_dict:
            if app.state == False and app.idle_time > self.timeout: # idle app
                self.app_dict.pop(app.ID)
            else:
                app.update()
                
class CachesimOwner(simOwner):
    def __init__(self, ID) -> None:
        super().__init__(ID)
        
    def update(self) -> None:
        # calculate priority for each app
        pass
        
class BaselineManager():
    def __init__(self, data_info, owner_dict) -> None:
        self.data_info = data_info
        self.system_clock = [0, 0] # [day, sec]
        self.owner_dict = owner_dict
        self.control_type = 'base'
    
    def __register_app(self, ID_arr, memory):
        [owner_id, app_id, func_id] = ID_arr
        if app_id not in self.owner_dict[owner_id].app_dict.keys():
            app = simApp(ID=app_id)
            app.mem_allocated = memory
            self.owner_dict[owner_id].app_dict[app_id] = app
            return True
        else:
            return False
    
    def __register_owner(self, ID):
        if ID not in self.owner_dict.keys(): # new owners app, register it
            self.owner_dict[ID] = simOwner(ID)
            return True
        else:
            return False
            
    
    def register_func(self, ID_arr, mem, times):
        """register a function

        Args:
            day (str): day of running
            ID_arr (ndarray): ID in a array ([OwnerID, AppID, FunctionID])
            times (int): how many times this function runs within 1 secs
        """
        
        
        
        [owner_id, app_id, func_id] = ID_arr
        result_owner = self.__register_owner(owner_id)
        result_app = self.__register_app(ID_arr, mem)
        
        # dur = self.__generate_func_dur(day=day, ID_arr=ID_arr)
        func = simFunction(func_id, times)
        
        if result_app == True: # which means cold start
            self.owner_dict[owner_id].app_dict[app_id].func_dict[func_id] = func
        else:
            if func_id in self.owner_dict[owner_id].app_dict[app_id].func_dict.keys():
                # which means previous invocation in queue
                self.owner_dict[owner_id].app_dict[app_id].func_dict[func_id].dur_left += func.dur_left
            else:
                self.owner_dict[owner_id].app_dict[app_id].func_dict[func_id] = func
                
class FixIntervalsimApp():
    def __init__(self, interval, func_series_in) -> None:
        # app properties
        self.keep_alive_interval = interval
        self.never_launch = True
        self.idle_timer = 0
        self.app_state = False # False: off, True: On
        self.app_status = False # False: idle, True: Busy
        # status counter
        self.cold_start_count = 0
        self.warm_start_count = 0
        self.cold_start_rate = 0.0
        # memory counter
        self.mem_record = []
        self.idle_time = 0
        self.busy_time = 0
        self.mem_waste_rate = 0.0
        # execute APP
        self.run(func_series_in)

    def run(self, func_series_in):
        app_series_out = [[] for _ in range(len(func_series_in))]
        self.mem_record = [[] for _ in range(len(func_series_in))]
        for t, func_state in enumerate(func_series_in):
            if not func_state:                              # function not in execution
                self.app_status = False
                if self.app_state:                          # app running but no function running
                    self.idle_time += 1
                    self.idle_timer += 1
                    if self.idle_timer > self.keep_alive_interval: # idle timeout, shutdown app
                        self.app_state = False
                    else:                                   # keep idle state
                        self.app_state = True
                else:                                       # app not running, do nothing
                    continue
            elif func_state == 2:                           # function invocation
                # ipdb.set_trace()
                
                self.app_status = True
                self.busy_time += 1
                self.idle_timer = 0
                if not self.app_state:                      # this is cold start
                    self.app_state = True
                    self.cold_start_count += 1
                else:                                       # this is warm start
                    self.warm_start_count += 1
            elif func_state == 1:                           # function still in execution
                # ipdb.set_trace()
                
                self.busy_time += 1
                self.idle_timer = 0
                
            app_series_out[t] = self.app_state
        # ipdb.set_trace()
        if (self.cold_start_count + self.warm_start_count): 
            self.never_launch = False
            self.cold_start_rate = self.cold_start_count / (self.cold_start_count + self.warm_start_count)
            self.mem_waste_rate = self.idle_time / (self.idle_time + self.busy_time)
            
class FixIntervalsimSys():
    def __init__(self, app_name_list) -> None:
        # app properties
        self.column_names = ["HashOwner",
                             "HashApp",
                             "state", 
                             "status", 
                             "memory", 
                             "idle_timer",
                             "never_launch",
                             "idle_time",
                             "busy_time", 
                             "cold_start_count", 
                             "warm_start_count"]
        self.sys_monitor = pd.DataFrame(columns=self.column_names)
        # self.sys_monitor[:] = 0
        self.app_name_list = app_name_list
        self.sys_monitor[self.column_names[:2]] = app_name_list
        self.sys_monitor = self.sys_monitor.fillna(0)
        
        self.sys_monitor = self.sys_monitor.set_index(self.column_names[:2])
        self.sys_monitor.loc[:, ("state", "status")] = False
        self.sys_monitor.loc[:, "never_launch"] = True
        self.keep_alive_interval = 0


    def update(self, exec_now):    
        # update never launch----------------------------------
        self.sys_monitor["never_launch"] = ~np.any([~self.sys_monitor["never_launch"].values, exec_now.astype(bool)], axis=0)

        # invocation func ----------------------------------
        invc_app = (exec_now > 1)
        # invc_app = (exec_now - 2) == 0
        # invc_app = (exec_now - 1) == 0
        self.sys_monitor.loc[invc_app, "status"] = True
        self.sys_monitor.loc[invc_app, "busy_time"] += 1
        self.sys_monitor.loc[invc_app, "idle_timer"] = 0
        
        # warm start invocation func need to be count first
        warm_app = np.all([self.sys_monitor["state"].values, self.sys_monitor["status"].values], axis=0) & invc_app
        self.sys_monitor.loc[warm_app, "warm_start_count"] += (exec_now[warm_app] - 1) # -1 is a flag added when generating the exec files
        
        # cold start invocation func count then (coz this will change state)
        cold_app = np.all([~self.sys_monitor["state"].values, self.sys_monitor["status"].values], axis=0) & invc_app
        self.sys_monitor.loc[cold_app, "state"] = True
        self.sys_monitor.loc[cold_app, "cold_start_count"] += 1
        self.sys_monitor.loc[cold_app, "warm_start_count"] += (exec_now[cold_app] - 2) # -2 is one for flag, one for cold start
        
        # stop idle timeout app --------------------------------
        timeout_apps = self.sys_monitor["idle_timer"].values >= self.keep_alive_interval
        self.sys_monitor.loc[timeout_apps, "state"] = False
        self.sys_monitor.loc[timeout_apps, "idle_timer"] = 0
        
        # for those func stop running---------------------------
        stop_funcs = ~exec_now.astype(bool)
        self.sys_monitor.loc[stop_funcs, "status"] = False
        
        # if app is running
        idle_apps = np.all([self.sys_monitor["state"].values, ~self.sys_monitor["status"].values], axis=0) & stop_funcs
        self.sys_monitor.loc[idle_apps, (["idle_time", "idle_timer"])] += 1 
        
        # running func ----------------------------------
        invc_app = (exec_now == 1) 
        self.sys_monitor.loc[invc_app, "busy_time"] += 1
        self.sys_monitor.loc[invc_app, "idle_timer"] = 0
        
        return True
        
    def cal_cold_rate(self):
        launch_func = ~self.sys_monitor["never_launch"].values
        cold_start_count = self.sys_monitor.loc[launch_func, "cold_start_count"].values
        warm_start_count = self.sys_monitor.loc[launch_func, "warm_start_count"].values
        return np.divide(cold_start_count, cold_start_count + warm_start_count).tolist()
    
    def cal_mem_waste(self):
        launch_func = ~self.sys_monitor["never_launch"].values
        idle_time = self.sys_monitor.loc[launch_func, "idle_time"].values
        busy_time = self.sys_monitor.loc[launch_func, "busy_time"].values
        return np.divide(idle_time, idle_time+busy_time).tolist()
    
class GreedysimSys(FixIntervalsimSys):
    def __init__(self, app_name_list, app_mem_list) -> None:
        super().__init__(app_name_list)
        self.total_mem = 0
        self.current_mem = 0
        self.system_clock = 0
        self.column_names = ["HashOwner",
                            "HashApp",
                            "state", 
                            "status", 
                            "memory", 
                            "idle_timer",
                            "never_launch",
                            "idle_time",
                            "busy_time", 
                            "cold_start_count", 
                            "warm_start_count",
                            "priority",
                            "clock",
                            "frequency"]
        self.sys_monitor["priority"] = 0
        self.sys_monitor["clock"] = 0
        self.sys_monitor["frequency"] = 0
        
        self.app_mem_list = app_mem_list     
        
    def update(self, exec_now):
        self.system_clock += 1
        # update never launch----------------------------------
        self.sys_monitor["never_launch"] = ~np.any([~self.sys_monitor["never_launch"].values, exec_now.astype(bool)], axis=0)
        
        # calculate newly need memory --------------------------------
        invc_app = (exec_now > 1)
        new_app = ~self.sys_monitor["state"].values & invc_app
        needed_mem = self.total_mem - self.current_mem - np.sum(self.app_mem_list[new_app])
        idle_app = self.sys_monitor["state"].values & self.sys_monitor["status"].values & ~invc_app # previous idle and current not to launch
        if needed_mem > np.sum(self.sys_monitor.loc[idle_app, "memory"].values): # system overflow
            return False
        
        # invocation func ----------------------------------
        # warm start
        warm_app = self.sys_monitor.loc[invc_app, "state"] & invc_app
        self.sys_monitor.loc[warm_app, "warm_start_count"] += (exec_now[warm_app] - 1)

        # cold start invocation func count then (coz this will change state)
        cold_app = ~self.sys_monitor["state"].values & invc_app
        self.sys_monitor.loc[cold_app, "state"] = True
        self.sys_monitor.loc[cold_app, "cold_start_count"] += 1
        self.sys_monitor.loc[cold_app, "warm_start_count"] += (exec_now[cold_app] - 2) # -2 is one for flag, one for cold start
        
        # update priority
        self.sys_monitor.loc[invc_app, "frequency"] += 1 # update frequency
        self.sys_monitor.loc[invc_app, "memory"] = self.app_mem_list[invc_app]
        # self.sys_monitor.loc[invc_app, "priority"] = self.system_clock + self.sys_monitor.loc[invc_app, "frequency"] # update priority
        self.sys_monitor.loc[invc_app, "priority"] = self.system_clock + np.divide(self.sys_monitor.loc[invc_app, "frequency"].values, self.app_mem_list[invc_app])# update priority
        
        # Engage management strategy -------------------------------
        for app in self.sys_monitor.loc[idle_app].reset_index().sort_values(by=['priority']):
            if needed_mem <= 0:
                break
            self.sys_monitor.loc[(app["HashOwner"].values, app["HashApp"].values), "state"] = False
            needed_mem -= self.sys_monitor.loc[(app["HashOwner"].values, app["HashApp"].values), "memory"].values
            self.sys_monitor.loc[(app["HashOwner"].values, app["HashApp"].values), "memory"] = 0 
            
        
        # update idle/busy time, system memory consumption
        # for those func stop running but app is running
        idle_apps = self.sys_monitor["state"].values & ~exec_now.astype(bool)
        self.sys_monitor.loc[idle_apps, (["idle_time", "idle_timer"])] += 1 
        
        # for thos func is running running and app is running
        invc_app = (exec_now == 1) 
        self.sys_monitor.loc[invc_app, "busy_time"] += 1
        self.sys_monitor.loc[invc_app, "idle_timer"] = 0

        self.current_mem = self.np.sum(self.sys_monitor["memory"].values)
        
        return True