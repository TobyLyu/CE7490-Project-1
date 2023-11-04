import pandas as pd
import numpy as np
import os
from tqdm import tqdm, trange
import ipdb

class DataLoader():
    def __init__(self, path) -> None:
        self.data_path = path
        self.mem_raw = None
        self.dur_raw = None
        self.inv_files = []
        self.json_ = False
        self.exec_ = False
        self.exec_files = []
        self.max_day = 0
        self.day_id = 1
        self.data_info_col = ["Day", "HashOwner", "HashApp", "HashFunction", "MemAve", "MemProb", "DurAve", "DurProb"]
        self.data_info = pd.DataFrame({"Day":[],
                                       "HashOwner":[], 
                                       "HashApp":[], 
                                       "HashFunction":[],
                                       "MemAve":[],
                                       "MemProb":[],
                                       "DurAve":[],
                                       "DurProb":[]})
        
    def __get_files(self) -> list:
        """get files in data path

        Returns:
            [list]: return file names of data [mem[], dur[], inv[], exe[], json[]]
        """
        files_names = os.listdir(self.data_path)
        # json_name = [v for v in files_names if 'json' in v]
        mem_files = [file for file in files_names if file[0] ==  'a']
        dur_files = [file for file in files_names if file[0] ==  'f']
        inv_files = [file for file in files_names if file[0] ==  'i']
        exe_files = [file for file in files_names if file[0] ==  'e']
        json_files = [file for file in files_names if file[0] ==  'p']
        mem_files.sort()
        dur_files.sort()
        inv_files.sort()
        exe_files.sort()
        json_files.sort()
        # some date's file may be incompeleted
        self.max_day = min(mem_files[-1][-6:-4], dur_files[-1][-6:-4], inv_files[-1][-6:-4])
        self.max_day = int(self.max_day)
        
        # if len(json_name): 
        #     self.json_ = True 

        # if len(exe_files): 
        #     self.exec_ = True          
            
        # # DEBUG:
        # self.max_day = 1
        
        return [mem_files[:self.max_day], dur_files[:self.max_day], inv_files[:self.max_day], exe_files[:self.max_day], json_files[:self.max_day]]
            
            
    def __cal_distribute(self, percentile_in) -> list:
        """calculate probability of each average based on percentile
        due to the incompleted percentile data in the dataset, we only calculate
        probability of 25%, 50%, 75%, 99%
        
        In case there are no changes in these percentile, these percentiles are neglected.

        Args:
            percentile_in (ndarray): input of percentile size 1x5 (1%, 25%, 50%, 75%, 99%)

        Returns:
            list: list of average, and list of probability of each average, size [1x4 1x4]
        """
        
        pert_range = np.array([0.24, 0.25, 0.25, 0.24])
        pert_1 = percentile_in
        pert_2 =  np.delete(np.append(percentile_in, 0), 0)
        diff = (pert_2 - pert_1)[:4]
        if sum(diff) == 0: # filter out zero value data
            raise ValueError
        else: # percentile weighted probability
            non_zero_idx = np.where(diff!=0)[0]
            diff = diff[non_zero_idx]
            ave_out = percentile_in[1:][non_zero_idx]
            pert_range = pert_range[non_zero_idx]
            prob = np.divide(pert_range, diff) / sum(np.divide(pert_range, diff))
            
            return [ave_out, prob]
            
            
    def __gen_properties(self, i, save=True) -> None:
        """ get app/func name
            calculate app/func's mem/dur distribution
            save to file if necessary 
        
        Args:
            save (bool): whether to save the properties
            
        """

        # print("Generating function properties...")
        # filter out app/func names that have both mem and dur properties
        # for i in range(self.max_day):
        this_day = [[] for _ in range(len(self.dur_raw))]
        this_mem_raw = self.mem_raw.drop_duplicates(subset=["HashOwner", "HashApp"]).set_index(["HashOwner", "HashApp"]) # remove duplicate info
        # print("Processing Day{} data...".format(i+1))
        with tqdm(total=len(self.dur_raw)) as pbar:
            for dur_idx, dur_row in self.dur_raw.iterrows():
                pbar.update(1)
                try:
                    mem_row = this_mem_raw.loc[dur_row["HashOwner"], dur_row["HashApp"]]
                    
                    # calculate duration properties
                    try:      
                        [dur_ave, dur_prob] = self.__cal_distribute(
                                                dur_row[["percentile_Average_1",
                                                        "percentile_Average_25",
                                                        "percentile_Average_50",
                                                        "percentile_Average_75",
                                                        "percentile_Average_99"]].values)
                    except ValueError:
                        dur_ave = [dur_row["Average"]]
                        dur_prob = [1.0]
                        if dur_ave == 0:
                            continue
                    
                    # calculate memory properties
                    try:
                        [mem_ave, mem_prob] = self.__cal_distribute(
                                            mem_row[["AverageAllocatedMb_pct1",
                                                    "AverageAllocatedMb_pct25",
                                                    "AverageAllocatedMb_pct50",
                                                    "AverageAllocatedMb_pct75",
                                                    "AverageAllocatedMb_pct99"]].values)
                    except ValueError:
                        mem_ave = [mem_row["AverageAllocatedMb"]]
                        mem_prob = [1.0]
                        if mem_ave == 0:
                            continue
                    # save all good value to this_day
                    this_day[dur_idx] = (str(i), dur_row["HashOwner"], dur_row["HashApp"], dur_row["HashFunction"], mem_ave, mem_prob, dur_ave, dur_prob)
                    if type(dur_ave[0]) != int:
                        
                        print(dur_ave)
                except KeyError:
                    continue
            this_day = list(filter(None, this_day)) # filter out unused entry
            # full_data += this_day
        self.data_info = pd.DataFrame(this_day, columns=self.data_info_col).drop_duplicates(subset=self.data_info_col[:4]) # remove duplicate info
        
        # self.data_info = self.data_info.pivot_table(["MemAve", "MemProb", "DurAve", "DurProb"], index=["Day", "HashOwner", "HashApp", "HashFunction"], aggfunc="mean")
        
        if save == True:
            output_path = os.path.join(self.data_path, "properties_{}.json".format(i))
            self.data_info.to_json(output_path, orient='index')
            
        self.data_info = self.data_info.set_index(self.data_info_col[:4]).sort_index()
    

    
    def load_dataset(self, i) -> None:
        self.day_id = i
        """load all completed dataset in the folder
        """
        
        # self.i = i
        [mem_files, dur_files, inv_files, exec_files, json_files] = self.__get_files()
        # self.mem_raw = [ [] for _ in range(self.max_day)]
        # self.dur_raw = [ [] for _ in range(self.max_day)]
        # self.inv_raw = [ [] for _ in range(self.max_day)]
        self.inv_files = inv_files
        self.exec_files = exec_files
        
        print("[P_{}] Getting dataset properties...".format(i))
        # if self.json_:
        json_ = "properties_{}.json".format(i)
        if json_ in json_files:
            print("[P_{}] Loading properties from JSON...".format(i))
            json_path = os.path.join(self.data_path, json_)
            self.data_info = pd.read_json(json_path).T.set_index(self.data_info_col[:4]).sort_index()
            
            # print("Now we are loading the invocation data...")
            # for i in trange(self.max_day):
            #     self.inv_raw[i] = pd.read_csv(os.path.join(self.data_path, inv_files[i]))
        else:
            print("[P_{}] Generating properties from dataset...".format(i))
            # print("It can be slow...But no worries we will save the result after finishing!")
            # for i in trange(self.max_day):
            # print(mem_files, i)
            # print(os.path.join(self.data_path, mem_files[i-1]))
            self.mem_raw = pd.read_csv(os.path.join(self.data_path, mem_files[i-1]))
            self.dur_raw = pd.read_csv(os.path.join(self.data_path, dur_files[i-1]))
                # self.inv_raw[i] = pd.read_csv(os.path.join(self.data_path, inv_files[i]))
            self.__gen_properties(i)
            # print(self.data_info)
        print("[P_{}] Properties getting SUCCESS!".format(i))