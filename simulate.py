import numpy as np 
import pandas as pd
import pdb
import matplotlib.pyplot as plt
from util.DataLoader import DataLoader
from util.Simulator import FaasSimulator

loader = DataLoader(path="dataset")
loader.load_dataset()

simulator = FaasSimulator(loader)

simulator.prepare()
simulator.run()