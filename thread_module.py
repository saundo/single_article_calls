# -*- coding: utf-8 -*-
"""
standard threading module
only inputs are function, timeframe, and temporary storage folder
"""

import pickle
import pandas as pd
import os
from queue import Queue
from threading import Thread

class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            func, start, end, dump_dir = self.queue.get()
            pump_and_dump(func, start, end, dump_dir)
            self.queue.task_done()

def run_thread(func, timeframe, dump_dir):
    """func - the API call to run 
    timeframe - needs to be a tuple of start, end; 
    dump_dir - where to temp store the data
    """
    
    #purge any files that might exist in the temp directory
    os.chdir(dump_dir)
    file_list = os.listdir()
    file_list = [i for i in file_list if 'threadtemp' in i]
    if len(file_list) > 0:
        for file in file_list:
            os.remove(file)
        
    queue = Queue()

    for x in range(8):
        worker = DownloadWorker(queue)
        worker.daemon = True
        worker.start()
    
    for start, end in timeframe:
        queue.put((func, start, end, dump_dir))

    queue.join()

def pump_and_dump(func, start, end, dump_dir):
    """makes a KEEN API call, and then saves the file to the given dump_dir
    """
    
    data = func(start, end)
    
    dump_dir = dump_dir
    ref = start[:16] + '--' + end[:16] + '--' + 'threadtemp'
    file = dump_dir +'/' + ref + '.pickle'

    with open(file, 'wb') as f:
        pickle.dump(data, f)

def read_data(dump_dir):
    """used to collect the files that are dumped by the threading
    compiles all the files together in a single, ungrouped data frame
    """
    
    os.chdir(dump_dir)
    file_list = os.listdir()
    file_list = [i for i in file_list if 'threadtemp' in i]
    
    storage = []
    for file in file_list:
        with open(file, 'rb') as f:
            x1 = pickle.load(f)
        df = pd.DataFrame(x1)
        storage.append(df)
        os.remove(file)
        
    return pd.concat(storage)


