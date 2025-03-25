import pickle
import os

def gnf(data,
        fi:str):
    """
    save result to file
    """
    with open(fi, "w") as fh:
        fh.write(data)
    print(f"{fi} had generated!")
    
def lp(pkl):
    """
    load pickle
    """
    with open(pkl, "rb") as fh:
        data = pickle.load(fh)
    return data

def cfile(infos):
    def check_file(f:str):
        if not os.path.exists(f):
            print(f"{f} is not exist, please check!")
            return False
        else:
            return True
    if type(infos) == list:
        if [check_file(f) for f in infos].count(True) == len(infos):
            return True
        else:
            return False
            
    else:
        return check_file(infos)


class DELFS:
    """delete file list"""
    
    def __init__(self,
                 file_list_in:list):
        
        import os
        import sys
        
        self.file_list_in = file_list_in
        self.pass_check = False
    
    def inner_func(self):
        if self.pass_check == None:
            sys.exit("File had been deleted, program shot down")
                
    def check(self):
        
        self.inner_func()
        
        suffix_list = []
        for f in self.file_list_in:
            suffix = f.split(".")[-1]
            suffix_list.append(suffix)
        
        if (len(list(set(suffix_list))) == 1) and (len(self.file_list_in) >= 2):
            print("All data is the same type, could be deleted!")
            self.pass_check = True
    
    def d(self):
        
        self.inner_func()
        
        Not_exit_files = []
        for f in self.file_list_in:
            try:
                os.remove(f)
                print(f"{f} had been delete!")
            except:
                Not_exit_files.append(f)
        
        self.file_list_in = Not_exit_files
        self.pass_check = None
        
        if self.file_list_in == []:
            print(f"\n>>>All files had been delete!<<<\n")
        else:
            print(f"below files are not exit or had been protected:")
            return self.file_list_in
        
def read_by_column_names(filein, select_column_names, sep="\t"):
    
    """
    Input:
    filein: tab-seperated file. The first line is column names.
    select_column_names: the list of names of column which you want to select.
    
    Output:
    A iterator.
    iter read each line of filein, and extract the values of select columns:
    
    For example:
    for column1, column2 in read_by_column_names(filein, ["column1", "column2"]):
        pass
    """
    
    def get_header(filein):
        header = ""
        with open(filein) as f:
            header = next(f)
        return header

    def get_header_index(filein, select_column_names, sep="\t"):
        select_indexs = []
        column_names = get_header(filein).rstrip("\n").split(sep)
        select_indexs = [column_names.index(column_name) for column_name in select_column_names]
        return select_indexs

    
    select_indexs = get_header_index(filein, select_column_names, sep)
    with open(filein) as f:
        next(f)
        for l in f:
            d = l.rstrip("\n").split(sep)
            yield([d[i] for i in select_indexs]) 
            
def get_lines(fi):
    """
    get lines very fast
    """
    buffer = 1024 * 1024 *8
    with open(fi, "r") as f:
        buf_gen = takewhile(lambda x: x, (f.read(buffer) for _ in repeat(None)))
        return sum(buf.count('\n') for buf in buf_gen)

def check_empty(file_path):
    size = os.path.getsize(file_path)
    if size == 0:
        print(f"{file_path} is empty.")
        return False
    else:
        print(f"{file_path} is not empty.")
        return True
