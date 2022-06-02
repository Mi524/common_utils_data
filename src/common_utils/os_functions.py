import os 
import re  
import time 
import sys 
import xlrd 
import datetime 
import codecs 
import hashlib
from pathlib import Path
from glob import glob 
from collections import defaultdict


def get_folder_list(path='.\\',folder_name = ''):

    if folder_name != '':
        folder_list = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))==False\
                                 and folder_name.lower().strip() in f.lower() and f != '__pycache__' ]
    else:
        folder_list = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))==False\
                                 and f != '__pycache__' ]

    return folder_list

def choose_folder(path=r'.\\', folder_name=''):
    """
    #参考choose_file 函数，除了选的是文件夹，其他地方都一样
    提供选项选择读取处理哪个EXCEL文档sheet,处理哪个字段,默认处理main_body
    """
    folder_list = get_folder_list(path, folder_name)

    folder_str_list = [' ' + str(i) +'-' + x for i,x in enumerate(folder_list)]

    if len(folder_list) == 0:
        enter_exit('No folder found!')
    elif len(folder_list) == 1 :
        folder_choose_index = 0
    else:
        print('\n')
        print('\n'.join(folder_str_list))
        folder_choose_index = input('\n---Please Select Folder Number(Default:First Folder): ').strip()
        #打开文档
        if folder_choose_index == '':
            folder_choose_index = 0

    choose_index_tag = 0 
    while choose_index_tag < 1:
        try:
            folder_choose_index = int(folder_choose_index)
            folder_choose = folder_list[folder_choose_index]
            choose_index_tag += 1 
        except :
            print('Invalid folder number!')
            folder_choose_index = input('\n---Please Select Folder Number(Default:First Folder): ').strip()

    return os.path.join(path,folder_choose)

def get_most_upper_level_path(file_name):
    #获取最高一个层级的目录位置，用来做临时文件储存地点
    cwd_upper_dir = Path(os.getcwd())
    cwd_most_upper_level = str(cwd_upper_dir).split('\\',1)[0]

    temp_path = os.path.join(f'{cwd_most_upper_level}',f'\\{file_name}')

    if os.path.exists(temp_path):
        os.remove(temp_path)    

    return temp_path

def generate_md5(string):
    return hashlib.md5(string.encode('utf-8')).hexdigest()

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

def enter_exit(print_info=''):
    print(print_info)
    input('\nPress Enter to exit')
    sys.exit()

def partial_filename_path(path,partial_file_name,prefered_num=-1):
    """
    通过部分文件名定位到其中一份文件
    """
    file_pathes = [ x for x in os.listdir(path) if '~$' not in x ]
    file_pathes = [ x for x in file_pathes if partial_file_name in x]
    file_pathes = [os.path.join(path,x) for x in file_pathes]
    #排序
    file_pathes.sort()

    if file_pathes:
       #找到多个符合条件的文件优先取prefered_num的一个
        file_path = file_pathes[return_num]
    else:        
        file_path = None
    
    return file_path

def get_walk_files(path):
    """
    填入文件夹位置获取该目录和其子目录下的所有文件列表,获取的相对路径
    :param path 
    return a list of files inside the path
    """
    file_name_list = []
    for root,dirs,files in os.walk(path):
        for file  in files:
            file_name_list.append(file)
    file_name_list = [x for x in file_name_list if '~$' not in x]
    return file_name_list

def get_walk_abs_files(path):
    """
    填入文件夹位置获取该目录和其子目录下的所有文件列表,获取的绝对路径
    :param path 
    return a list of files inside the path
    """
    if not os.path.exists(path):
        enter_exit(f"Folder {path} doesn't exists !")
    file_name_list = []
    for root,dirs,files in os.walk(path):
        for file  in files:
            file_path = os.path.join(root,file)
            if os.path.isfile(file_path) and '~$' not in file :
                file_name_list.append(file_path)

    return file_name_list

def get_walk_folders(path):
    """
    填入文件夹路径获取该目录和其子目录下的所有文件夹列表
    :param path
    return a list of folders inside the path
    """
    folder_name_list = [ ]
    for root,dirs,files in os.walk(path):
        for d in dirs:
            folder_name_list.append(os.path.join(root,d))
    return folder_name_list

def check_require_files(path,require_name_list,regex=False):
    """
    检查某个路径是否包含必须的文档
    :param path:路径
    :param require_name_list: 要检查的文档/文件夹是否存在
    :param regex : 是否需要用re去匹配
    """
    path_files = get_walk_abs_files(path)
    #如果是精确匹配
    if regex == False:
        require_list = [ x for x in require_name_list if '~$' not in x and x in path_files]
    else: 
        require_dict = defaultdict(str)
        require_list = [ (x,re.search(x,y).group()) for x in require_name_list for y in path_files \
                         if re.search(x,y)!=None and '~$' not in  y ]
        for r in require_list:
            require_dict[r[0]] = r[1]
        require_list = require_dict.keys()
    #缺失的文件/文档
    file_lack = set(require_name_list) - set(require_list)
    if file_lack :
        print('请补充缺失的文档或文件夹:',','.join(file_lack))
        return False 
    else:
        return True

def get_require_files(path,require_file_list,regex=True,matched_part='xls',if_walk_path=True):
    """
    检查某个路径是否包含必须的文档
    :param path:路径
    :param require_file_list: 要检查的文档/文件夹是否存在
    :param regex : 是否需要用re去匹配
    :return : 如果不存在，返回空字典，如果文档存在 返回需要文档的对应绝对路径字典
    """
    if type(require_file_list) != list:
        require_file_list = [ require_file_list ]
        
    if if_walk_path == True:
        path_files = get_walk_abs_files(path)
    else: 
        path_files = [ os.path.join(path,x) for x in os.listdir(path) ]

    path_files.sort()

    require_dict = defaultdict(str)
    #如果是精确匹配
    if regex == False :
        require_list = [ (x,y) for x in require_file_list 
                               for y in path_files if '~$' not \
                               in  x and '.py' not in x and x == y.split('\\')[-1] ]  
    else: 
        if matched_part == None:
            require_list = [ (x,y) for x in require_file_list \
                                   for y in path_files if x.lower() in y.lower().split('\\')[-1] \
                                   and '~$' not in y ]
        else:
            require_list = [ (x,y) for x in require_file_list \
                                   for y in path_files if x.lower() in y.lower().split('\\')[-1] \
                                   and '~$' not in y and matched_part in y ]

    for r in require_list:
        if r[0] not in require_dict.keys():
            #记录返回的目标路径,已经有记录的不再更新
            require_dict[r[0]] = r[1]

    #对比哪些文档缺失
    require_list = require_dict.keys()
    #缺失的文件/文档
    file_lack = set(require_file_list) - set(require_list)

    if file_lack :
        enter_exit('Required files not found:{0}'.format(','.join(file_lack)))
        return {} 
    else:
        return require_dict

def get_require_file_list(path,require_file_list,regex=True,matched_part='xls',if_walk_path=True):
    """
    检查某个路径是否包含必须的文档
    :param path:路径
    :param require_file_list: 要检查的文档/文件夹是否存在
    :param regex : 是否需要用re去匹配
    :return : 如果不存在，返回空字典，如果文档存在 返回需要文档的对应绝对路径字典
    """
    if type(require_file_list) != list:
        require_file_list = [ require_file_list ]
        
    if if_walk_path == True:
        path_files = get_walk_abs_files(path)
    else: 
        path_files = [ os.path.join(path,x) for x in os.listdir(path) ]

    path_files.sort()

    require_dict = defaultdict(set)
    #如果是精确匹配
    if regex == False :
        require_list = [ (x,y) for x in require_file_list 
                               for y in path_files if '~$' not \
                               in  x and '.py' not in x and x == y.split('\\')[-1] ]  
    else: 
        if matched_part == None:
            require_list = [ (x,y) for x in require_file_list \
                                   for y in path_files if x.lower() in y.lower().split('\\')[-1] \
                                   and '~$' not in y ]
        else:
            require_list = [ (x,y) for x in require_file_list \
                                   for y in path_files if x.lower() in y.lower().split('\\')[-1] \
                                   and '~$' not in y and matched_part in y ]

    for r in require_list:
        #记录返回的目标路径,已经有记录的不再更新
        require_dict[r[0]].add(r[1])

    #对比哪些文档缺失
    require_list = require_dict.keys()

    #缺失的文件/文档
    file_lack = set(require_file_list) - set(require_list)

    if file_lack :
        enter_exit('Required files not found:{0}'.format(','.join(file_lack)))
        return False 
    else:
        return require_dict

def check_create_new_folder(dir_or_path):
    """
    检查是否存在某个文件夹，或者输入的文件所在的目录是否存在，如果不存在话 就生成新的文件夹
    """
    path = Path(dir_or_path)
    if not path.exists():
        path.mkdir(mode=0o777,parents=True,exist_ok=True)
    #如果传入的是一个文档，需要删掉新建的以文档命名的文件夹
        if not path.is_file() :
            path.rmdir()
    return dir_or_path

def choose_file(path=r'.\\'):
    """
    提供选项选择读取处理哪个EXCEL文档sheet,处理哪个字段,默认处理main_body
    """
    file_list = [ x for x in os.listdir(path) if '~$' not in x and '.xls' in x ]
    file_str_list = [' ' + str(i) +'-' + x for i,x in enumerate(file_list)]

    if len(file_list) == 0:
        enter_exit('Folder is empty')
    elif len(file_list) == 1 :
        file_choose_index = 0
    else:
        print('\n')
        print('\n'.join(file_str_list))
        file_choose_index = input('\n---Please Select File Number(Default:First File): ').strip()
        #打开文档
        if file_choose_index == '':
            file_choose_index = 0

    choose_index_tag = 0 
    while choose_index_tag < 1:
        try:
            file_choose_index = int(file_choose_index)
            file_choose = file_list[file_choose_index]
            choose_index_tag += 1 
        except :
            print('Invalid file number!')
            file_choose_index = input('\n---Please Select File Number(Default:First File): ').strip()

    return os.path.join(path,file_choose)

def choose_sheet_column(file_path):
    """
    打开文档选择sheeth和column,并且返回一个worksheet内容,方便后面使用
    """
    print('\nReading Sheet...')
    wb = xlrd.open_workbook(file_path, on_demand=True)
    sheet_names = wb.sheet_names()
    sheet_str_list = [ ' ' + str(i) +'-' + str(x).rstrip('0').rstrip('.') for i,x in enumerate(sheet_names) ]

    if len(sheet_names) == 1 :
        sheet_choose_index = 0
    else:
        print('\n'.join(sheet_str_list))
        sheet_choose_index = input('\n---Please input Sheet Number and press Enter(Default:First sheet): ').strip()
        #打开文档
        if sheet_choose_index == '':
            sheet_choose_index = 0

    choose_index_tag = 0 
    while choose_index_tag < 1:
        try:
            sheet_choose_index = int(sheet_choose_index)
            sheet_choose = sheet_names[sheet_choose_index]
            choose_index_tag += 1 
        except :
            print('Invalid Sheet number!')
            sheet_choose_index = input('\n---Please input Sheet Number and press Enter(Default:First sheet): ').strip()

    #打开工作簿 读取列
    ws = wb.sheet_by_name(sheet_choose)
    columns = [x.value for x in ws.row(0)]

    column_str_list = [ ' ' + str(i) +'-' + str(x).rstrip('0').rstrip('.') for i,x in enumerate(columns)]

    if len(columns) == 1 :
        column_choose_index = 0
    else:
        print('\n')
        print('\n'.join(column_str_list))
        column_choose_index = input('\n---Please input the Column Number that needs highlighted（Default:main_body）:').strip()
        #打开文档
        if column_choose_index == '':
            try:
                column_choose_index = columns.index('main_body')
            except ValueError:
                column_choose_index = 0

    column_choose_index = int(column_choose_index)
    column_choose = columns[column_choose_index]    
    #返回的是xlrd的wb
    return wb,ws,column_choose
