import gc 
import re 
import sys  
import warnings 
import os 
import time  
from datetime import datetime 
import warnings   
import pandas as pd
import numpy as np

import hashlib
from collections import defaultdict, Counter

from common_utils.sequence_functions import list_diff_outer_join, lcs, filter_lcs
from common_utils.os_functions import *
from common_utils.df_functions import *
from common_utils.config_table import ConfigReader 
from common_utils.excel_functions import write_format_columns, refresh_excel_calculations, save_csv
from common_utils.regex_functions import *
from common_utils.decorator_functions import *

def replace_brackets2blank(string):
    for x in '()（）-_':
        string = string.replace(x,' ')
    return string 

@catch_and_print
def check_mapping_complete(df_worksheet, complete_header_df, original2cn_dict,file_tag):
    #检查是否遗漏了不是“无”的映射字段`
    c_required_columns = get_dict_unique_values(original2cn_dict)
    check_required_columns = set(c_required_columns) - set(df_worksheet.columns)
    check_required_columns = [x for x in check_required_columns \
                if 'fillbeforeconcat:' not in x.replace(' ','') or '合并前填充:' not in x.replace(' ','') ]

    if check_required_columns:
        warning_msg = 'Warning: Failed to find the the mapping for column ' \
                      + f"{','.join(check_required_columns)}" \
                      + f'of excel file tagging with "{file_tag}"'

@catch_and_print
def dtype_handle(complete_header_df, dtype_dict, output=False):
    if dtype_dict:
        #转换成读取格式,主要处理日期有时读取成float格式的问题
        for dtype_column, dtype_type in dtype_dict.items():
            #处理输入数据的类型 日期类型--不包含时分秒 normalize().date 时间类型-包含时分秒-to_datetime, 和其他pandas支持的数据类型
            dtype_type_temp = dtype_type.strip().lower()
            if dtype_column not in list(complete_header_df.columns):
                enter_exit(f'Cannot find "{dtype_column}" in concatnated_data!')
            if dtype_type_temp == '' or dtype_type_temp == '默认' or dtype_type_temp == 'default':
                pass 
            elif 'date' in dtype_type_temp or 'time' in dtype_type_temp:
                complete_header_df = normalize_dates(complete_header_df, dtype_column)
                if 'date' in dtype_type_temp:  #如果是只要日期，把它转成没有时分秒的格式
                    complete_header_df[dtype_column] = complete_header_df[dtype_column].dt.normalize()
                    #time 不用处理，前面已经normalize_date转成时间格式了
            else:
                #如果是日期，需要转成固定的2020/09/09的格式 提供给技术的java补录工具读取
                if output and dtype_type_temp == 'str' and np.issubdtype(complete_header_df[dtype_column].dtype, np.datetime64):
                    complete_header_df[dtype_column] = complete_header_df[dtype_column].dt.strftime('%Y/%m/%d')
                else:
                    #防止转成空值的float 类型变成 nan, 统一fillna
                    try:  #防止有空值concat后全是float转str 其他非空的int也会带个小数点
                        complete_header_df[dtype_column] = complete_header_df[dtype_column].fillna('')\
                        .astype(dtype_type).apply(lambda x: x.rstrip('.0') if type(x) == str and len(x) >= 2 and x[-2:] == '.0' else x )
                    except :
                        enter_exit(f'Failed to convert column "{dtype_column}" to dtype "{dtype_type}"')

    return complete_header_df

@catch_and_print
def get_dict_unique_values( dictionary):
    """提取一个字典里面的所有非重复的values作为key构建新的字典，新字典values统一为1"""
    record_set = set()
    for k, v in dictionary.items():
        record_set.add(v)
    return record_set

@catch_and_print
def combine_multi_plus(df_worksheet,original2cn_dict):
    #加入系统表格处理,联系方式合并Tel,Email,Facebook
    multi_plus_list =  [x.lower() for x in original2cn_dict.keys() if '+' in x ]
    if multi_plus_list:
        for m in multi_plus_list:
            m_split_list = m.split('+')
            find_lack_columns(df_worksheet, m_split_list,'combine_multi_plus')
            df_worksheet[m] = df_worksheet[m_split_list[0]] 
            for m_s in m_split_list[1:]:
                df_worksheet[m] = df_worksheet[m].fillna('').astype(str).replace('/','')\
                                  + ' ' + df_worksheet[m_s].fillna('').astype(str).replace('/','')
            df_worksheet[m] = df_worksheet[m].str.strip()

    return df_worksheet


@catch_and_print
def get_filter_condition_match(join_table_df, filter_condition):
    #匹配的字段条件和模糊匹配(字段标准化)不同
      
    if filter_condition != '' :
        join_table_df = df_query(join_table_df,filter_condition)
        
    return join_table_df 

@catch_and_print
def is_df_column(string):
    return '[' == string[0] and ']' == string[-1] 

@catch_and_print
def get_filter_condition_standardize_tag(filter_condition):
    #仅限于模糊匹配，里面可能会填入 [国家/地区 == 国家/地区] 的形式
    #代表是否填入的两个表的相同或不同字段
    filter_condition_2_columns_tag = False 
    filter_left_column, filter_right_column = '', ''
    if is_df_column(filter_condition) : 
        filter_condition_2_columns_tag = True 

        filter_condition = filter_condition.lstrip('[').rstrip(']')
        filter_condition_list = [ x.strip() for x in filter_condition.split('==') ]
        if len(filter_condition_list) < 2 :
            enter_exit(f'Standardization:failed to compile filter condition:{filter_condition}!') 
        else:
            filter_left_column = filter_condition_list[0]
            filter_right_column = filter_condition_list[1] 

    return filter_condition_2_columns_tag, filter_left_column, filter_right_column

@catch_and_print
def process_sort_order(join_table_df, join_columns, string):
    if string != '' :
        order_columns = string.split('\n')[0].split(':')
        reverse_order = string.split('\n')[1].split(':')

        reverse_order = [ True if x.lower() == 'desc' else False for x in reverse_order ]

        order_columns_new = [ ]
        reverse_ordere_new = [ ]
        for sort, order in zip(order_columns, reverse_order) :
            #join_columns是用来确保 匹配前的on字段也有参与排序，并且不会和人工填入的字段出现重复
            if sort in join_table_df.columns:
                if sort not in join_columns :
                    order_columns_new.append(sort)
                    reverse_ordere_new.append(order)
            else:
                print(f'Match table cannot find column "{sort}"')

        order_columns_new = join_columns + order_columns_new
        reverse_ordere_new = [ True for x in range(len(join_columns)) ]  + reverse_ordere_new

        join_table_df = join_table_df.sort_values(by=order_columns_new, ascending=reverse_ordere_new )

        join_table_df = join_table_df.fillna('').astype(str)
        
    return join_table_df

@catch_and_print
def calc_similarity(not_standard_str, new_standard_str,calc_func ):

    #先把括号换成空格
    not_standard_str = replace_brackets2blank(not_standard_str)
    new_standard_str = replace_brackets2blank(new_standard_str)

    str_similarity = 0 

    match_list = calc_func(not_standard_str, new_standard_str.strip())
    #如果这里就发现一个都没有发现匹配到，直接填相似度为0
    if len(match_list) < 2:
        return str_similarity

    #未处理符号的连续字母命中最多的优先,'PD1945F/FF/CF/DF/EF'， 'PD1945BF/DF/FF_EX' PD1945F  应该优先选第一个，第二个只是长度短而已
    continues_match = 0 
    for i in range(len(not_standard_str)):
        try:
            if not_standard_str[i] == new_standard_str[i]:
                continues_match += 1 
            else:
                break
        except:
            break

    #解决上面有些因为内存而导致匹配到的字符长，但却匹配错的问题
    not_standard_str_no_space = ' '.join(not_standard_str.split(' ')[:-1])
    new_standard_str_no_space = ' '.join(new_standard_str.split(' ')[:-1])
    #用分母的方式，解决 Y15 优先 配到了 Y15S 而不是 Y15(4+64G)的问题
    match_list_no_space = calc_func(not_standard_str_no_space, new_standard_str_no_space )
    
    match_list_len = len(match_list)
    match_list_len_no_space = len(match_list_no_space) 


    try: #也门 -- 对应被拆开的 卡塔尔 也门 科威特 阿曼,第一个卡塔尔拆出来之后 分母为0 
        match_list_len_no_space_divide = len(not_standard_str_no_space) / len(new_standard_str_no_space)
    except ZeroDivisionError:
        match_list_len_no_space_divide = 0 

    str_similarity = match_list_len + continues_match + match_list_len_no_space + match_list_len_no_space_divide

    return str_similarity

@catch_and_print
def replace_by_dict(string, replace_dict={}):
    if replace_dict :    
        #先做替换
        for k,v in replace_dict.items():
            string, sub_num = re.subn(k, v, string, flags=re.I)
            #如果已经替换成功就停止，不往下替换
            if sub_num != 0 :
                break
    return string 

@catch_and_print
def check_similarity_number( not_standard_str, standard_dict,special_syn_list,
                             ignore_punctuation=True):

    #某些情况不符合模糊匹配条件默认加入的结果 
    not_match_default = [0, '', '']

    first_syn = [ x for x in special_syn_list if x != '' ][0]

    for standard_str, target_str in standard_dict.items():
        #创建一个专门用来匹配的新standard_str
        new_standard_str = standard_str.lower().strip()

        #和其他两个匹配方式有区别，忽略掉ignore_punctuation为了保证regex的结果
        if special_syn_list:
            syn_checking = check_syn_str_regex_number(not_standard_str,new_standard_str,special_syn_list)
            #如果检查两边两边的同步字符不同, 直接判断为不符合
            if not syn_checking:
                continue 
        #需要填入ram_rom 获取到的部分 才能对内存数字是否相同做到完美的判断, 不放入first_syn会导致 (y3 16G+128)可能会匹配到(3+16G)
        match_list = number_similarity(not_standard_str,new_standard_str, first_syn)

        #以下可以直接用是否返回match_list来判断
        if match_list :
            return [[99, new_standard_str, target_str]]

    return [ not_match_default ]

@catch_and_print  
def compare_number_part(string1, string2):
    not_standard_number = ''
    new_standard_number = ''

    counter = 0 
    for s in string1:
        if s.isdigit():
            counter += 1 
            not_standard_number += s 
        else:
            if counter >= 1 : 
                break 
    counter = 0 
    for s in string2:
        if s.isdigit():
            counter += 1 
            new_standard_number += s 
        else:
            if counter >= 1 :
                break

    return not_standard_number ==  new_standard_number

@catch_and_print
def check_similarity_simple( not_standard_str, standard_dict,special_syn_list,
                             ignore_punctuation=True):
    """
    1. 完全相等直接返回
    2. 需要两边同时有special_syn_list, 同时ignore_punctuation
    3. 相同的字符至少要有两个及以上
    4. 从遇到第一个数字开始连续的数字部分（第一个数字部分）必须相同, PD1945不能因为没有别的相似而配上PD1948
    """
    #not_standard_str不要填入空字符串防止运行累赘,在前面的函数做判断
    standard_similarity_list = []

    #某些情况不符合模糊匹配条件默认加入的结果 
    not_match_default = [0, '', '']

    for standard_str, target_str in standard_dict.items():
        #创建一个专门用来匹配的新standard_str
        new_standard_str = standard_str.lower().strip()
       #确认至少两个字符相同，并且如果出现数字，从左往右遇到第一个完整连续的数字串，必须完全相同
        check_number_part = compare_number_part(not_standard_str, new_standard_str)
        if not check_number_part:
            standard_similarity_list.append(not_match_default)
            continue  

        #如果完全相等直接,不往下面走, 直接返回
        if not_standard_str == new_standard_str:
            return [[99, standard_str, target_str]]

        if ignore_punctuation == True:
            new_standard_str = replace_punctuations(new_standard_str)

        if special_syn_list:
            syn_checking = check_syn_str_regex(not_standard_str,new_standard_str,special_syn_list)
            #如果检查两边两边的同步字符不同, 直接返回判断为
            if not syn_checking:
                standard_similarity_list.append(not_match_default)
                continue

        str_similarity = calc_similarity(not_standard_str, new_standard_str, lcs )

        #上面一截 除了lcs函数不同，其他都一样
        if str_similarity >=2  :
            standard_similarity_list.append([str_similarity, standard_str, target_str])
        else:
            standard_similarity_list.append(not_match_default)

    standard_similarity_list = sorted(standard_similarity_list, key=lambda x:x[0],reverse =True)

    return standard_similarity_list

@catch_and_print
def check_similarity_strict( not_standard_str, standard_dict,special_syn_list,
                             replace_dict={}, ignore_punctuation=True):
    #not_standard_str不要填入空字符串防止运行累赘
     #机型匹配模式，用的是filter_lcs,因为要确保前面2个或3个字母都相同
    standard_similarity_list = []

    #某些情况不符合模糊匹配条件默认加入的结果 
    not_match_default = [0, '', '']

    for standard_str, target_str in standard_dict.items():
        #创建一个专门用来匹配的新standard_str
        new_standard_str = standard_str.lower().strip()

        #如果完全相等直接,不往下面走, 直接返回
        if not_standard_str == new_standard_str:
            return [[99, standard_str, target_str]]

        if ignore_punctuation == True:
            new_standard_str = replace_punctuations(new_standard_str)

        if special_syn_list:
            syn_checking = check_syn_str_regex(not_standard_str,new_standard_str,special_syn_list)
            #如果检查两边两边的同步字符不同, 直接返回判断为
            if not syn_checking:
                standard_similarity_list.append(not_match_default)
                continue

        str_similarity = calc_similarity(not_standard_str, new_standard_str,filter_lcs )

        if str_similarity >= 2 : 
            # 有3个和3个以上的字符，前3个全是字母确保3个字母都相同，前3个字符有数字和英文，确保英文和数字都相同
            # 前两/三位字母相同,方便后面elif写条件
            first2_letter_equal = not_standard_str[:2] == new_standard_str[:3].split(' ')[0][:2]
            first3_letter_equal = not_standard_str[:3] == new_standard_str[:3].split(' ')[0][:3]

            #前3位字符串包含2个数字的情况 必须满足前3个字母相同
            first3_letter_2num_not_standard = re.search('[0-9]{2,}',not_standard_str[:3]) != None
            first3_letter_2num_standard =  re.search('[0-9]{2,}',new_standard_str[:3]) != None

            first3_letter_less2num_not_standard = not first3_letter_2num_not_standard
            first3_letter_less2num_standard = not first3_letter_2num_standard

            #1. 如果输入机型只有2个字符，空格分割开的字符必须完全相同: 因为V1不能配成V11
            if len(not_standard_str) == 2 :
                new_standard_str = new_standard_str.split(' ')[0]

                if not_standard_str == new_standard_str:
                    standard_similarity_list.append([str_similarity, standard_str, target_str])
                else:
                    standard_similarity_list.append(not_match_default)

            elif   (len(not_standard_str) >= 3 and first2_letter_equal) and \
             (     #输入字符 前3位有2个数字 + 标准字符 前3位也有2个数字 -- > 前3个必须要相等
                   (first3_letter_2num_not_standard and first3_letter_2num_standard and first3_letter_equal) \
                   #输入字符 前3位没有2个数字 + 标准字符 前3位也没有2个数字 -- > 前2个字符必须要相等
                or (first3_letter_less2num_not_standard and first3_letter_less2num_standard and first2_letter_equal)
             ):
                standard_similarity_list.append([str_similarity, standard_str,target_str])
            else :
                standard_similarity_list.append(not_match_default)
        else:
            standard_similarity_list.append(not_match_default)

    standard_similarity_list = sorted(standard_similarity_list, key=lambda x:x[0],reverse =True)

    return standard_similarity_list

@catch_and_print
def standardize_by_similarity( not_standard_str, standard_dict, special_syn_list,
                               replace_dict={},  mode ='filter_lcs',ignore_punctuation=True):
    #mode, simple_lcs --用来匹配简单的国家 or filter_lcs -- 用来匹配机型
    #替换干扰符号和干扰词
    #处理中文国家缩写和完整国家名称无法匹配到的情况

    #前面已经做过去重
    not_standard_str = replace_by_dict(not_standard_str, replace_dict)

    not_standard_str = not_standard_str.lower().strip()

    standard_dict = dict(sorted(standard_dict.items(), key=lambda x:len(x[0]), reverse= False))

    if mode == 'simple_lcs':
        standard_similarity_list = check_similarity_simple(not_standard_str, standard_dict,special_syn_list,
                                                            ignore_punctuation)
    elif mode == 'number_similarity':
        standard_similarity_list = check_similarity_number(not_standard_str, standard_dict,special_syn_list,
                                                            ignore_punctuation)
    else:
        standard_similarity_list = check_similarity_strict(not_standard_str, standard_dict,special_syn_list,
                                                            ignore_punctuation)

    return standard_similarity_list

@catch_and_print
def standardize_column_func( not_standard_str, standard_dict, special_syn_list,
                             replace_dict={}, mode ='filter_lcs', ignore_punctuation=True):

    standard_similarity_list = standardize_by_similarity(not_standard_str, standard_dict, special_syn_list,
                                                         replace_dict, mode,ignore_punctuation)

    #确保输入的不为空
    if not_standard_str != '' and not_standard_str ==  not_standard_str :
        if standard_similarity_list and standard_similarity_list[0][0] > 0 :
            return standard_similarity_list[0][2]
        else:
            return ''
    
    return ''

@catch_and_print
def dropping_not_mapping(df_worksheet, original2cn_dict, target_cn_columns):
    mapping_column_list = [ x[1] for x in  original2cn_dict.items()]
    #如果现有的字段和需要的target字段有重复，但其实并不想映射进目的字段（因为可能数据是不同含义），先要删掉原始表的重复字段
    for d in df_worksheet.columns: 
        if d in target_cn_columns and d not in mapping_column_list:
            df_worksheet = df_worksheet.drop(d, axis=1) 
    return df_worksheet

@catch_and_print
def read_csv_data(table_path):
    df_worksheet = pd.DataFrame([])
    for encoding in ['utf-8','gbk','gb2312'] :
        try:
            df_worksheet = pd.read_csv(table_path, encoding=encoding )
            break
        except:
            print('')
            print(f'***** Failed to read file:{table_path} with encoding: {encoding} ***** ')
            print('')
            continue
    if df_worksheet.empty:
        print(f'***** Failed to read file:{table_path} *****')
    return df_worksheet

@catch_and_print
def read_xls_special(table_path):
    df_worksheet = pd.DataFrame([])
    try:
        df_worksheet = pd.read_html(table_path,header=0)[0]
    except:
        print('')
        print(f'***** Failed to read file:{table_path} ***** ')
        print('')

    return df_worksheet 

@catch_and_print
def get_min_max_date(result_df ):
    min_max_date_range = ''
    date_time_df = result_df.select_dtypes(include=['datetime'])

    if not date_time_df.empty:
        datetime_series = date_time_df.iloc[:,0]
        #报错的情况：所有值都是nan  ['NaT' 'NaT' 'NaT' ... 'NaT' 'NaT' 'NaT']
        try:
            min_datetime = datetime.datetime.strftime(datetime_series.min(), '%Y.%m.%d')
            max_datetime = datetime.datetime.strftime(datetime_series.max(), '%Y.%m.%d')
            min_max_date_range = f'{min_datetime}-{max_datetime}'
        except:
            pass
    return min_max_date_range

@catch_and_print
def drop_duplicated_columns_before_rename(df_worksheet, original2cn_dict):
    original_columns = list(df_worksheet.columns)

    duplicated_columns = [ ]
    for k, v in original2cn_dict.items():
        if v in original_columns:
            duplicated_columns.append(v)

    df_worksheet = df_worksheet.drop(duplicated_columns,axis=1)
    return df_worksheet

@catch_and_print
def refresh_configs(config_table_path_list):
    #读取配置前，是否刷新一遍文档内的公式并保存，注意这里不能打开配置文件，否则会无法保存成功
    for c in config_table_path_list:
        print(f'Refreshing excel:{c}')
        refresh_excel_calculations(c)

@catch_and_print
def get_standard_mode(standardize_mode):
    if 'number' in standardize_mode.lower():
        mode = 'number_similarity'
    elif 'simple' in standardize_mode.lower():
        mode = 'simple_lcs'
    else:
        mode = 'filter_lcs'
    return mode 

@catch_and_print
def get_partial_not_match(complete_header_df_notna, seq,source_column, 
                            standard_table_name, standard_column, 
                           filter_condition, target_column_edit, filter_left_column):

    column_order = ['Standization No.','Source_column', 'Standard_column', 'Content needs to standardized', 'Filter condition','Filter content']

    stand_column = '--'.join([standard_table_name,standard_column])

    #获取模糊匹配(标准化)失败的部分
    find_lack_columns(complete_header_df_notna, [target_column_edit, source_column])

    temp_columns = [source_column ]
    temp_headers = ['Content needs to standardized']
    if filter_left_column != '':
        temp_columns.append(filter_left_column)
        temp_headers.append('Filter content')

    partial_match_not_match_df = complete_header_df_notna\
            .loc[(complete_header_df_notna[target_column_edit].isna()==True)\
            |(complete_header_df_notna[target_column_edit]==''), temp_columns ] 

    if not partial_match_not_match_df.empty:
        #获取到的原始列内容
        partial_match_not_match_df.columns = temp_headers

        partial_match_not_match_df['Source_column']  = source_column
        partial_match_not_match_df['Standard_column'] = stand_column
        partial_match_not_match_df['Filter condition'] = filter_condition

        partial_match_not_match_df['Standization No.'] = seq

    partial_match_not_match_df = func_loc(partial_match_not_match_df, column_order) 

    partial_match_not_match_df = partial_match_not_match_df.drop_duplicates()

    return partial_match_not_match_df


@catch_and_print
def process_duplicates( df, drop_duplicates_condition):
    #字段去重
    if type(drop_duplicates_condition) == str and drop_duplicates_condition.strip() != '':

        drop_columns = drop_duplicates_condition.split(':')
        lack_column_list = find_lack_columns(df, drop_columns)
        df = df.drop_duplicates(subset = drop_columns)

    return df 

@catch_and_print
def replace_value_func(complete_header_df_new, replace_value_str, input_column, output_column):
    if replace_value_str != '':
        #统一变成字符型类
        replace_values = replace_value_str.split('\n')

        for replace_value in replace_values:

            #获取到需要替换的原始值和替换值
            replace_list = replace_value.rsplit(':',1)
            if len(replace_list) > 1 :
                #允许替换前的字段内容包含有冒号,从右开始拆分
                replace_original = replace_list[0]
                replace_result = replace_list[1]
            else:
                replace_original = ''
                replace_result = replace_list[1]

            #如果原始内容非空 并且 需要替换成单纯的其他字符串
            if replace_original != '' and 'regex(' != replace_original[:6].lower() :  #支持regex替换
                complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                        .str.replace(replace_original,replace_result,regex=False)

            elif replace_original != '' and 'regex(' == replace_original[:6].lower():
                replace_original = replace_original[6:-1]

                complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                        .str.replace(replace_original,replace_result,case=False, regex=True)
                complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                        .apply(lambda x: re.sub(replace_original,'', x, flags=re.I)).copy()
            else: #如果原始内容为空，即特别需要填充空值,且不能用series.str
                complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                        .fillna(value=replace_result).replace('',replace_result,regex=False)

    return complete_header_df_new


@catch_and_print
def fillna_value_func(complete_header_df, complete_header_df_new, fillna_value_str, output_column):

    if fillna_value_str != '' :
        #如果不是用别的字段做填充
        if not is_df_column(fillna_value_str):
            complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                                .fillna(value=fillna_value_str).replace('',fillna_value_str)
        #如果是属于表格的字段作为填充
        else :
            fillna_value_str_strip = fillna_value_str.lstrip('[').rstrip(']')
            #注意" 在前面整个字段可能已经被转成string格式，不能单纯通过fillna的方式填充空值,
            #而replace的方式又无法把其他字段值作为入参
            if fillna_value_str_strip in complete_header_df_new.columns :
                complete_header_df_new[output_column] = complete_header_df[output_column]\
                .replace('',float('nan')).fillna(value=complete_header_df_new[fillna_value_str_strip])
            else:
                try:  #想填充的可能是来自旧complete_header_df的字段
                    complete_header_df_new[fillna_value_str_strip] = complete_header_df[fillna_value_str_strip]

                    complete_header_df_new[output_column] = complete_header_df_new[output_column]\
                    .replace('',float('nan')).fillna(complete_header_df_new[fillna_value_str_strip])
                                        
                except KeyError:
                    enter_exit(f"Fillna error: Column '{fillna_value_str_strip}' doesn't exist!")

                complete_header_df_new = complete_header_df_new.drop(fillna_value_str_strip, axis=1)

    return complete_header_df_new


@catch_and_print
def sort_value_func(complete_header_df_new, sort_column_list,sort_column_order_list):
    #通过数字来排列数字
    order_list = [ ]
    for column,order in zip(sort_column_list,sort_column_order_list):
        order_list.append((column, order))

    order_list = sorted(order_list,key=lambda x: abs(x[1]))

    sort_column_list = [ x[0] for x in order_list ]
    ascending_list = [ True if x[1] >= 0 else False for x in order_list ]

    try:
        complete_header_df_new = complete_header_df_new.sort_values(by=sort_column_list,ascending=ascending_list)
    except :
        print('Fill&Order:Error when sorting the values')
        
    return complete_header_df_new


@catch_and_print
def check_mapping_duplicates(df, target_cn_columns, table_stem=''):
    #检查映射后是否出现重复字段,  返回需要保留的映射字段即可
    used_cols = [x for x in df.columns if x in target_cn_columns]

    # 检查有没有出现重复字段
    count_duplicate = Counter(used_cols).most_common(1)

    if count_duplicate and count_duplicate[0][1] > 1:
        enter_exit('Table "{}" has {} original fields can be mapped to target field "{}" that leads to duplication!'
                   .format(table_stem, count_duplicate[0][1], count_duplicate[0][0]))

    return df 

@catch_and_print
def get_replace_dict(string):
    try:
        replace_dict = [x.strip()for x in string.split('\n') if x.strip() != '']
        replace_dict = dict([[y.strip() for y in split_colon(x) ] for x in replace_dict])
    except:
        replace_dict = {}

    return replace_dict


@catch_and_print
def get_save_name(output_dir,config_file_dir, min_max_date_range):

    #获取config后面的一截用来做output文件的部分名称
    save_name = re.split('[_\-]',config_file_dir,maxsplit=1)[-1]
    
    if min_max_date_range != '':
        save_name = save_name + '-' +  min_max_date_range

    save_name = os.path.join(output_dir, save_name)

    return save_name

@catch_and_print
def get_save_sheet_name(config_table_name):

    sheet_name = re.sub(pattern='(.*)(\.xlsx|\.csv|.xls)', repl='\g<1>', string=config_table_name)
    sheet_name = re.split('[_\-]',sheet_name)[-1]

    #重复符号换成一个
    sheet_name = replace_multi_symbol(sheet_name,'-')

    sheet_name = strip_puntuations(sheet_name)

    if sheet_name == '':
        sheet_name = f'result{counter}'

    return  sheet_name


@catch_and_print
def save_results(save_name, result_df_list, sheet_name_list,
                 checking_result_list, checking_result_sheet_names, output_file_type):
    
    #确保output目录存在，否则创建output目录
    check_create_new_folder(f'{save_name}.xlsx')

        #CSV文档只针对单个输出结果，多个输出结果的话统一用excel保存
    if type(output_file_type) == str and output_file_type.lower() == 'csv':
        if len(result_df_list) == 1 :
            save_csv(result_df_list[0],f'{save_name}.csv' )
        else: #如果有多个结果，需要分开写入CSV
            for result_df,sheet_name in zip(result_df_list, sheet_name_list):
                save_csv(result_df,f'{save_name}({sheet_name}).csv' )

    elif len(result_df_list) == 1 and result_df_list[0].shape[0] > 100 * 10000 :
        print('The total number of rows exceeds 1 million, result will be saved in CSV format.')
        save_csv(result_df[0], f'{save_name}.csv')

    else:
        write_format_columns(f'{save_name}.xlsx',result_df_list, sheet_name_list, min_column_width=14)

    #写入检查数据
    if checking_result_list:
        path_stem = Path(f'{save_name}.xlsx').stem
        write_format_columns(os.path.join('.\\result_checking', f'Checking results-{path_stem}.xlsx'),
                             checking_result_list, checking_result_sheet_names, min_column_width=16)
            

def split_colon(string):
    lst = [ x for x in string.replace('：', ':').split(':')  if x != '' ]
    return lst

    
def process_join_table(join_table_df, join_columns, target_columns,filter_condition,sort_order, join_table_name = ''):
    #防止里面有重复字段
    join_table_df = remove_duplicate_columns(join_table_df)

    # 获取过滤条件后的关联表
    if filter_condition  != '':
        join_table_df = get_filter_condition_match(join_table_df, filter_condition)

    #定位join_table_df中所有需要的字段.不需要的字段删掉
    temp_used_cols = list(set(join_columns + target_columns))
    join_table_df = join_table_df.loc[:, temp_used_cols]

    # 将所有关联列转成string格式, 这里截取了固定的字段
    #需要对关联的所有字段清楚掉空值，否则如果匹配表填的有问题 会出现匹配错误
    join_table_df = join_table_df.apply(lambda x: 
        x.fillna('').astype(str).str.strip().replace('',float('NaN')) if x.name in join_columns else x )

    join_table_df = join_table_df.dropna(subset=join_columns, how='any')

    #剩下如果是空 直接返回空匹配表
    if not join_table_df.empty:

        if sort_order != '':
            join_table_df = process_sort_order(join_table_df, join_columns, sort_order)

        #如果不是用原始表做关联表的话，关联之前必须做去重，避免笛卡尔积现象
        if  join_table_name != '':
            join_table_df = join_table_df.drop_duplicates(subset=join_columns)

    return join_table_df

def process_match_complete_table(complete_header_df,source_columns, target_columns , join_columns, join_table_name):
    #转成str
    for s in source_columns:
        complete_header_df[s] = complete_header_df[s].fillna('').astype(str)
    #如果原始表格包含了需要获取到的字段，删掉
    for t in target_columns:
        if t not in source_columns and t in complete_header_df.columns:
            complete_header_df = complete_header_df.drop(target_columns,axis=1)

    return complete_header_df

def check_only_one_match_column(join_table_df, join_columns, target_columns ):
    only_one_match_column = False
    if len(join_columns) == 1 and len(target_columns) == 1 and join_columns[0] == target_columns[0]:
        only_one_match_column = True
        join_table_df['additional_temp'] = join_table_df[target_columns[0]]
        join_columns = join_columns + ['additional_temp']
    else:
        join_columns = join_columns + [target_columns]

    return join_table_df, only_one_match_column