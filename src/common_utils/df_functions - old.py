import pandas as pd 
from collections import defaultdict 
import datetime 
from xlrd import xldate_as_datetime
import os 
import sys
import json
from openpyxl import load_workbook
from common_utils.os_functions import last_day_of_month,enter_exit, generate_md5
from common_utils.regex_functions import replace_re_special, strQ2B
from common_utils.nlp_functions import get_keyword_dict, get_word_freq_dict, convert_key2list
import gc 
import re 
import warnings 
import traceback 
import logging 
from pandas.errors import OutOfBoundsDatetime
import swifter

warnings.filterwarnings('ignore')


def read_data_file(file_path):
    df = pd.DataFrame([])
    #读取数据文件，只读取第一个sheet
    if '.csv' == file_path[-4:]:
        df = pd.read_csv(file_path)
    elif '.xlsx' == file_path[-5:]:
        df = pd.read_excel(file_path)
    else:
        try:
            df = pd.read_excel(file_path)
        except:
            df = pd.read_html(file_path,header=0)
    return df 


#打开EXCEL文档前，激活一遍所有的sheet 防止包含有公式的单元格 读取到的数据不是最新的，比如=today()不刷新一遍，
#pandas读取到的永远是EXCEL文档保存的当天
def generate_complete_index(df, group_columns):
    #通过需要做成索引的字段，补充出一份完整的索引
    number_column = ''

    original_group_columns = group_columns[:]
    #获取到数字列的最大即可，文字列 不用交叉生成新数据
    column_min = 0 
    column_max = 0 
    num_unique_values = [ ]

    total_df_list = [ ]
    for c in group_columns:
        row_list = [ ]
        try: #是否是数字列， 如果是获取最大最小值, 通常不会拿float当成索引
            df[c] = df[c].astype(float)
            c_min = int(df[c].fillna(0).min(skipna=True))
            c_max = int(df[c].fillna(0).max(skipna=True))
            if c_max > column_max:
                column_max = c_max
                number_column = c
            if c_min < column_min:
                column_min = c_min

            if column_max <= 13: #临时限定补充的部分不超过15，通常计算不会超过这个，否则计算的数据量太大
                num_unique_values = [ x for x in range(column_min, column_max + 1)]
            else:
                num_unique_values = [ x for x in range(1,13) ]
        except: #如果不是数字列，获取所有唯一值
            pass 

    #如果存在数字列,生成完整的df 
    if num_unique_values:
        group_columns.remove(number_column)
        df_temp = df.loc[:,group_columns]
        df_temp = df_temp.drop_duplicates()

        for i in num_unique_values:
            df_temp[number_column] = i
            total_df_list.append(df_temp.copy())

        df_temp_total = pd.concat(total_df_list,axis=0,ignore_index=True)
    else:
        df_temp_total = df.loc[:,group_columns]
        df_temp_total = df_temp_total.drop_duplicates()

    df_temp_total = df_temp_total.set_index(original_group_columns)

    return df_temp_total

#创建一个完整的主键
def set_index_drop_all(df, index_columns):
    df_copy = df.copy()
    #指定特定的索引，清空所有字段数据（为了可能的有过滤的运算，做左关联的完整匹配）
    drop_columns = set(df_copy.columns) - set(index_columns)
    df_copy = df_copy.drop_duplicates(subset=list(index_columns))
    df_copy = df_copy.set_index(list(index_columns))
    df_copy['temp'] = True
    df_copy = df_copy.drop(drop_columns,axis=1)

    return df_copy

def df_query(df, condition):
    try:
        df = df.query(condition,engine='python')
    except:
        write_format_columns('Filter condition-Result when error.xlsx',df,'content')
        enter_exit(f'Unable to compile the following filter condition：\n"{condition}"')
    return df 


def find_lack_columns(df, require_columns, error_func=''):

    lack_column_list = [ ]
    for r in require_columns:
        if r not in df.columns:
            lack_column_list.append(r)

    if lack_column_list:
        error_msg = ','.join(lack_column_list)
        enter_exit(f"{error_func} Error - Missing columns:{error_msg}")

    return lack_column_list


def column_gen_md5(df,unique_id_columns,target_column='MD5'):
    #几列合并 sum(多个字段) 
    df[target_column] = df.loc[:,unique_id_columns].fillna('').astype(str).sum(1).swifter.apply(lambda x : generate_md5(x))
    return df 

def fillna_with_dict(df,fillna_dict):
    for k,  v in fillna_dict.items():
        if k in df.columns:
            df[k] = df[k].fillna(value=v) 
        else:
            df[k] = v 
    return df


def df_fillna_str(df):
    df = df.fillna(value='').astype(str).swifter.apply(lambda x: x.str.strip())
    return df  

#定位目标字段
def func_loc(df,used_cols):

    new_used_cols = [ ]
    #防止重复定位同一个字段
    seen_column = [ ]
    for u in used_cols:
        if u not in seen_column:
            new_used_cols.append(u)

        if u not in df.columns:
            df[u] = ''

    df = df.loc[:,new_used_cols]

    return df

def merge_case_insensitive(df_1,df_2,how, on):
    #忽略大小写和全角半角符号的匹配
    temp_list = [ ]
    for i, o in enumerate(on) :
        temp_name = f'lower_case_{i+1}' 
        df_1[temp_name] = ''
        df_2[temp_name] = ''
        df_1[temp_name] = df_1[o].swifter.apply(lambda x: strQ2B(x.lower().strip()) if type(x) == str else x )
        df_2[temp_name] = df_2[o].swifter.apply(lambda x: strQ2B(x.lower().strip()) if type(x) == str else x )
        temp_list.append(temp_name)

    if how == 'right':
        df_1 = df_1.drop(on,axis=1)
    else:
        df_2 = df_2.drop(on,axis=1)

    merged_df = pd.merge(df_1, df_2,how=how, on = temp_list )

    merged_df = merged_df.drop(temp_list, axis=1)

    return merged_df


def fill_header_period(df,fill_period='实销-个月'):
    #转置表之后string类型的表头无法正确排序，用数字型表头转置完成后，再填充周期的文字 比如填充实销1个月，第4周
    before_str = fill_period.split('-')[0]
    after_str = fill_period.split('-')[1]

    new_header = []
    for c in df.columns:
        if type(c) != str or re.match('\d+',c) != None:
            new_header.append( before_str + str(int(c)) + after_str)
        else:
            new_header.append(c)
    df.columns = new_header
    return df

def get_year_month(string):
    """检查"""
    if type(string) ==str and re.search('\d{4}[年\- /]\d{1,2}[月\- /]',string) != None:
        year_month = re.findall('\d{1,4}',string)
        return [int(x) for x in year_month]     
    return None 

def process_enddate(df):
    #处理截止时间,如果是X年X月的STR格式就提取年和月组合成日期，其他情况采用pd.to_datetime尝试转换
    df['enddate'] = df['enddate']\
                .swifter.apply(lambda x: datetime.datetime(get_year_month(x)[0],get_year_month(x)[1],1) \
                    if get_year_month(x) != None else pd.to_datetime(x))
    #变成最后一天为截止日期
    df['enddate'] = df['enddate'].swifter.apply(lambda x:last_day_of_month(x))
    return df

def split2multi_tables(df,index_list,column_list):
    """将一个表根据列维度拆分成多个表
    :param df : input df table 
    :param index_list : 
    :param column_list :
    """
    df_list = [ ]
    for c in columns:
        df_copy = df.loc[:,index_list + column_list]

def pivot2multi_tables(df,index,columns,value_list,filt_column=None,filt_target=None,fillna=None):
    """将一个需要分N批次保存多个列数值的表摊开成N个
    :param index :same as pd.pivot index, regarding to the fix index column
    :param columns: the original column that needs to be expanded to the header row
    :param  value_list : the rest value columns corresponding to pd param values 
    """
    #转置，N个渠道摊开，日期放上面一行
    df_copy = df.copy()
    df_list =  []
    #特定字段只提取某个值
    prefix = ''
    if all([filt_column,filt_target]):
        if isinstance(filt_target,list): 
            df_copy = df_copy.loc[df[filt_column].isin(filt_target),:]
        else: 
            df_copy = df_copy.loc[df[filt_column]==filt_target,:]
        prefix = filt_target + '-'
        #注意过滤后过滤的目标字段已经剩下唯一值，故要把过滤字段剔除
        df_copy = df_copy.drop(filt_column,axis=1)

    for values in value_list:
        df_temp = df_copy.pivot(index=index,columns=columns,values=values)

        df_temp.index.name = prefix + values
        if fillna != None :
            df_temp = df_temp.fillna(value=fillna)
        df_list.append(df_temp)

    return df_list

def delete_unnamed_behind(df):
    original_columns = list(df.columns) 
    #去掉unnamed间隔以及间隔后面的所有列
    unnamed_columns =[ x for x in original_columns if \
             (type(x) == str and re.search('unnamed',x,flags=re.I) != None)] 

    if unnamed_columns :
        end_index = original_columns.index(unnamed_columns[0])
        df = df.iloc[:,:end_index]
    return df 


#删除重复列
def remove_duplicate_columns(df):
    columns = [x for x in df.columns if re.search('\.\d{1,3}$',x) == None]
    df = df.loc[:,columns]
    return df

def check_abnormal_dates(df_worksheet,date_column,table_path,sheet):
    """
    检查日期是否为空，日期是否填写异常不能正常转换成pandas的datetime
    """
    #转换发帖时间字段, 可能出现日期错误
    check_date_error = 0 

    check_date_empty = df_worksheet.loc[df_worksheet[date_column].isna()==True,:]
    #记录日期为空的部分，不允许填空
    if not check_date_empty.empty:
        empty_date_row = [str(x+2) for x in check_date_empty.index]
        empty_date_row = ','.join(empty_date_row)
        print('日期不允许为空：文档:{},来源Sheet:{},第{}行，请补充以上日期再运行'.format(table_path,sheet,empty_date_row))
        input('回车键退出')
        sys.exit()

    #如果发帖时间 被转成了int,float格式的时间 （在EXCEL会显示 1月2日，点进去单元格会出现正确的时间格式）
    df_worksheet[date_column] = df_worksheet[date_column].swifter.apply(lambda x: \
        datetime.strftime(xldate_as_datetime(x,0),'%Y-%m-%d') if (type(x) == int or type(x)==float) else x)

    try:
        df_worksheet[date_column] = pd.to_datetime(df_worksheet[date_column])
        check_date_error += 1 
    except (ValueError,OutOfBoundsDatetime) as e :      
        for i,t in zip(df_worksheet.index,df_worksheet[date_column]):
            try:
                convert_t = pd.to_datetime(t)
            except ValueError:  
                print('错误日期格式："{}"'.format(t),'文档第{}行'.format(i+2))
    #找不到发帖时间 
    except KeyError:
        print('无法映射出"{}"字段，请检查该字段对应表："{}","{}"'.format(date_column,table_path,sheet))
        input('回车键退出') 

    if check_date_error < 1:
        print('日期格式错误："{}", "{}"，请修改以上日期再运行'.format(table_path,sheet))
        input('回车键退出')
        sys.exit()

    return df_worksheet


def normalize_column_dates(df_worksheet,date_columns):
    """
    日期是否填写异常不能正常转换成pandas的datetime
    """
    if type(date_columns) != list:
        date_columns = [date_columns]

    for date_column in date_columns:
        #转换发帖时间字段, 可能出现日期错误

        try : #如果没问题 直接转成datetime 
            df_worksheet[date_column] = pd.to_datetime(df_worksheet[date_column])
        except :
            #如果发帖时间 被转成了int,float格式的时间 （在EXCEL会显示 1月2日，点进去单元格会出现正确的时间格式）
            df_worksheet[date_column] = df_worksheet[date_column].swifter.apply(lambda x: \
                datetime.strftime(xldate_as_datetime(x,0),'%Y-%m-%d') if (type(x) == int or type(x)==float) else x)
            try:
                df_worksheet[date_column] = pd.to_datetime(df_worksheet[date_column])
            except :      
                pass

    return df_worksheet

def normalize_dates(df_worksheet,date_columns,table_path='默认',sheet='默认'):
    """
    检查日期是否为空，日期是否填写异常不能正常转换成pandas的datetime
    """
    if type(date_columns) != list:
        date_columns = [date_columns]

    for date_column in date_columns:
        #转换发帖时间字段, 可能出现日期错误
        check_date_error = 0 

        check_date_empty = df_worksheet.loc[df_worksheet[date_column].isna()==True,:]
        #记录日期为空的部分，不允许填空
        if not check_date_empty.empty:
            empty_date_row = [str(x+2) for x in check_date_empty.index]
            empty_date_row = ','.join(empty_date_row)
            enter_exit('Date must not be empty：File:{},Sheet:{},Row:{},Please add an valid date then try again!'.format(table_path,sheet,empty_date_row))

        try : #如果没问题 直接转成datetime 
            df_worksheet[date_column] = pd.to_datetime(df_worksheet[date_column])

        except :
            #如果发帖时间 被转成了int,float格式的时间 （在EXCEL会显示 1月2日，点进去单元格会出现正确的时间格式）
            df_worksheet[date_column] = df_worksheet[date_column].swifter.apply(lambda x: \
                datetime.datetime.strftime(xldate_as_datetime(x,0),'%Y-%m-%d') if (type(x) == int or type(x)==float) else x)
            try:
                df_worksheet[date_column] = pd.to_datetime(df_worksheet[date_column])
                check_date_error += 1 
            except (ValueError,OutOfBoundsDatetime) as e :      
                for i,t in zip(df_worksheet.index,df_worksheet[date_column]):
                    try:
                        convert_t = pd.to_datetime(t)
                    except ValueError:  
                        print('错误日期格式："{}"'.format(t),'文档第{}行'.format(i+2))
            #找不到发帖时间 
            except KeyError:
                print('无法映射出"{}"字段，请检查该字段对应表："{}","{}"'.format(date_column,table_path,sheet))
                input('回车键退出') 

            if check_date_error < 1:
                print('日期格式错误："{}", "{}"，请修改以上日期再运行'.format(table_path,sheet))
                input('回车键退出')
                sys.exit()

    return df_worksheet

def normalize_dates_single(date):
    #上面的简单版，针对单个日期的标准化
    try:
        date = pd.to_datetime(date)
    except:
        if (type(date) == int or type(date)==float):
            date = datetime.datetime.strftime(xldate_as_datetime(x,0),'%Y-%m-%d')
            try:
                pd.to_datetime(date)
            except :
                pd.to_datetime(date)
        else:
            enter_exit(f'Error when converting "{date}"" to datetime')

    return date


def copy_seperate_header_columns(df,pattern):
    """
    外销国家字段对应.xlsx，把共同的国家部分拆分成多个相同列
    :param df :
    :param pattern :re pattern   '[A-Za-z]{2}'
    """
    df_copy = df.copy()
    level_0_columns = df_copy.columns.get_level_values(0)

    for c in level_0_columns:
        match_list = re.findall(pattern,c)
        #如果匹配到多个目标，将目标拆分成多个字段，含有相同的数据，但表头变成拆分后的表头结果
        if len(match_list) > 1 :
            for m in match_list:
                #需要将多列分别赋值
                df_copy_need_copy = df_copy.loc[:,df_copy.columns.get_level_values(0)==c]
                for each_col in df_copy_need_copy.columns.tolist():
                    df_copy[(m,each_col[1])] = df_copy[(c,each_col[1])]
            #删除Multi index
            df_copy = df_copy.drop([(each_col)],axis=1)
    return df_copy

def normalize_multi_header(df):
    """将有MultiIndex的column字符串做标准化处理，去掉两边空格等"""
    df_copy = df.copy()
    df_copy_columns = [ tuple(y.strip().lower() for y in x) for x in df_copy.columns ]
    df_copy.columns = pd.core.index.MultiIndex.from_tuples(df_copy_columns)
    return df_copy

def split_column_by_words(df, split_words_dict):
    #split_config_dict {'需要拆分的字段':[ 用来做拆分标准词组的列表 ]}
    for column, split_standard_list in split_words_dict.items():
        #先获取到分隔符
        split_symbol = split_standard_list[1]
        split_standard_list = [ replace_re_special(x).strip() for x in split_standard_list[0] ]
        split_standard_list = sorted(split_standard_list, key = lambda x : len(x), reverse=True)

        split_pat = '(' + '|'.join(split_standard_list) +  ')'

        try:
            if split_standard_list:
                df[column] = df[column].fillna('').astype(str)

                #区分本来就是空的和非空的,否则会把原本是空的列删除
                df_column_null = df.loc[df[column]=='',:]
                df_column_notnull = df.loc[df[column] !='',:]
                
                #找到所有能匹配上的 进行拆分
                df_column_notnull[column] = df_column_notnull[column]\
                    .swifter.apply(lambda x: [ x if x.strip() != '' else x for x in re.split(split_pat,x.strip(), flags=re.I)])
                df_column_notnull[column] = df_column_notnull[column]\
                    .swifter.apply(lambda x: [ y for y in x if (y !='' and y != split_symbol) and y == y ])

                #重新合并
                df = pd.concat([df_column_null,df_column_notnull],axis=0,ignore_index=True)

            elif split_symbol != '':
                df[column] = df[column]\
                .swifter.apply(lambda x:x.split(split_symbol) if type(x) == str else [x])
            else:
                pass
        except KeyError :
            enter_exit(f'目的表找不到要拆分的字段：{column}')

        df = stack_list_column(df,column)

    return df

#将某个表的某个字段通过某个符号拆分之后组成叠成新的表(除了被拆分的字段，其他字段需要复制同样内容新生成一条)
def stack_list_column(df,split_column):
    """
    :param df : dataframe to be processed 
    :param split_column : list, column that need to be split and stack
    :param split_symbol : symbol(sign) used to split the specific column
    :return stacked Dataframe, specific list type column becomes string on each new row 
    """
    #防止误修改原始表
    df_copy = df.copy()
    column_list = df_copy.columns.tolist()
    
    #准备构建新的DataFrame
    df_copy_list = []

    symbol_index = column_list.index(split_column)

    for index, value_list in zip(df_copy[split_column].index,df_copy[split_column].values):
        #不含有逗号的部分  和前面含有逗号处理方式 不一样，会存在 有类别的没有现象的情况,所以不能过滤value<1的
        insert_list = df_copy.loc[index,:].tolist()
        if value_list: #如果拆分的该列为空也一样要保留这行数据，不能直接删除！
            for value in value_list:  
                insert_list_copy = insert_list.copy()
                insert_list_copy[symbol_index] = value
                df_copy_list.append(insert_list_copy)
        else:
            insert_list_copy = insert_list.copy()
            insert_list_copy[symbol_index] = ''
            df_copy_list.append(insert_list_copy)
    
    #重新组合DF
    df_copy = pd.DataFrame(df_copy_list,columns= column_list)
    return df_copy

def expand_stacked_column_to_list(df,expand_column,unique_key):
    """
    #为了统计蓝色部分字段，把故障类别堆积回到列表的状态
    :param df : 原语言拆分匹配后已经被展开的表格
    :param expand_column : 被展开过的字段
    :param unique_key : 唯一编码字段
    """
    original_columns = df.columns.tolist()
    new_expand_column_name = expand_column + '_列表'
    
    df_copy = df.copy()
    
    df_convert_list = []
    unique_dict = defaultdict(list)
    empty_df = pd.DataFrame()
    
    for index,original_row in df_copy.iterrows():  #读取每一行数据
        _id = original_row[unique_key]
        value = original_row[expand_column]
        
        if unique_dict.get(_id,empty_df).empty:
            original_row[new_expand_column_name] = [value]
            unique_dict[_id] = original_row            
        else:     
            unique_dict[_id][new_expand_column_name].append(value)

    record_list = [x[1].values for x in unique_dict.items()]

    record_df = pd.DataFrame(record_list)
    record_df.columns = original_columns + [new_expand_column_name]
    
    return record_df

def expand_to_columns_pd(df):
    import gc 
    """
    把相同id的结果 叠到同一行,填入的只能是两列字段，一个是唯一字段，另一个是需要叠成一行并形成不同columns的字段
    默认叠第二个字段
    """
    original_columns = df.columns
    unique_key = original_columns[0]
    expand_column = original_columns[1]
    
    df_dict = defaultdict(set)
    
    for rows in df.iterrows():
        index = rows[0]
        rows_tuple = rows[1]
        
        _id = rows_tuple[unique_key]
        phen = rows_tuple[expand_column]

        df_dict[_id].add(phen)

    #形成字典后做一个中间格式，然后通过pandas再读取出来
    temp_path = 'temp_dict.txt'

    #计算ECC分类最多的ID有多少列数据
    maximum_ecc_column = 0 
    content = ''

    for k,v in df_dict.items() :
        len_v = len(v)
        if len_v >= maximum_ecc_column:
            maximum_ecc_column = len_v 
        content += k + '\t' + '\t'.join(v) + '\n'
            
    with open(temp_path,'w',encoding='utf-8') as f :
        f.write(content)
        
    del f 
    gc.collect()
    #根据最多的列决定如何读取table 
    new_columns = [unique_key] + [ expand_column + ' ' + str(x) for x in range(1,maximum_ecc_column + 1 ) ]
    #再把数据读回来
    record_df = pd.read_csv(temp_path,header=None,sep='\t', names=new_columns)  
    #删除不需要的临时文件
    os.remove(temp_path) 
    return record_df 


def get_list_partial_sorted(lst,number_regex='([^0-9]+)[0-9]{1,2}'):
    #获取一个列表中按照顺序排列的最后一组的部分，比如 [1,2,1,2,3,4],获取到后面的[1,2,3,4]
    previous = re.match(number_regex,[lst][0]).group()#默认第一个数字是最小值
    store_list = []
    for i in lst[1:]:
        i_num = int(re.match(number_regex,i).group())
        if i_num <= previous:
            store_list = []
            continue
        else:
            store_list.append(i)
    return store_list

def stack_columns_to_multi_row(df,target_stack_name=None,regex_foramt=None):
    """将多个故障摊开的列以_id为主键，摊开成多行,
    默认最后几列表头中带有0,1,2,3之类的属于应该被叠起来的故障现象
    :param target_stack_name 转换的目标默认字段名
    :param regex_foramt
    return result df 
    """
    #检查哪几个列带有数字的后缀
    stack_column_pat = '([^0-9]+)[0-9]{1,2}'
    
    df_columns = df.columns
    stack_columns = []

    if regex_foramt == None:
        stack_columns = [x for x in df_columns if re.match(stack_column_pat,x) != None]
    else:
        stack_columns = [x for x in df_columns if re.match(stack_column_pat,x) != None \
        and re.match(regex_foramt) != None]

        if target_stack_name == None and stack_columns:
            target_stack_name = re.match(regex_foramt,stack_columns[0]).group()

    #判断最后一个按顺序排列的列表
    stack_columns = get_list_partial_sorted(stack_columns)

    stack_number_list = [ re.match(stack_column_pat,x).group() for x in df_columns if re.match(stack_column_pat,x) != None \
        and re.match(regex_foramt) != None ]
    #保存数据
    stack_line_list = [ ]
    if len(stack_columns) < 2:
        enter_exit('电商爬虫数据中没有找到任何带有数字后缀的多个故障现象列')
    else:  #如果找到了对应列
        stack_header_name = re.match(stack_column_pat,stack_columns[0]).group(1)
        for i,row in df.loc[:,stack_columns].iterrows():
            stack_line = ','.join(sorted([x for x in row.values if type(x)==str and x !='nan']))
            stack_line_list.append(stack_line)

    df[target_stack_name] = stack_line_list
    df_columns = [x for x in df_columns if x not in stack_columns] + [target_stack_name]

    #堆叠字段全部放后面
    df = df.loc[:,df_columns]
    return  df 

# def stack_columns_to_multi_row(df,stack_column):
#   """将多个故障摊开的列以_id为主键，摊开成列表形式,之后可以进行摊开或者叠起操作，
#   注意原始文档不能带有重复字段，否则也会被叠起来
#   默认最后几列表头中带有0,1,2,3之类的属于应该被叠起来的故障现象"""
#   #检查哪几个列带有数字的后缀
#   stack_column_pat = '([^0-9]+)[0-9]{1,2}'
#   df_columns = list(df.columns)
#   stack_columns = [x for x in df_columns if re.match(stack_column_pat,x) != None]
#   stack_line_list = [ ]
#   if not stack_columns:
#       print('数据中没有找到任何带有数字后缀的列')
#   else:  #如果找到了对应列
#       #先确认这些列是否连在一起，优先提取连在一起,并且处于后面的表头序列
#       stack_column_index = [ df_columns.index(s) for s in stack_columns ]
#       index_list = [ ]
#       pre_index = 0 
#       for i in range(1,len(df_columns)+1):
#           if i in stack_column_index:
#               if i - pre_index == 1 :
#                   pre_index = i 
#                   index_list.append(i)
#               else:



#       for i,row in df.iloc[:,stack_columns].iterrows():
#           stack_line = ','.join([x for x in row.values if type(x)==str and x !='nan'])
#           stack_line_list.append(stack_line)

#   df[stack_column] = stack_line_list
#   df_columns = [x for x in df_columns if x not in stack_columns] + [stack_column]
#   df = df.loc[:,df_columns]
#   return  df 

def get_target_sheet(path,target_name):
    """
    通过Sheet名称获取目标Sheet，不读取隐藏的Sheet, 出现同名不同日期的Sheet优先读取后面最新的
    """
    match_list = [ ]
    df_workbook = pd.ExcelFile(path)
    sheets_property_list = df_workbook.book.sheets()

    for sheet_property in sheets_property_list:
        sheet_name = sheet_property.name 
        sheet_visibility = sheet_property.visibility 
        if sheet_visibility ==  0  and target_name in sheet_name:
            match_list.append(sheet_name)
    try:
        match_sheetname = match_list[-1]
    except IndexError:
        print('找不到名称为"{}"的Sheet'.format(target_name))
        return None

    if len(match_list) > 1 :
         print('\"{0}\"存在{1}份sheet文档,正在读取{2}'.format(target_name,len(match_list),match_sheetname))
    #遇到重复字段 overwrite 覆盖掉 -----mangle_dupe_cols not supported yet...还没支持
    # df = df_workbook.parse(match_sheetname,mangle_dupe_cols=False)
    df = df_workbook.parse(match_sheetname)
    df.columns = [x.strip() for x in df.columns]
    #关闭
    # df_workbook.close()
    df_workbook.close()
    return df

def get_target_sheet_wb(workbook,target_name,header=None):
    """
    通过Sheet名称获取目标Sheet，不读取隐藏的Sheet, 出现同名不同日期的Sheet优先读取后面最新的
    和get_target_sheet的区别在于传入的是一个workbook，并且最后不会关闭workbook（需要读取其他sheet）
    """
    match_list = [ ]
    sheets_property_list = workbook.book.sheets()

    for sheet_property in sheets_property_list:
        sheet_name = sheet_property.name 
        sheet_visibility = sheet_property.visibility 
        if sheet_visibility ==  0  and target_name in sheet_name:
            match_list.append(sheet_name)
    try:
        match_sheetname = match_list[-1]
    except IndexError:
        print('找不到名称为"{}"的Sheet'.format(target_name))
        return None

    if len(match_list) > 1 :
         print('\"{0}\"存在{1}份sheet文档,正在读取{2}'.format(target_name,len(match_list),match_sheetname))
    #遇到重复字段 overwrite 覆盖掉 -----mangle_dupe_cols not supported yet...还没支持
    # df = df_workbook.parse(match_sheetname,mangle_dupe_cols=False)
    if header != None:
        df = df_workbook.parse(match_sheetname,header=header)
    else:
        df = df_workbook.parse(match_sheetname)

    df.columns = [x.strip() for x in df.columns]
    return df


def convert_word_freq_dict(dict_text):
    text = ''
    if type(dict_text) == str:
        dict_text = json.loads(dict_text)
        for k,v in json.items():
            for i in range(v):
                text += ' ' + k

        return text.strip()
    else:
        return dict_text

def group_by_concat(df, group_column, agg_func_column):

    #防止改到另外的部分
    df_copy = df.copy()
    df_copy[agg_func_column] = df_copy[agg_func_column].swifter.apply(lambda x : x if type(x) == str else str(x))
    df_agg = df_copy.groupby(group_column)[agg_func_column].apply(' '.join)
    #以上结果是一个Series,需要转成DF, 不在这里reset_index，后面append本次结果 后面会用到groupby的index来concat
    df_agg = df_agg.to_frame()

    return df_agg 

def word_agg_func(df, group_column, agg_func, agg_func_column, keyword_list, stopword_list):

    #每条出现多次，只统计一次,不适用与sum的逻辑
    if 'unique' in agg_func and 'count' in agg_func:
        df[agg_func_column] = df[agg_func_column].swifter.apply(lambda x: ' '.join(set(process_text(x,stopword_dict))))

    if  agg_func  == "word_count_en" :
        #获取分组词频, 每行单词不去重
        df_agg = group_by_concat(df,group_column,agg_func_column)
        #进行分词和统计等处理, 使用swifter加速
        df_agg[agg_func_column] = df_agg[agg_func_column].swifter.apply(
                            lambda x: get_word_freq_dict(x,keyword_list,stopword_list,200))

    elif agg_func == 'word_sum_en':
        df[agg_func_column] = df[agg_func_column].swifter.apply(lambda x: json.loads(x) if type(x) ==str else x )
        #再根据词频转回文本格式
        df[agg_func_column] = df[agg_func_column].swifter.apply(lambda x: convert_word_freq_dict(x))

        #合并所有文本
        df_agg = group_by_concat(df,group_column,agg_func_column)
        #进行分词和统计等处理
        df_agg[agg_func_column] = df_agg[agg_func_column].swifter.apply(
                        lambda x: get_word_freq_dict(x,keyword_list,stopword_list))
    else: #以下是暂时复制的，中文分词还没写
        print('pass here ')
        pass 
    #转成json,文本格式
    df_agg[agg_func_column] = df_agg[agg_func_column].swifter.apply(lambda x: json.dumps(x))

    return df_agg

def group_basic_agg(df,group_column,agg_func, value_column=None, keyword_list= [],stopword_list=[ ], group_index=False):
    """
    根据分类统计基本的类别数量
    :param df : input df 
    :param group_column : groupby column, list
    :param agg_function : same as df agg func
    :return :  dataframe contains agg result
    """
    df_copy = df.copy()

    if type(group_column) != list:
        group_column = [ group_column ]

    #是否有传入指定数值的列,不传入只能做简单的count统计
    if value_column == None:
        agg_func_column = [ agg_func ]
        df_copy[agg_func_column] = True
    else:
        agg_func_column =  [ value_column ] 

    lack_column_list = find_lack_columns(df_copy, agg_func_column + group_column, 'Group statistic')

    df_copy = df_copy.loc[:, agg_func_column + group_column]

    df_copy = df_copy.fillna(0)

    if 'word' in agg_func:
        #注意这里的agg_func_columns是一个列表，如果需要做词频统计，需要取出第一个
        df_copy_agg = word_agg_func(df_copy,group_column, agg_func, agg_func_column[0],keyword_list,stopword_list)
    else:
        try:
            df_copy_agg =  df_copy.groupby(group_column).agg(agg_func).sort_values(by=agg_func_column,ascending=False)
        except AttributeError:
            enter_exit(f'"{agg_func}" function not found')

    if not group_index :
        df_copy_agg = df_copy_agg.reset_index()
    
    return df_copy_agg


def calc_total(df,total_column=0):
    """最后计算一行总计数量
    :param df : input df 
    :return : new df that has the last row name total 
    """
    df_copy = df.copy()
    df_copy = df_copy.fillna(value=0)
    #calculate total get the last index
    last_index = df_copy.index.values[-1] + 1 
    total_series = pd.Series(df_copy.sum(axis=0,numeric_only=True),name=last_index)
    # total_series 
    df_copy = df_copy.append(total_series)
    if type(total_column) == int :
        df_columns = df.columns.tolist()
        total_column = df_columns[total_column]

    df_copy[total_column][last_index] = 'total'
    
    return df_copy


def calc_percent(df,total_column,calc_columns=None):
    """
    每个数字列的都计算一个比率, 默认左边第一列是文字类型的列，默认不计算非数字类型的列
    :param df : 含有各个类别统计数值的表格，基本格式：第一列是类型，第二列是每个类型的数量，
               后面的列都是不同条件的数量，最后一行是总计
    :param total_column : 指定的总计列，会通过总计列决定从哪里开始计算每一列的比率
    :param calc_columns : 指定哪些列需要计算百分比结果
    :return new df with percentage counted new columns, if df is empty return None 
    """
    if not df.empty:
        df_copy = df.copy()
        original_columns = list(df_copy.columns)
        #默认计算所有的数字类型列(numerical)的百分比
        if  calc_columns == None :
            calc_columns = original_columns

        elif type(calc_columns) != list :
            calc_columns = [calc_columns]

        #去掉非数字类型的列
        numerical_cols =  df_copy.select_dtypes(exclude=['object','datetime']).columns

        calc_columns = [x for x in calc_columns if x in numerical_cols]

        for c in calc_columns:
            #如果是总数比例，分母不同，而且需要去掉最后一行的总计再SUM 
            if c == total_column:
                denominator = df_copy[c].iloc[:-1].sum()
            else:
                denominator = df_copy[total_column]

            columns = list(df_copy.columns)
            c_index = columns.index(c)
            series_name = c + '-pct'
            percent_series = df_copy[c]*100 / denominator

            #注意pandas的insert不返回任何结果
            df_copy.insert(c_index+1, series_name, percent_series)
            #格式化展示比率
            # df_copy[series_name] = df_copy[series_name].swifter.apply(lambda x: '{}%'.format(round(x,2)) if type(x)==float else x )
        return df_copy
    else:
        return None 

def get_2_columns_div(df,dividend,divisor):
    """
    计算一个表格两列字段相处的结果，并且将结果round两位小数点填充百分百展示
    :param df : input df 
    :param dividend : 被除数  分子
    :param divisor :除数   分母
    """
    result_series = (df[dividend]*100/df[divisor]).swifter.apply(lambda x: '{0:0.2f}%'.format(x) if type(x)==float else x )

    return result_series


def get_outer_join_diff(df_list,on=None,left_right=None):
    """
    参考：http://www.datasciencemadesimple.com/join-merge-data-frames-pandas-python/
    Pandas的outer join函数默认是full outer join,即返回的是全部两边都有的数据，求不相交的部分需要进行以下操作
    获取outer join结果集 减去 inner join 结果集 取得不相同的数据
    
    :param df list :  list containing two dataframes 
    :param on : columns that used to join on 
    :param left_right : indicates the name of each df in the df_list,will be added to the new_column nams 
    return 两个表不相交的集合
    
    """
    #防止修改到原表格
    df_1_copy = df_list[0].copy()
    df_2_copy = df_list[1].copy()
    #如果关联字段没有填，默认关联两个表之间的相同
    if on == None:
        columns_1  = set(df_1_copy.columns)
        columns_2  = set(df_1_copy.columns)
        on = list(columns_1 & columns_2)

    if type(df_list) != list or len(df_list) != 2:
        raise ValueError("df_list must be of list type and contain 2 dataframes")
    #outer join是包含有所有结果集的，只是匹配不上关联字段的 第三个字段会出现空，需要确保第三个字段原本是非空才好进行判定
    if left_right == None:
        left_right = ['Left','Right']
        
    df_1_copy['table_source_1'] = left_right[0]
    df_2_copy['table_source_2'] = left_right[1]
    
    outer_join_df = pd.merge(df_1_copy,df_2_copy,'outer',on=on)
    #此时两个表里面的join_check_x和join_check_y一旦出现空就是要求的非交集
    outer_join_diff_df = outer_join_df.loc[(outer_join_df['table_source_1'].isna()==True)|(outer_join_df['table_source_2'].isna()==True),:]
    #填充构建一个新的合并来源列
    outer_join_diff_df['table_source_1'].fillna(value=outer_join_diff_df['table_source_2'],inplace=True)
    outer_join_diff_df = outer_join_diff_df.rename({'table_source_1':'from'},axis=1)
    #删除已经填充过去的列
    del outer_join_diff_df['table_source_2']
    
    return outer_join_diff_df

def fillna_by_column(df,same_column_name):
    """
    填充经过关联后 带有_x和_y的同名字段, 用右边填充左边的数据
    :param df 
    :param same_column_name 
    """
    df_copy = df.copy()
    column_x = same_column_name+'_x'
    column_y = same_column_name+'_y'
    
    df_copy[column_x] = df_copy[column_x].fillna(df_copy[column_y])
    del df_copy[column_y]
    df_copy = df_copy.rename({column_x:same_column_name},axis=1)
    
    return df_copy
