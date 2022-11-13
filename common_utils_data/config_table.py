"""
==========
测试去掉文件的前面一部分注释
==========
"""

import gc 
import re 
import sys  
import warnings 
import os 
import time  
from datetime import datetime 
import warnings  
import numpy as np 
import pandas as pd
import hashlib
from xlsxwriter import Workbook 
from collections import defaultdict,Counter

from .os_functions import get_walk_files,get_walk_abs_files,\
check_require_files,check_create_new_folder,get_require_files,enter_exit
from .df_functions import normalize_multi_header,copy_seperate_header_columns,\
check_abnormal_dates, stack_list_column, df_fillna_str
from .excel_functions import write_pct_columns

warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)

from pandas.core.index import MultiIndex

class ConfigReader(object):
	"""
	读取配置文件,测试中文搜索内容

	:param config_file_dir: 配置文件路径
	:type config_file_dir: str
	:param config_table_name: 配置表格名称
	:type config_table_name: str
	:param config_list: 配置列表
	:type config_list: str
	:example:尝试对齐能不能实现多行
			 第二行example注释
			 再indent一次
	:return: A buffered writable file descriptor 
	:rtype: list
	"""
	def __init__(self,config_file_dir:str, 
				 config_table_name:str, 
				 config_list:str, 
				 *args, **kwargs) -> list:

		self.config_file_dir = config_file_dir
		self.config_table_name = config_table_name
		self.config_list = config_list 

		self.require_file_dir = kwargs.get('require_file_dir','.\\require_tables')
		self.data_file_dir = kwargs.get('data_file_dir',r"..\\data_files")

	def get_header_table(self, 
						 header_table_df:pd.DataFrame) -> pd.DataFrame:
		"""
		测试中文搜索结果，对齐表格表头的结果,检测波动幅度

		:param header_table_df: 表头表格的dataframe
		:return: 处理过的表格
		:example:多行
				 对齐测试
				 多一行
		:raise: Exceptions, FileNotFoundError
		"""
		#过滤全都是空的行
		header_table_df = header_table_df.dropna(how='all',axis=0)
		header_table_df = df_fillna_str(header_table_df)
		header_table_df =  normalize_multi_header(header_table_df)

		return header_table_df

	def get_complete_header_df(self, 
							   header_table_df: pd.DataFrame) \
							   -> tuple[pd.DataFrame, list]:
		#保留的标准表头数量 以第一列的序号为准
		header_table_columns = header_table_df.columns
		standard_column = header_table_df[header_table_columns[0]].fillna('').tolist()

		standard_column = [x for x in standard_column if x != '']

		for s in standard_column[::-1] :
			if s == '':
				standard_column.pop(-1)
			else:
				break

		target_column_num = len(standard_column)
		target_cn_columns = header_table_df[header_table_columns[2]][:target_column_num].tolist()

		complete_header_df = pd.DataFrame(data= [],columns=target_cn_columns)

		return complete_header_df,target_cn_columns 

	def get_config_tables(self, if_walk_path = True ):

		require_file_dict = get_require_files(self.config_file_dir, self.config_table_name,if_walk_path=if_walk_path)

		header_table_path = require_file_dict[self.config_table_name]
		
		df_workbook = pd.ExcelFile(header_table_path)

		sheet_property_list = df_workbook.book.sheets()

		table_dict = { }
		for sheet_property in sheet_property_list:
			sheet = sheet_property.name

			sheet_visibility = sheet_property.visibility

			if sheet_visibility == 0 :  #只读取可见的sheet
				for config in self.config_list:
					if config in sheet.lower().strip():
						#需要特殊处理的合并表
						if 'mapping' in config:
							table = df_workbook.parse(sheet, header = [0, 1])
							table = self.get_header_table(table)
							complete_header_df, target_cn_columns = self.get_complete_header_df(table)
						else:
							table = df_workbook.parse(sheet, header = 0 )

						if not table.empty:
							table = df_fillna_str(table)
							table_dict.update({sheet:table})

		table_dict.update({ 'complete_header_df' :complete_header_df,
							'target_cn_columns':target_cn_columns })

		return table_dict


if __name__ == '__main__':

	config_list = [ 'mapping',
					'standardization',
					'split',
					'match',
					'deduplication',
					'fill&sort',
					'filter',
					'extraction']


	config_list =[ 'mapping',
				   'time process',
				   'statistic groups',
				   'calculations',
				   'fill&sort']

	table_reader = ConfigReader(config_file_dir= '.\\',config_list=config_list,config_table_name= 'config',)

	table_dict = table_reader.get_config_tables(if_walk_path=False)

	df = table_dict['time process']

	print(df)
