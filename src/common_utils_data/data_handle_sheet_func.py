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
from collections import defaultdict,Counter

from common_utils.sequence_functions import list_diff_outer_join, lcs, filter_lcs
from common_utils.os_functions import *
from common_utils.df_functions import *
from common_utils.config_table import ConfigReader 
from common_utils.excel_functions import write_format_columns
from common_utils.regex_functions import replace_re_special, replace_punctuations
from common_utils.decorator_functions import *
from common_utils.data_handle_func import * 

from pandas.core.indexes.multi import MultiIndex

class CsvSheetClass(object):
	def __init__(self, table_path):
		self.name = '{}'.format(table_path)
		self.visibility = 0

class Handler(object):

	def __init__(self, require_file_dir, input_dir,table_dict):
		#获取以下两个即可读取所有的原始数据和规则表
		#original data files 
		self.input_dir = input_dir
		#config table dict 
		self.table_dict = table_dict
		self.require_file_dir = require_file_dir

		# #从concat_data得到的结果记录输入数据的最大最小日期
		self.min_max_date_range = '' 

		#concat_data之后 保存一个原始表提供给后面的match做提取
		self.original_complete_header_df = None

	@catch_and_print
	def get_original2cn_dict(self, header_table_df, file_tag):
		"""
		将所有原始mapping成中文表头,按国家区分字典
		"""
		original2cn_dict_list = []
		original2cn_dict = defaultdict(str)
		fillna_dict = {}
		dtype_dict = {}

		if file_tag.lower() not in [ x.lower() for x in header_table_df.columns.get_level_values(0) ] :
			file_tag = 'Without file tag'

		header_column_index = header_table_df.columns.get_level_values(
			0) == file_tag.lower()

		header_table_df_c = header_table_df.iloc[:, header_column_index]

		header_table_first_three_c = header_table_df.loc[:, header_table_df.columns.get_level_values(0)[0]]

		# 同时获取填充的
		for row, last_three in zip(header_table_df_c.iterrows(), header_table_first_three_c.iterrows()):
			# 表头统一小写，换行符,空格全部去掉
			row_list = row[1].values
			last_three_list = last_three[1].values
			a_list = list(row_list)
			b_list = list(last_three_list)
			a_list = [str(x).lower().strip().replace('\n', '').replace('\xa0', '').replace(' ', '').replace('\t', '')
					  for x in a_list if x.strip() != '无' and x.strip().lower() != 'none' and x.strip() != '/' and x.strip() != '']

			if a_list:
				for x in a_list:
					original2cn_dict[x] = b_list[2]

			# 构建需要合并前填充的字典
			c_list = [x for x in a_list if split_colon(x)[0].lower().strip() == 'fillbeforeconcat' or split_colon(x)[0].strip() == '合并前填充']

			if c_list:
				for x in c_list:
					fillna_dict[b_list[2]] = split_colon(x)[1]

			if (b_list[1] != '默认' and b_list[1].lower() != 'default' and b_list[1] != '') and b_list[2] != '':
				dtype_dict.update({b_list[2]: b_list[1]})

		return original2cn_dict, fillna_dict, dtype_dict

	#合并读取的数据表格, 该函数需要输入table_dict因为需要读取到, complete_header_df, 和target_cn_columns
	@get_run_time
	def concat_data(self ):
		# 此函数读取放入的数据表，必须要运行
		for keys in self.table_dict.keys():
			if 'mapping' in keys.lower():
				mapping_key = keys
		try:
			header_table_df = self.table_dict[mapping_key]
		except KeyError:
			enter_exit('Cannot find mapping configuration sheet!')

		complete_header_df = self.table_dict['complete_header_df']
		target_cn_columns = self.table_dict['target_cn_columns']

		header_table_df = df_fillna_str(header_table_df)
		info_path_list = get_walk_abs_files(self.input_dir)

		# 检查是否有读取到各国的原始数据
		info_path_list = [x for x in info_path_list if '~$' not in x and (
			x[-5:].lower() == '.xlsx' or x[-4:].lower() in ['.xls', '.csv'])]

		if len(info_path_list) == 0:
			enter_exit(f'Cannot find any data file in folder "{self.input_dir}" !\n')

		success_sheet_df_list = []

		for table_path in info_path_list:
			table_p = Path(table_path)
			table_stem = table_p.stem
			table_suffix = table_p.suffix

			# 读取文件名的信息
			file_tag = table_stem.split('-')[0].split('_')[0].strip()

			# 获取这个文档的映射字典  将原始mapping成中文表头
			original2cn_dict, fillna_dict, dtype_dict = self.get_original2cn_dict(header_table_df, file_tag)

			if not original2cn_dict:
				enter_exit('"Data_processing_configuration" required mapping field "{}" not found !'.format(file_tag))

			# 如果是CSV文档
			is_csv = False
			is_xls_special = False
			if table_suffix == '.csv':
				is_csv = True
				csv_sheet_class = CsvSheetClass(table_stem)
				sheets_property_list = [csv_sheet_class]
			else:
				try:
					df_workbook = pd.ExcelFile(table_path)
					sheets_property_list = df_workbook.book.sheets()
					#试下能不能读取第一个sheet
					df_workbook.parse(str(sheets_property_list[0].name))
				except : #如果读取失败，尝试读取其他国家xls文档的格式
					is_xls_special = True 
					xls_sheet_class = CsvSheetClass(table_stem)
					sheets_property_list = [xls_sheet_class]

			# 过滤掉模板数据
			for sheets_property in sheets_property_list:
				sheet = sheets_property.name
				sheet_visibility = sheets_property.visibility
				if sheet_visibility == 0:  # 只读取可见的Sheet

					if is_csv:
						df_worksheet = read_csv_data(table_path)
						if df_worksheet.empty:
							continue
					elif is_xls_special: #这个格式的只读第一个sheet
						df_worksheet = read_xls_special(table_path)
						if df_worksheet.empty:
							continue
					else:
						df_worksheet = df_workbook.parse(str(sheet), na_values='')

					# 表头做小写等替换并且，通过字典rename,全部调整成去掉中间空格、去掉一切无意义符号的字段
					df_worksheet.columns = [str(x).lower().strip().replace('\n', '').replace('\xa0', '')
											.replace(' ', '').replace('\t', '')if x == x else x for x in 
											df_worksheet.columns]

					df_worksheet = dropping_not_mapping(df_worksheet, original2cn_dict, target_cn_columns)

					#mapping填入了 + 号
					df_worksheet = combine_multi_plus(df_worksheet, original2cn_dict)

					#mapping前检查是否有重复的字段，如果原表已经有别的字段映射成"机型"，那原表里面的"机型"字段属于要抛弃的字段
					df_work_sheet = drop_duplicated_columns_before_rename(df_worksheet, original2cn_dict)

					df_worksheet = df_worksheet.rename(original2cn_dict, axis=1)

					# 还必须要确认映射的字段没有重复，否则会影响到后面的数据列, 返回一个没有重复的字段列
					df_work_sheet = check_mapping_duplicates(df_worksheet, target_cn_columns, table_stem=table_stem)

					# 重命名之后，合并前需要填充默认值
					df_worksheet = fillna_with_dict(df_worksheet, fillna_dict)

					# 检查完重复映射之后 需要再定位一次需要的字段, 注意处理顺序
					df_worksheet = func_loc(df_worksheet, target_cn_columns)
					
					if not df_worksheet.empty:
						check_mapping_complete(df_worksheet, complete_header_df, original2cn_dict,file_tag=file_tag)

						#做一次字段格式处理，可以提示在哪个文档转错，但后面合并还是会把date转成object,所以需要再转一次
						complete_header_df = dtype_handle(complete_header_df, dtype_dict)
						# 记录成功表格
						success_sheet_df_list.append([table_stem, sheet, df_worksheet.shape[0]])
						#complete_header_df 是一个完整的表头，可以避免concat造成的表头混乱/缺失，
						#但合并会导致字段全变成object(CSV文档，xlsx的输入不受影响)
						complete_header_df = pd.concat([complete_header_df, df_worksheet], axis=0, sort=False, ignore_index=True)

						print(f'Getting from: "{table_stem}",Sheet:{sheet}, {df_worksheet.shape[0]} rows')

			success_sheet_df = pd.DataFrame(success_sheet_df_list, columns=['File Name', 'Source Sheet', 'Success reading records'])

		complete_header_df = dtype_handle(complete_header_df, dtype_dict)

		self.min_max_date_range = get_min_max_date(complete_header_df)
		self.original_complete_header_df = complete_header_df.copy()

		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df, success_sheet_df


	 # 1.字段标准化: 是否考虑先做关联匹配以加速运行? -- 不可行，关联的国家字段需要先标准化才能用来统一关联匹配
	@get_run_time
	def standardize_columns(self, complete_header_df, standardize_config_df):
		# 将字段处理为标准表里的字段（模糊匹配）
		table_values = standardize_config_df.values
		not_standardize_df_list = []
		complete_header_df_sub_list = []
		partial_match_not_match_df_list = []

		row_counter = 0
		for row in table_values:
			row_counter += 1
			source_column = row[1]
			standard_table_name = row[2]
			standard_column = row[3]
			target_column = row[4]
			target_column_edit = row[5]
			order_columns = row[6]
			replace_dict = get_replace_dict(row[7])
			special_syn = [x.lower() for x in row[8].split('\n')]  #两边需要同时存在的字符
			filter_condition = row[9]
			# 标准化模式：简单模糊匹配 -- simple_lcs  严格模糊匹配 -- filter_lcs, 内存配置匹配--number_similarity
			standardize_mode = row[10]

			#决定最后的结果字段名称
			temp_column = ''
			if target_column_edit == source_column:
				temp_column = source_column
			else:
				temp_column = target_column_edit

			# 先把空值的数据剔除再做模糊匹配
			complete_header_df_notna = complete_header_df.loc[complete_header_df[source_column].isna() == False, :]
			complete_header_df_nan = complete_header_df.loc[complete_header_df[source_column].isna(), :]
			#过滤后全都是空值的话 直接继续下一行
			if complete_header_df_notna.empty:
				continue

			if standard_table_name != '' and standard_column.strip() != '':

				mode = get_standard_mode(standardize_mode)
				
				print(f'{row_counter}.Processing standardization for "{source_column}"')
				print(f'-- Referencing from table "{standard_table_name}" column "{target_column}",Mode:{standardize_mode}')
				standard_table_path = get_require_files(self.require_file_dir, [standard_table_name])[standard_table_name]
				# 统一将原始关联表转成str格式
				standard_table_df = read_config_table(standard_table_path)
				##读取出来的表 统一做删除重复字段处理, 如果模式是机型匹配模式
				standard_table_df = remove_duplicate_columns(standard_table_df)

				# 标准对照表排序,排序完之后删除重复项，似的做了排序后的结果取的是第一行上市日期最近的机型（后面循环的时候做duplicates删除）
				if order_columns != '':
					standard_table_df = process_sort_order(standard_table_df, [standard_column], order_columns)

				# 需要对标准表做重复删除，类似字段匹配, 但不相同
				# 标准化前的第一层简单过滤
				filter_condition_2_columns_tag, filter_left_column, filter_right_column = False, '', ''

				if filter_condition != '':
					filter_condition_2_columns_tag, filter_left_column, filter_right_column = \
						get_filter_condition_standardize_tag(filter_condition)

				if filter_condition != '' and filter_condition_2_columns_tag == False:
					try:
						standard_table_df = standard_table_df.query(filter_condition)
					except:
						enter_exit(f'Standardization: Failed to compile condition: {filter_condition}')

				#全部转成半角符号
				standard_table_df[standard_column] = standard_table_df[standard_column].apply(
															lambda x : normalize_punctuations(x) if type(x) == str else x ) 
				complete_header_df_notna[source_column] = complete_header_df_notna[source_column].apply(
															lambda x : normalize_punctuations(x) if type(x) == str else x)
				# 标准化前的第二层检查过滤(过滤条件涉及两张表的字段相等(不同国家的机型匹配))
				# 如果存在另外一种过滤方式--左右表的字段相等, 需要循环条件 进行模糊匹配
				if filter_condition_2_columns_tag:
					# 模糊关联匹配之前，必须做去重，防止笛卡尔积现象（模糊匹配防止获取的不是排序第一的数据）
					find_lack_columns( standard_table_df, [standard_column, target_column, filter_left_column])
					# 需要保留一个模糊匹配需要获取的 target_column
					standard_table_df_x = standard_table_df.loc[:, [standard_column, target_column, filter_left_column]]\
						.drop_duplicates(subset=[standard_column, filter_left_column], keep='first')

					# 循环获取模糊匹配结果
					if filter_right_column not in complete_header_df_notna.columns:
						enter_exit(f'Standardization Error: Cannot find column:"{filter_right_column}"')
					#记录每个的情况
					complete_header_df_sub_list = []
					for u in complete_header_df_notna[filter_right_column].unique() :
						temp_standard_df = standard_table_df_x\
								.loc[standard_table_df_x[filter_left_column] == u, [standard_column, target_column]]
						standard_dict = temp_standard_df.to_dict()

						print(f'Standardizing: "{u}"--"{source_column}"')

						standard_dict = {x: y for x, y in zip(
							standard_dict[standard_column].values(), standard_dict[target_column].values())}

						complete_header_df_sub = complete_header_df_notna.loc[
							complete_header_df_notna[filter_right_column] == u, :]

						complete_header_df_sub[temp_column] = complete_header_df_sub[source_column].fillna(value='').astype(str)

						complete_header_df_sub[temp_column] = complete_header_df_sub[temp_column].apply(
								lambda x: standardize_column_func(
								x, standard_dict, special_syn, replace_dict, ignore_punctuation=True, mode=mode))

						complete_header_df_sub_list.append(complete_header_df_sub)

					if len(complete_header_df_sub_list) == 1:
						complete_header_df_notna = complete_header_df_sub_list[0]
					elif len(complete_header_df_sub_list) >= 2 :
						complete_header_df_notna = pd.concat(complete_header_df_sub_list, axis=0, ignore_index=True)
					else: #complete_header_df_sub都没有,生成一列空白的结果
						complete_header_df_notna[temp_column] = ''

				# 如果是普通的df quiry过滤方式
				else:
					lack_column_list = find_lack_columns(
									standard_table_df, [standard_column, target_column],'Standardization reference table')
					# 提取出标准列表
					standard_dict = standard_table_df.loc[:, [standard_column, target_column]].to_dict()
					standard_dict = {x: y for x, y in zip(
						standard_dict[standard_column].values(), standard_dict[target_column].values())}

					complete_header_df_notna[temp_column] = complete_header_df_notna[source_column]\
							.apply(lambda x: standardize_column_func(x, standard_dict, special_syn, replace_dict,
													ignore_punctuation=True, mode=mode) if type(x) == str else x)

				# 空的和处理过的非空数据记得合并
				complete_header_df = pd.concat([complete_header_df_notna, complete_header_df_nan], axis=0, ignore_index=True)

				# 需要记录这两个字段分别有哪些记录匹配不上
				partial_match_not_match_df = get_partial_not_match(complete_header_df_notna,row_counter,source_column,
																	standard_table_name, standard_column,
																	filter_condition, target_column_edit,  filter_left_column)

				partial_match_not_match_df_list.append(partial_match_not_match_df)

		if partial_match_not_match_df_list:
			partial_match_not_match_df = pd.concat(partial_match_not_match_df_list, axis=0, ignore_index=True)

		# 模糊匹配的因为涉及"获取结果的字段名称"可能会修改成原始字段名称，判断复杂, 故不做记录
	
		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df, partial_match_not_match_df

	# 2.字段拆分
	@get_run_time
	def split_columns(self, complete_header_df, split_table_df):

		#拆分字段,并做堆叠, 获取的配置格式 {'拆分字段':[ 标准词组列表]} 
		split_table_df = df_fillna_str(split_table_df)
		split_table_values = split_table_df.values

		split_config_dict = { }
		for row in split_table_values:
			split_column = row[1]
			split_table_name = row[2]
			split_table_column = row[3]
			filter_condition = row[4]
			split_mode = row[5]
			split_symbol = row[6]

			if split_table_column != '' and split_table_name.strip() != '' and split_column.strip() != '' :

				split_file_dict = get_require_files(self.require_file_dir,[split_table_name])
				split_file_path = split_file_dict[split_table_name] 
				split_standard_df = read_config_table(split_file_path)
				split_standard_df.columns = [ x.strip() if type(x) == str else x for x in split_standard_df.columns  ]

				if filter_condition:
					split_standard_df = df_query(split_standard_df, filter_condition)
				if split_table_column not in split_standard_df.columns:
					enter_exit(f'Failed to find column "{split_table_column}" in table "{split_table_name}" ')
				else:
					split_standard_list = [ x for x in split_standard_df[split_table_column].tolist() if x == x and x != '' ] 
					split_standard_list = sorted(split_standard_list, key= len , reverse=True)
				split_config_dict.update({split_column: [split_standard_list, split_symbol ]})
			else:
				print(f'Standard column used to split is empty, "{split_symbol}" will be the symbol used to split')
				split_config_dict.update({split_column:[[], split_symbol] })

		complete_header_df = split_column_by_words(complete_header_df, split_config_dict, mode=split_mode)

		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df

	# 3.字段匹配
	@get_run_time
	def match_columns(self, complete_header_df, match_table_df):
		table_values = df_fillna_str(match_table_df).values

		not_match_df_list = []
		for row in table_values:
			source_columns = split_colon(row[1])  # 目的表字段
			join_table_name = row[2]  # 关联表
			join_columns = split_colon(row[3])    # 关联字段
			target_columns = split_colon(row[4])  # 想获取的目标字段
			target_columns_edit = split_colon(row[5])  # 结果字段名称
			sort_order = row[6]  # 匹配前的去重排序
			replace_dict = get_replace_dict(row[7]) 
			filter_condition = row[8]  # 关联表过滤条件
			match_mode = row[9]  # 匹配模式：1.case insensitive 2.case sensitive
			# For columns with spaces in their name, you can use backtick quoting.
			
			#如果目标字段和重命名的目标字段 长度不等，直接全部改成所需要的字段名称
			if len(target_columns_edit) != len(target_columns) :
				target_columns_edit = target_columns 

			if source_columns !='' and len(join_columns) > 0 and len(target_columns) > 0  :
				#如果两边的字段数量不一致
				if len(source_columns) != len(join_columns):
					min_num = min([len(source_columns), len(join_column)])
					source_columns = source_columns[:min_num+1]
					join_columns = join_columns[:min_num+1]

				#如果填了匹配表，开始读取
				if join_table_name != '':
					join_table_path = get_require_files(self.require_file_dir, [join_table_name])[join_table_name]
					join_table_df = read_config_table(join_table_path)
				else: #如果没有填匹配表，则使用原始输入表格进行匹配
					join_table_df = self.original_complete_header_df

				# 检查两张表 有没有字段缺失
				find_lack_columns(complete_header_df, source_columns,'Matching, complete_header_df')
				find_lack_columns(join_table_df, set(join_columns + target_columns), 'Matching, join_table')

				join_table_df = process_join_table(join_table_df=join_table_df, join_columns=join_columns, 
												   target_columns=target_columns, filter_condition = filter_condition, 
												   sort_order = sort_order, join_table_name = join_table_name)

				complete_header_df = process_match_complete_table(complete_header_df= complete_header_df, 
																  source_columns = source_columns ,target_columns = target_columns, 
																  join_columns = join_columns, join_table_name = join_table_name)

				#重命名进行匹配，必须要做的，无论是否用的外部表匹配
				for i in range(len(source_columns)):
					join_table_df[source_columns[i]] = join_table_df[join_columns[i]]

				# 防止只匹配表只有一列，并且只想获取该列结果
				join_table_df, only_one_match_column = check_only_one_match_column(join_table_df, join_columns, target_columns )

				# 匹配默认忽略大小写
				if type(match_mode) == str and match_mode.lower().replace('-', '') == 'case sensitive':
					complete_header_df = pd.merge(complete_header_df, join_table_df, 'left', on=source_columns)
				else:
					complete_header_df = merge_case_insensitive(complete_header_df, join_table_df, 'left', on=source_columns)
				if only_one_match_column:
					complete_header_df = complete_header_df.rename({'additional_temp': target_columns_edit[0]}, axis=1)

				#重命名，直接列表重新赋值即可
				if target_columns_edit != target_columns:
					for t1, t2 in zip(target_columns, target_columns_edit):
						complete_header_df[t2] = complete_header_df[t1]

				# 记录无法匹配到结果的数据，第一个target_columns如果为空，判断为无法匹配
				not_match_df = complete_header_df.loc[complete_header_df[target_columns_edit[0]].isna(), source_columns]\
				.drop_duplicates()

				not_match_df['Content'] = not_match_df[source_columns].agg('+'.join, axis=1)

				not_match_df['Matching field'] = '+'.join(source_columns) + ' Match ' + '+'.join(join_columns)

				#转换表头顺序
				shift_order_list = list(not_match_df.columns)
				not_match_df = not_match_df.loc[:,['Matching field','Content']]

				not_match_df_list.append(not_match_df)

		if join_table_name != '':
			not_match_df = pd.concat(not_match_df_list, axis=0, ignore_index=True)

			not_match_df = not_match_df.drop_duplicates()
		else:
			not_match_df = pd.DataFrame([])

		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df, not_match_df

	# 4. 字段去重, 顺便生成MD5
	@get_run_time
	def drop_duplicate_data(self, complete_header_df, drop_duplicates_table_df):
		table_values = drop_duplicates_table_df.values
		drop_subset_list = []
		md5_unique_column_list = []
		for row in table_values:
			column = row[0]
			if_drop_duplicate = row[1]
			if_gen_md5 = row[2]

			if column.strip() != '' and column in complete_header_df.columns:
				if if_drop_duplicate.lower().strip() in ['是', 'yes', 'y']:
					drop_subset_list.append(column)
				if if_gen_md5.lower().strip() in ['是', 'yes', 'y']:
					md5_unique_column_list.append(column)
			else:
				enter_exit(
					f'Deduplicate reference table error: Column "{column}" not found! ')

		if drop_subset_list:
			complete_header_df = complete_header_df.drop_duplicates(
				subset=drop_subset_list)

		if md5_unique_column_list:
			complete_header_df = column_gen_md5(
				complete_header_df, md5_unique_column_list, 'MD5')

		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df

	# 5.填充排序
	@get_run_time
	def fill_and_sort_columns(self, complete_header_df, fill_and_sort_table):
		
		table_values = fill_and_sort_table.values
		
		source_columns = [ row[0] for row in table_values]
		complete_header_df_new = pd.DataFrame([])

		seen_columns = set()
		sort_column_list = []
		sort_column_order_list = []
		dtype_dict = {}

		for row in table_values:
			input_column = row[0]
			output_column = row[1]
			# 输出的类型 日期类型--不包含时分秒 normalize().date 时间类型-包含时分秒-to_datetime, 和其他pandas支持的数据类型
			output_dtype = row[2]
			# 先做内容替换，再做空值填充
			replace_value_str = row[3]
			sort_value_str = row[4]
			fillna_value_str = row[5]

			if input_column != '' and output_column != '':
				if input_column not in complete_header_df.columns:
					if_skip = input(
						f'"{input_column}" not found in result table, thus not able to be mapped to the output result, continue?(Enter to continue)')
					if if_skip == '' or if_skip.lower() == 'yes' or if_skip.lower() == 'y':
						continue
					else:
						enter_exit('')
				#先将旧数据赋值到新数据 , 如果是重复填入的则保持用complete_header_df_new的数据处理即可
				if input_column not in seen_columns:
					complete_header_df_new[output_column] = complete_header_df[input_column].fillna('')
					seen_columns.add(input_column)
					
				if input_column != '' and output_column != '':
					if replace_value_str != '':
						complete_header_df_new = replace_value_func(
							complete_header_df_new, replace_value_str, input_column, output_column)

					# 过滤： 以下全部沿用上面获得的complete_header_df_new和他的所有output_column
					if fillna_value_str != '':
						for fillna_value_str_x in split_colon(fillna_value_str):
							complete_header_df_new = fillna_value_func(
								complete_header_df, complete_header_df_new, fillna_value_str_x, output_column)
					# 排序：统一加到列表，统一排序, 通过正-顺序，负数-倒序，数字代表是优先第几sort的顺序
					if sort_value_str.strip() != '':
						try:
							sort_value_number = int(float(sort_value_str))
							sort_column_list.append(output_column)
							sort_column_order_list.append(sort_value_number)
						except:
							pass
					# 输出类型：
					if output_dtype != '':
						dtype_dict.update({output_column: output_dtype})

		complete_header_df_new = dtype_handle(
			complete_header_df_new, dtype_dict, output=True)
			
		if sort_column_list:
			complete_header_df_new = sort_value_func(
				complete_header_df_new, sort_column_list, sort_column_order_list)

		print(f'Data rows:{complete_header_df_new.shape[0]}')

		return complete_header_df_new

	# 6.条件过滤
	@get_run_time
	def filter_columns(self, complete_header_df, filter_condition_table):

		table_values = filter_condition_table.values
		for row in table_values:
			condition = row[0].strip()
			complete_header_df = df_query(complete_header_df, condition)

		print(f'Data rows:{complete_header_df.shape[0]}')
		return complete_header_df

	@get_run_time
	def regex_extraction(self, complete_header_df, regex_extraction_table):

		table_values = regex_extraction_table.values

		for row in table_values:
			source_column = row[1]
			output_column = row[2]
			extract_pattten = row[3]

			find_lack_columns(complete_header_df, [source_column])

			if source_column != '' and extract_pattten != '':
				complete_header_df[output_column] = ''
				complete_header_df[output_column] = complete_header_df[source_column]\
				.apply(lambda x: '\n'.join(re.findall(extract_pattten,x,flags=re.I)).strip() if type(x) == str else '')

		return complete_header_df


	#***************以下是数据统计的函数***************************#

	#如果配置表有时间的处理的部分，传入需要计算的时间周期
	def calc_time(self, data_df,time_process_df):

		if not time_process_df.empty and not data_df.empty:

			for row in time_process_df.values:
				time_column = row[1]
				enddate = row[2]
				original_period = row[3]
				period = row[4].strip().lower()

				#获取时间范围对应的统计截止时间, 如果输入的是原始数据表格的字段，将该字段转成时间格式
				if enddate in data_df.columns:
					data_df = normalize_column_dates(data_df,enddate)
					enddate = data_df[enddate]
				else:
					enddate = normalize_dates_single(enddate)

				#获取需要统计的时间区间
				re_result = re.search('^([a-zA-Z\u4e00-\u9fa5]{1,}).?(\d{1,})$',original_period)

				#确保输入的时间区间格式
				try:
					period = re_result.group(1)
					period_number = re_result.group(2).strip()
				except:
					enter_exit('Time range input error: Make sure the input time range is like day_1, day_2, week_1, etc')

				if time_column not in data_df.columns:
					enter_exit(f'Time process error: Missing column:{time_column}')

				#将原始时间字段转成时间格式  
				data_df = normalize_column_dates(data_df, time_column)

				#用截止时间来计算时间不同的维度，day, week , month, year
				#使用np.timedelta64来计算 需要将日期转成pandas to_datetime的日期格式
				if period in ['day','days','天','日','d']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'D')
				elif period in [ 'week','weeks', '周','星期','w']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'W')
				elif period in [ 'month','months', '月','月数','m']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'M')
				elif period in [ 'year','years', '年','年数','y']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'Y')
				elif period in [ 'hour','hours', '时','小时','h']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'h')
				elif period in [ 'minute','minutes', '分','分钟','m']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'m')
				elif period in [ 'sales week', 'sales weeks','实销周','实销周数']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'W')
				elif period in [ 'sales month','sales months','实销月','实销月数']:
					data_df[original_period] = (enddate - data_df[time_column])/np.timedelta64(period_number,'M')
				else:
					enter_exit(f'Unknown time period:{original_period}')

				#再检查一次时间是否完整
				data_df_empty_date = data_df.loc[data_df[time_column].isna(),:]
				if not data_df_empty_date.empty:
					enter_exit('Time process error: column "{}" has {} empty datetime rows after combining all the data files!'\
						.format(time_column,data_df_empty_date.shape[0]))
				#往上取整，统一转成int格式
				data_df[original_period] = data_df[original_period].apply(np.ceil).astype(int)

				#将结果转成True和False,因为后面统计不能用数字结果去做分组, 结果为1的就是属于填入的时间区间
				# data_df[original_period] = data_df[original_period].apply(lambda x: True if x==1 else False)
		else:
			return data_df 
		return data_df

	@catch_and_print
	def process_statistic_groups(self, data_df, statistic_groups_df):

		#防止读取的透视字段是合并的单元格
		group_column = list(statistic_groups_df.columns)[1]
		statistic_groups_df[group_column] = statistic_groups_df[group_column].replace('',float('Nan')).fillna(method='ffill')

		values = statistic_groups_df.values

		groups = sorted(list(set([ x for x in split_colon(values[0][3]) if x !=''])))
		# #防止有过滤条件的聚合之后 部分主键丢失导致无法做skipna的计算功能
		agg_df_sub = generate_complete_index(data_df,groups)

		temp_df_list = [ agg_df_sub ]

		for row in values:
			filter_condition = row[1]
			drop_duplicates_condition = row[2]
			groups_str = row[3]
			groups = sorted(list(set([ x for x in split_colon(row[3]) if x !=''])))
			value_column = row[4]
			agg_func = row[5].strip()
			result_name = row[6]

			if value_column :
				#检查是否少字段
				lack_column_list = find_lack_columns(data_df, groups, 'Grouping error')
				#过滤和去重
				data_df_sub = df_query(data_df,filter_condition)
				data_df_sub = process_duplicates(data_df_sub, drop_duplicates_condition)

				if 'word' in  agg_func: #如果为空 返回空字典
					if data_df_sub.empty: 
						agg_df_sub = create_group_empty(data_df, groups, value_column, fillna=r'{}')
					else:
						require_file_dict = get_require_files(self.require_file_dir, ['keyword','stop_word'])
						keyword_dict, keyword_format_dict = get_keyword_dict(require_file_dict['keyword'])
						stopword_dict, stopword_format_dict = get_keyword_dict(require_file_dict['stop_word'])

						keyword_list = convert_key2list(keyword_dict)
						stopword_list = convert_key2list(stopword_dict) 
						#词频统计需要传入关键词字典
						agg_df_sub = group_basic_agg(data_df_sub, groups,  agg_func, 
											value_column,keyword_list,stopword_list,group_index=True)
				else:
					if data_df_sub.empty:
						agg_df_sub = create_group_empty(data_df, groups, value_column, fillna=0 )
					else:
						agg_df_sub = group_basic_agg(data_df_sub, groups,  agg_func, value_column,group_index=True)

				agg_df_sub = agg_df_sub.rename({value_column:result_name},axis=1)

				temp_df_list.append(agg_df_sub)

		if temp_df_list :
			agg_df = pd.concat(temp_df_list, axis=1)

		agg_df = agg_df.fillna(0).reset_index()

		return agg_df

	@catch_and_print
	def process_calculations(self, data_df, calculation_df):
		counter = 0 
		for row in calculation_df.values:
			counter += 1 
			calc_content = row[1].strip()
			result_name = row[2]
			if calc_content != '':
				try:
					data_df[result_name] = data_df.eval(calc_content)
				except :
					enter_exit(f' Calculation Error on row {counter}, \n Content:{calc_content}')

		return data_df

	@catch_and_print
	def pivot_table(self, data_df, pivot_table_df):
		#只读取第一行
		row_counter = 0  
		for row in pivot_table_df.values:
			row_counter += 1 
			if all([ x == x and x != '' for x in row[:-1] ]) :
				index = [ x for x in split_colon(row[0]) if x != '' ] 
				columns =  [ x for x in split_colon(row[1]) if x != '' ] 
				values =  [ x for x in split_colon(row[2]) if x != '' ] 
				#检查所有字段
				find_lack_columns(data_df, index + columns + values)
				data_df = pd.pivot_table(data_df,index=index, columns=columns, values=values)

				#如果得到的结果有多个表头，只保留最后一行表头
				if isinstance(data_df.columns, MultiIndex):
					try: # index error will occur if df is empty
						level_elem = len(data_df.columns[0])
						drop_len = [ x for x in range(level_elem) ][:-1] 
					except:
						drop_len = [0]
					data_df.columns = data_df.columns.droplevel(level=drop_len)

				data_df = data_df.reset_index(drop=False)

		return data_df 