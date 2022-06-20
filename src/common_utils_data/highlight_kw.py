from xlsxwriter.workbook import Workbook
from xlsxwriter.exceptions import FileCreateError
from collections import defaultdict
import sys
import xlrd
import re
import os 
import time 
import math
from common_utils.os_functions import choose_file,choose_sheet_column,get_require_files

import warnings 
warnings.filterwarnings('ignore')

def replace_re_special(word):
	#注意\要写在前面，因为后面循环替换了\\进去
	for special_symbol in r'\-+()[]{}.*^$~|?':
		new_special_symbol = '\\' + special_symbol
		word = word.replace(special_symbol, new_special_symbol)
	return word

def get_keyword_dict(path):
	found_tag = 0 
	while found_tag <= 0:
		try:
			wb = xlrd.open_workbook(path)
			found_tag += 1 
		except FileNotFoundError:
			input('\n "Keywords.xlsx" File Not Found in Current Folder!\n \
Please put the Keywords.xlsx to current folder and Press Enter to continue')
			continue 

	ws = wb.sheet_by_index(0)

	#保存每个关键词列的文字
	keyword_dict = defaultdict(set)

	#表头,根据表头获取应该写入红色还是蓝色，还是粗体
	header_list = []
	for x in ws.row(0):
		if type(x.value) == str and x.value.strip() != '':
			header = x.value.strip()
		elif (type(x.value) == float or type(x.value) == int) :
			header = str(x.value).rstrip('0').rstrip('.').strip()
		else:
			#为了防止两列中间隔一个空的表头单元格
			header = None
		header_list.append(header)

	if not header_list:
		print('Keywords.xlsx should have table header/headers such as red,blue,bold etc.')
		input('\nPress Enter to exit')
		sys.exit()

	seen_keywords = set()
	for row in list(ws.get_rows())[1:]:
		for i,format_word in enumerate(header_list):
			if format_word != None:
				keyword_value = row[i].value 
				if type(keyword_value) == float and math.ceil(keyword_value) == keyword_value:
					keyword = str(keyword_value).rstrip('0').rstrip('.').strip()
				else:  #必须去掉容易导致歧义的特殊符号
					keyword = replace_re_special(str(keyword_value).strip().lower())

				if keyword not in seen_keywords and keyword != "" :
					keyword_dict[format_word].add(keyword)
					seen_keywords.add(keyword)

	wb.release_resources()

	return keyword_dict

def get_rich_param_list(text,keyword_dict,insert_dict):
	"""
	通过文本和关键词获取需要填入xlsxwriter write_rich_string的第三个内容/param的格式,忽略大小写,re.I
	:param text:
	:param keyword_list 
	"""
	#关键词列表为空返回原文
	keyword_list = []
	for x in keyword_dict.values():
		keyword_list += x

	if not keyword_list:
		return {'record_list':text}

	#3.5 导致无法正确定位数字和文字
	keyword_list = [str(x).lower().strip() for x in  keyword_list ]
	keyword_list = sorted(keyword_list,key=lambda x:len(x),reverse=True)

	keyword_pat = u'('+ '|'.join(keyword_list) + ')'
	#通过关键词拆分的句子
	try:
		split_list = re.split(keyword_pat,text,flags=re.I)
		split_list =  [str(x) for x in split_list if x != '' ]	
		#找到的关键词目标
		findall_list = re.findall(keyword_pat,text,flags=re.I)
		findall_list = [str(x) for x in findall_list if x != '']
		#不符合re的匹配格式,直接返回原文本
	except TypeError:
		return {'record_list':text}
	#找不到目标也返回原来的文本
	if findall_list == []:
		return {'record_list':text}

	#需要保证所有都是字符串格式
	record_list = []

	#记录不同颜色的关键词列表
	keyword_record_dict = defaultdict(list)
	for k in keyword_dict:
		keyword_record_dict[k] = []

	while findall_list:
		keyword = findall_list.pop(0)
		target_index = split_list.index(keyword)

		#配合keyword_dict的小写
		#keyword = keyword.lower()
		for k in keyword_dict.keys():
			if replace_re_special(keyword.lower()) in keyword_dict[k]:
				keyword_record_dict[k].append(keyword)
				#没有出现过的关键字才能继续写入，否则不做写入
				split_list.insert(target_index,insert_dict[k.lower()])

		#按顺序记录
		record_list += split_list[0:target_index+2]
		split_list = split_list[target_index+2:]
	#记录最后一段,如果split_list 为空
	record_list += split_list
	#返回一个需要插入文字的record_list和一个命中关键词的keyword_record_dict {'red':['blueteeth','face']}

	return {'record_list':record_list,'keyword_record_dict':keyword_record_dict}


def write_header(xlsxwriter_ws,header_column):
	for column_index,c in enumerate(header_column):
		xlsxwriter_ws.write(0,column_index,c)
	return xlsxwriter_ws


def get_insert_dict(xlsxwriter_wb,keyword_dict):
	insert_dict = defaultdict()
	#高亮关键字格式  红色
	for format_word, k_list in keyword_dict.items():
		#区分颜色和字体粗体或斜体
		format_word = format_word.lower()
		if format_word == 'bold' :
			custom_format = xlsxwriter_wb.add_format({'bold':True})
		else:
			custom_format = xlsxwriter_wb.add_format({'font_color':format_word})
		insert_dict[format_word] = custom_format

	return insert_dict

def write2wb(xlsxwriter_wb,highlight_column_list,keyword_dict,highlight_column='Text'):

	#获取insert_dict 
	insert_dict = get_insert_dict(xlsxwriter_wb,keyword_dict)

	new_highlight_name ='{0} With Macro'.format(highlight_column)

	bold_format = xlsxwriter_wb.add_format({'bold':True})
	#第一个sheet记录高亮关键词结果
	xlsxwriter_ws = xlsxwriter_wb.add_worksheet('highlight')
	xlsxwriter_ws_target = xlsxwriter_wb.add_worksheet('keyword_matched')
	#第一个sheet表头，只写一列即可,表头标黄色
	xlsxwriter_ws.write(0,0,new_highlight_name,bold_format)

	#第二个sheet表头
	for i,k in enumerate(keyword_dict.keys()):
		xlsxwriter_ws_target.write(0,i,k,bold_format)

	total_len = len(highlight_column_list)
	
	print('')
	for row_index,value in enumerate(highlight_column_list):  #[7743:7745]
		if  (row_index + 1) % 2000 == 0 or (row_index + 1== total_len)  :
			print(' Processing...',row_index+1,'/',total_len)

		rich_param_dict = get_rich_param_list(value,keyword_dict,insert_dict)

		params = rich_param_dict['record_list']
		if type(params) == list:
			params = [ x for x in params if x != ''] 

		keyword_record_dict = rich_param_dict.get('keyword_record_dict',None)

		#1.写入第一个文档的部分
		column_index = 0
		if type(params) == str:
			xlsxwriter_ws.write(row_index+1,column_index,params)
		elif type(params) == list and params:  #是列表格式并且不是空列表 
			success = xlsxwriter_ws.write_rich_string(row_index+1,column_index,*params)	
			#如果返回是-5代表列表只有两个元素，不能写入,需要把文本取出来重新写
			if success == -5:
				one_word_text = params[-1]
				success = xlsxwriter_ws.write(row_index+1,column_index,one_word_text,params[0])
		else:
			xlsxwriter_ws.write(row_index+1,column_index,params)

		#写入第二个sheet的部分，通过关键词字典提取目标关键词
		if keyword_record_dict:
			counter = 0 
			for k,v in keyword_record_dict.items():
				if k == 'bold':
					cell_format = xlsxwriter_wb.add_format({'bold':True})
				else:
					cell_format = xlsxwriter_wb.add_format({'font_color':k})
				column_index = list(keyword_record_dict.keys()).index(k)

				v_text = ','.join(v)
				xlsxwriter_ws_target.write(row_index+1,column_index,v_text,cell_format)

	#调整每个列的宽度
	xlsxwriter_ws.set_column(0,0,width = 50)
	xlsxwriter_ws_target.set_column(0,len(keyword_dict),width=20)

	return xlsxwriter_wb

def highlight(xlsxwriter_wb,xlrd_ws,highlight_column,keyword_dict):
	"""
	可以自己选处理哪个内容
	"""
	#获取columns
	columns = [x.value for x in xlrd_ws.row(0)]
	#定位第几列需要高亮
	highlight_column_index = columns.index(highlight_column)
	#不取第一行
	highlight_column_series = [x.value for x in xlrd_ws.col(highlight_column_index)][1:]
	xlsxwriter_wb = write2wb(xlsxwriter_wb,highlight_column_series,keyword_dict,highlight_column)

	return xlsxwriter_wb

def save_xlsxwriter_wb(xlsxwriter_wb,path):
	close_tag = 0
	while close_tag <= 0:
		try:
			xlsxwriter_wb.close()
			close_tag += 1 
		except FileCreateError:
			input('\nFailed to write file!\n  Please Close "{}" Then Press Enter to Continue'.format(path))
	print('\n{0} Saved'.format(path))


def highlight_kw(path,keyword_dict):

	content_path = choose_file(path)
	xlrd_wb,xlrd_ws,sheet_name,highlight_column = choose_sheet_column(content_path)

	new_path = re.match('(.*).(xlsx|xls|xlsm)$',content_path).group(1) + '_Macro.xlsx'

	xlsxwriter_wb = Workbook(new_path)
	xlsxwriter_wb = highlight(xlsxwriter_wb,xlrd_ws,highlight_column,keyword_dict)

	#传过来只是为了关闭
	xlrd_wb.release_resources()

	save_xlsxwriter_wb(xlsxwriter_wb,new_path)

if __name__ == '__main__' :
	keyword_path = get_require_files('.\\',["keyword"],if_walk_path=False)['keyword']

	keyword_dict = get_keyword_dict(keyword_path)

	highlight_kw('.\\',keyword_dict)

	input("\nPress Enter to Exit")
