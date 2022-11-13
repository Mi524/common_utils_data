"""
list , dict , itertools, Counter functions  
"""
import os
import re  
from itertools import combinations 
from collections import defaultdict


#检查sheet_name，如果出现重复会无法写入，后面加入不同的数字
def duplicate_elem_add_seq(lst):
	#给列表出现重复的字符串末尾加上序号
	temp_dict = defaultdict(int)
	result_list = [ ]
	for s in lst:
		if temp_dict[s] != 0:
			result_list.append(str(s) + str(temp_dict[s]))
		else:
			result_list.append(s)

		temp_dict[s] += 1 
		
	return result_list


#模糊匹配的函数
def lcs(a, b):
	# longest common subsequence
    tbl = [[0 for _ in range(len(b) + 1)] for _ in range(len(a) + 1)]
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            tbl[i + 1][j + 1] = tbl[i][j] + 1 if str(x) == str(y) else max(tbl[i + 1][j], tbl[i][j + 1])
    res = []
    i, j = len(a), len(b)
    while i and j:
        if tbl[i][j] == tbl[i - 1][j]:
            i -= 1
        elif tbl[i][j] == tbl[i][j - 1]:
            j -= 1
        else:
            res.append(a[i - 1])
            i -= 1
            j -= 1
    return res[::-1]

#严格模糊匹配函数
def filter_lcs(a,b):
    #过滤掉 断层的命中字符串
    match_list = lcs(a,b)
    a = a.replace(' ','')
    b = b.replace(' ','')
    match_list = [ x for x in match_list if x != ' ']
    new_match_list = [ ]

    if match_list:
        for i in range(len(match_list)):
            if match_list[i] != a[i]:
                break 
            else:
                new_match_list.append(match_list[i])
    else :
        return new_match_list

    return new_match_list



def list_diff_outer_join(split_result, findall_result):
	#寻找没有匹配上的集合，求两个列表的outer join差集
	not_match_list = [ ]
	for elem_f in findall_result:
		while split_result :
			elem_s = split_result.pop(0)
			if elem_s != elem_f :
				not_match_list.append(elem_s)
			else :
				break
				
	return [ x for x in not_match_list + split_result if x != '' ] 

def sublist_combinations_all(value_list):
	"""获取一个完整列表的所有有序子列表的组成可能"""
	value_len = len(value_list)
	value_combination_list = [ ]
	for i in range(2,value_len+1): 
		value_combination = combinations(value_list,i)
		for c in value_combination:
			value_combination_list.append(list(c))

	return value_combination_list

def find_sublists_seq(value_list):
	"""获取一个完整列表的所有有序子列表的组成可能,
	[3, 4, 3, 5, 6] -->
	[[3, 4], 3, 5, 6]
	[3, [4, 3], 5, 6]
	[3, 4, [3, 5], 6]
	[3, 4, 3, [5, 6]]
	例如：not up to mark  notuptomark，not good --> notgood  """
	list_len = len(value_list)
	record_list = [ ]
	for word_num in range(2,list_len+1):  
		for start_pos in range(0,list_len - word_num + 1 ):
			temp_list =  value_list[:start_pos] \
					  + [value_list[start_pos:start_pos+word_num]] \
					  +  value_list[start_pos+word_num:]

			record_list.append(temp_list)
	return record_list

def find_sublist_indexes(complete_list,sub_list):
	"""从完整大列表中搜索出小的子列表 从头到尾 在大列表中对应的索引（优先第一个元素,可以求出前中后的所有索引位置
	[a,b,c,d,e] --> [b,d,e]  -->  [1,3,4]  --> result range_index_dict
	:param complete_list:
	:param sub_list
	:return : 	
	 range_index_dict = {
	'sub_list_index':record_index_list,  
	'pre_range_index':pre_range_index_list,
	'mid_range_index':mid_range_index_list,
	'back_range_index':back_range_index_list
	}
	"""
	#防止原始列表被修改，复制版本列表会被修改
	complete_list_copy = complete_list.copy()
	record_index_list = [ ]
	record_index = 0 
	for s in sub_list:
		split_index = complete_list_copy.index(s)
		#跳过本元素取下一个开始的列表后部分
		complete_list_copy = complete_list_copy[split_index + 1 : ]
		#记录每次的索引位置,split_index是每次拆分后的单独记录
		record_index += split_index 
		record_index_list.append(record_index)
		#每次带走一个元素,记录索引需要对应加上带走的元素数量(+1)
		record_index += 1 
	#重叠的元素范围 range_index_dict

	#返回前中后三个部分的所有概念index
	complete_list_index = [ x for x in range(len(complete_list))]
	pre_range_index_list = complete_list_index[:record_index_list[0]]
	mid_range_index_list = [ x for x in complete_list_index[record_index_list[0]:record_index_list[-1]+1] \
						if x not in record_index_list ] 

	back_range_index_list = complete_list_index[record_index_list[-1]+1:]

	range_index_dict = {
	'sub_list_index':record_index_list,  
	'pre_range_index':pre_range_index_list,
	'mid_range_index':mid_range_index_list,
	'back_range_index':back_range_index_list
	}

	return range_index_dict


# a = find_sublist_indexes(['C_FACE', 'D_BE_VERB', 'D_BAD_1', 'D_AND', 'D_WORK', 'D_PREPOSITION'],['C_FACE', 'D_BE_VERB', 'D_BAD_1'])

# print(a)

def convert_twolist2dict(list_a,list_b):
	"""
	将两个列表转成字典形式
	"""
	return dict(zip(list_a,list_b))

def list_transpose(original_list):
	"""列表转置"""
	return list(map(original_list,zip(*l)))



def uneven_list_dictify(values):
	#为了让最后一级也全都是键值,补充进空字符串
	values = [ x+ [''] for x in values ]
	d = {}
	for row in values:
		here = d
		for elem in row[:-2]:
			if elem not in here:
				here[elem] = {}
			here = here[elem]
		here[row[-2]] = row[-1]
	return d

def forward_fill(values):
	#区别于pandas的forwordfill,不会填充后面为空的部分，只向前填充前面列空的部分
	complete_row = values[0]
	result = [complete_row]
	#第一行必定是有完整的前几列数据,从第二行开始读取填充
	for v_index in range(1,len(values)):
		value = values[v_index] 
		for i in range(len(value)):
			#如果发现有缺失的部分
			if value[i] == '':
				value[i] = complete_row[i]
			else:
				complete_row = value
				break
		result.append(value)
	return result


def recursive_find_nodes(dictionary,parrent_node,record_list):
	#记录数字的previous_num为列表形式 [ ]
	#需要返回一组列表，形式为 [ current_node, parrent_node]
	#例子：# recursive_find_nodes(nested_dict,parrent_node = '',record_list= record_list)
	if type(dictionary) == dict:
		for k in dictionary.keys():
			recursive_find_nodes(dictionary[k],k,record_list)
			record_list.append([k,parrent_node])
	else:
		record_list.append([dictionary,parrent_node])

	return record_list

def recursive_gen_nodecodes(dictionary,parrent_node,record_list,level,pre_counter):
	#同recursive_gen_nodecodes, 添加了
	#不生成编码的函数
	#例子：recursive_gen_nodecodes(nested_dict,parrent_node = '',record_list= record_list,level=0,pre_counter=['00','00','00','00'])
	counter_list = [ ]  
	counter = 0 
	if type(dictionary) == dict:
		for k in dictionary.keys():
			counter += 1 
			pre_counter[level] = str(counter).zfill(2)
			record_list.append([''.join(pre_counter[:level+1]),k,parrent_node])
			recursive_gen_nodecodes(dictionary[k], k ,record_list,level + 1, pre_counter)
	else:
		record_list.append([''.join(pre_counter),dictionary,parrent_node])

	return record_list