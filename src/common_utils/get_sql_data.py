from common_utils.sql_functions import execute_fetchall_engine
from common_utils.excel_functions import save_xlsxwriter_wb
from common_utils.os_functions import enter_exit
from xlsxwriter.exceptions import FileCreateError
import re 
import os 
import pandas as pd 
import time 
import configparser

#读取ini配置
cfparser = configparser.ConfigParser()
cfparser.read('mysql_connection_config.ini',encoding="utf-8")

config_sections = cfparser.sections()

host = cfparser.get('connection','host')
port = cfparser.get('connection','port')
database = cfparser.get('connection','database')
charset = cfparser.get('connection','charset')
username = cfparser.get('connection','username')
password = cfparser.get('connection','password')

#读取每次超过50W行就保存到另一个XLSX文档
seperate_batch = 500000

engine_text = f'mysql://{username}:{password}@{host}:{port}/{database}?charset={charset}'

with open('query_sql.txt','r',encoding='utf-8') as file:
	sql = file.read()
	#去掉可能出现的看不见的UTF8-BOM空格
	sql = sql.replace('\ufeff','')

t1 = time.clock()
df = execute_fetchall_engine(engine_text=engine_text,sql=sql)

t2 = time.clock()

print('Results get in',round(t2 - t1,0),'seconds')

df_length = df.shape[0]
save_name = 'sql_result'

save_path = "{0}.xlsx".format(save_name)

print('Writing to excel')

if df_length > seperate_batch:
	counter = 0 
	for i in range(0,df_length,seperate_batch):
		counter += 1 
		df_batch = df.iloc[i:i+seperate_batch,:]

		print('Getting the {}th Row to {}th Row'.format(i,i+seperate_batch))
		#创建新的文档
		save_path  = "{0}_{1}.xlsx".format(save_name,counter)

		writer = pd.ExcelWriter(save_path,engine='xlsxwriter',options={'strings_to_urls': False,\
						'strings_to_formulas': False},date_format='yyyy-mm-dd',datetime_format='yyyy-mm-dd')

		df_batch.to_excel(writer,sheet_name='result',index=False)

		save_xlsxwriter_wb(writer,save_path)
		t3 = time.clock()
		print(round(t3-t2,0),'seconds used')

else:#如果并没有超过预定的记录数直接提取后写入 
	writer = pd.ExcelWriter(save_path,engine='xlsxwriter',options={'strings_to_urls': False,\
					'strings_to_formulas': False},date_format='yyyy-mm-dd',datetime_format='yyyy-mm-dd')

	df.to_excel(writer,'result',index=False)
	save_xlsxwriter_wb(writer,save_path)
	t3 = time.clock()
	print(round(t3-t2),'seconds used')

enter_exit()