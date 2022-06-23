from .os_functions import enter_exit 
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text
from pathlib import Path
from configparser import ConfigParser
from shutil import copyfile 
import pandas as pd 
import traceback
import logging
import os 
import cx_Oracle 

# 批量扫描需要增加的表是否都有数据且数据正常更新

def get_connection_by_config(database_type='mysql',config_path= 'connection_config.ini'):
    cfparser = ConfigParser()
    cfparser.read('connection_config.ini',encoding="utf-8")
    config_sections = cfparser.sections()
    print('读取到的Sections:',config_sections)

    host = cfparser.get(database_type,'host')
    port = cfparser.get(database_type,'port')
    database = cfparser.get(database_type,'database')
    charset = cfparser.get(database_type,'charset')
    username = cfparser.get(database_type,'username')
    password = cfparser.get(database_type,'password')

    if database_type == 'mssql':
        database_type += '+pymssql'

    engine_text = f'{database_type}://{username}:{password}@{host}:{port}/{database}?charset={charset}'

    conn, db = get_sql_connection(engine_text)

    return conn, db

def gen_engine_text(config_path, database_type):
    #读取ini配置
    cfparser = ConfigParser()
    cfparser.read(config_path,encoding="utf-8")
    config_sections = cfparser.sections()

    host = cfparser.get(database_type,'host')
    port = cfparser.get(database_type,'port')
    database = cfparser.get(database_type,'database')
    charset = cfparser.get(database_type,'charset')
    username = cfparser.get(database_type,'username')
    password = cfparser.get(database_type,'password')

    engine_text = f'{database_type}://{username}:{password}@{host}:{port}/{database}?charset={charset}'

    if 'oracle' in database_type.lower():
        #Oracle需要指定client位置
        lib_dir = cfparser.get(database_type,'oci_folder_path')
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)

        database = cfparser.get(database_type,'database')
        try:
            service_name = cfparser.get(database_type,'service_name')
        except:
            service_name = ''

        try:
            sid = cfparser.get(database_type,'sid')
        except:
            sid = ''

    # engine = create_engine("oracle+cx_oracle://<username>:<password>@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = <host>)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = devdb)))") 

    if 'oracle' in database_type.lower() and service_name != '':
        engine_text = f'{database_type}://{username}:{password}@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (schema={database}) (SERVICE_NAME = {service_name}) (CHARSET=utf8)))'

    if 'oracle' in database_type.lower() and sid != '':
        engine_text = f'{database_type}://{username}:{password}@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (schema={database})(SID = {sid}) (CHARSET=utf8)))'

    return engine_text


def get_sql_connection(engine_text):
    db = create_engine(engine_text,poolclass= NullPool,isolation_level='AUTOCOMMIT')
    conn = db.connect()
    return conn, db

def close_sql_connection(conn,db):
    conn.close()
    db.dispose()
    
def execute_fetchall(conn,sql):
    try:
        sql =  text(sql)
        result = conn.execute(sql).fetchall()
        return result
    except :
        print('没有找到任何对应数据或者未知异常\n SQL:')
        print(sql)
        return None 
        
def convert_fetchall2df(fetchall_result):
    if fetchall_result :
        return pd.DataFrame(fetchall_result,columns=fetchall_result[0].keys())
    else:
        print('没有转换成功DF数据或未知异常')
        return pd.DataFrame([]) 


def execute_fetchall_engine(engine_text,sql):
    # print(engine_text)
    db = create_engine(engine_text,poolclass= NullPool)
    conn = db.connect()
    result = None 
    sql =  text(sql)
    try:
        fetchall_result = conn.execute(sql).fetchall()
        if fetchall_result :
            print('Results get:{} rows'.format(len(fetchall_result)))
            result = pd.DataFrame(fetchall_result,columns=fetchall_result[0].keys())
        else:
            print('Results set is empty')
            return pd.DataFrame([])
    except Exception as e :
        print('No data was found or unknow error happened\n:')
        logging.error(traceback.format_exc(e))
        return pd.DataFrame([])
    finally:
        conn.close()
        db.dispose()

    return result 

def execute_updates_engine(engine_text,sql):
    #通过engine_text直接执行语句
    db = create_engine(engine_text,poolclass= NullPool,isolation_level='AUTOCOMMIT')
    conn = db.connect()
    conn.execute(" set sql_safe_updates = 0; ")
    try:
        conn.execute(sql)
    except Exception as e:
        print(sql,'\n执行失败')
        logging.error(traceback.format_exc(e))
    finally:
        close_sql_connection(conn,db)

def get_sql_result(conn,sql):
    result = execute_fetchall(conn,sql)
    result_df = convert_fetchall2df(result)

    return result_df 

def get_alter_index_text(conn,table_name):
    #获取到增加或删除表索引的可执行TEXT文本, 废弃的function,mysql load方法不用去掉索引写入也非常快
    show_index_sql = text("""
                      SELECT table_name AS `Table`,
                           index_name AS `Index`,
                           GROUP_CONCAT(column_name ORDER BY index_name,seq_in_index  ) AS `Columns`
                        FROM information_schema.statistics
                        WHERE table_schema = database()
                        and table_name  = '{0}'
                        GROUP BY table_name, index_name
                        order by max(seq_in_index), length(GROUP_CONCAT(column_name ORDER BY index_name,seq_in_index )) 
                    """.format(table_name)) 
    #将获取的索引结果组合，清除掉索引 
    result = conn.execute(show_index_sql)
    table_indexes = result.fetchall()

    add_index_list = [ ]
    del_index_list = [ ]

    for idx in table_indexes:
        index_name = idx[1]
        column_name = idx[2]

        if index_name.upper() != 'PRIMARY':
            add_index_text = 'add index ' + index_name + '(' + column_name + ')' + '\n'
            del_index_text = 'drop index ' + index_name  + '\n'

            add_index_list.append(add_index_text)
            del_index_list.append(del_index_text)

    if add_index_list:
        add_index_text = text('alter table {} '.format(table_name) + ','.join(add_index_list) + ';').execution_options(autocommit=True)
        del_index_text = text('alter table {} '.format(table_name) + ','.join(del_index_list) + ';').execution_options(autocommit=True)

    return add_index_text,del_index_text 

def write2table(engine_text,df,table_name,how='normal'):
    # engine_text format : "mysql://root:00000000@localhost:3306/web_data?charset=utf8"
    """
    4种方式写入: 1.normal: 直接写入，可选参数是否清空原有表；
                2.complete_rewrite: 删除原有的所有数据，并整个写入新数据
                3.mysql_load：直接通过mysql_load 方式写入 (这个方式经测试已经非常快，不用再重建索引也很快)
               （适用于特别大的数据集, 都需要确保MYSQL已经有完整的表结构）
    """
    db = create_engine(engine_text,poolclass=NullPool, isolation_level='AUTOCOMMIT')
    conn = db.connect()

    #schema是发现oracle无法写入数据加上的
    if how == 'normal' :
        df.to_sql(table_name,con=conn,if_exists='append',index=False,chunksize=100000)

    elif how == 'complete_rewrite':
        try:
            truncate_statement = "truncate {};".format(table_name)
            conn.execute(truncate_statement)
        except :
            print('truncate table"{}" failed!'.format(table_name))

        df.to_sql(table_name,con=conn,if_exists='append',index=False,chunksize=100000)

    #如果采用第二种方式写入
    elif how == 'mysql_load' : 
        #如果存在自增主键,去掉表头的自增主键
        auto_increment_key = ''

        header_column_sql = text(" describe {} ;".format(table_name))
        result = conn.execute(header_column_sql).fetchall()
        if result:
            header_columns = [ x[0] for x in list(result) ]
        else:
            enter_exit('找不到表格:',table_name)
        #根据MYSQL表头结构构建一个能LOAD的CSV文档,如果表格存在自增主键，
        auto_increment_key_sql = """ SELECT COLUMN_NAME
                                     FROM INFORMATION_SCHEMA.COLUMNS  
                                     WHERE TABLE_SCHEMA = DATABASE()  AND TABLE_NAME = '{}'  AND DATA_TYPE = 'int'
                                     AND COLUMN_DEFAULT IS NULL AND IS_NULLABLE = 'NO' AND EXTRA like '%auto_increment%';
                                 """.format(table_name)

        auto_increment_key_sql = text(auto_increment_key_sql)
        result_auto = conn.execute(auto_increment_key_sql).fetchone()

        if result_auto != None:
            auto_increment_key = list(result_auto)[0]

        header_columns = [x for x in header_columns if x != auto_increment_key]

        for h in header_columns:
            if h not in df.columns:
                df[h] = None

        df = df.loc[:,header_columns]
        #保存数据到临时目的地
        temp_path = get_most_upper_level_path('df_temp_file.csv')

        df.to_csv(temp_path,encoding='utf8',sep=',',quotechar='"',escapechar='\\',index=False,header=None)

        if auto_increment_key != '':
            load_infile_sql = r"""  LOAD DATA INFILE '{0}'
                                    INTO TABLE {1} 
                                    CHARACTER SET 'utf8mb4'
                                    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                                    ESCAPED BY '\\'
                                    LINES TERMINATED BY '\r\n'
                                    ({2})
                                    SET {3} = NULL; -- 将默认需要自增的键设置为NULL即可正常写入
                                    """.format(temp_path,table_name,','.join(header_columns),auto_increment_key)
        else:
            load_infile_sql = r"""  LOAD DATA INFILE '{0}'
                        INTO TABLE {1} 
                        CHARACTER SET 'utf8mb4'
                        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                        ESCAPED BY '\\'
                        LINES TERMINATED BY '\r\n';
                        """.format(temp_path,table_name)

        load_infile_sql = text(load_infile_sql).execution_options(autocommit=True)
        try:
            conn.execute(load_infile_sql)
        except Exception as e:
            logging.error(traceback.format_exc())
        finally:
            conn.close()
            db.dispose()
        #删除temp CSV文档
        os.remove(temp_path)
    else:
        print(how,'写入方法未知,数据未写入')

    conn.close()
    db.dispose()

def load_sql_data(engine_text,table_name,load_file_path,if_truncate=False):
    """write csv file into table"""
    db = create_engine(engine_text,poolclass=NullPool, isolation_level='AUTOCOMMIT')
    conn = db.connect()

    # temp_path = get_most_upper_level_path('df_temp_file.csv')


    #复制一份数据到英文路径
    # copyfile(load_file_path,temp_path)

    load_infile_sql = r"""  LOAD DATA INFILE '{0}'
            INTO TABLE {1} 
            CHARACTER SET 'utf8mb4'
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            ESCAPED BY '\\'
            LINES TERMINATED BY '\r\n';
            """.format(load_file_path,table_name)
    
    load_infile_sql = text(load_infile_sql)
    conn.execute(load_infile_sql)

    #删除临时文件
    # os.remove(temp_path)

    print(load_file_path,'dump complete.')

    conn.close()
    db.dispose()


def get_sql_data(engine_text,table_name,sql,save_path,how='normal'):
    """通过SQL获取到目标数据并保存到文档"""
    db = create_engine(engine_text,poolclass=NullPool, isolation_level='AUTOCOMMIT')
    conn = db.connect()

    if how == 'normal':
        #execute 返回ResultProxy,fetchall如果为空返回空列表
        fetchall_result = execute_fetchall(conn,sql)
        result_df = convert_fetchall2df(fetchall_result)
        result_df.to_excel(save_path,index=False)
    elif how == 'mysql_dump':
         #保存数据到临时目的地,mysqldump不能识别中文的路径，所以必须获取到最高一级的类似C盘C://的路径才能正常写入
        temp_path = get_most_upper_level_path('df_temp_file.csv')

        #先获取一次表头,sql输入结尾不能填分号
        if ';' ==  sql.strip()[-1]:
            sql = sql.strip()[:-1]

        try:
            first_row = conn.execute(sql)
            header = list(first_row.keys())
        except Exception as e:
            logging.error(traceback.format_exc())
            print(sql,'数据提取失败')
            conn.close()
            db.dispose()
            return None

        dump_sql = r"""  {}
                        INTO OUTFILE '{}' 
                        CHARACTER SET 'utf8mb4'
                        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                        ESCAPED BY '\\' 
                        LINES TERMINATED BY '\r\n' ; 
                    """.format(sql,temp_path)

        if os.path.exists(temp_path):
            os.remove(temp_path)

        dump_sql = text(dump_sql)
        conn.execute(dump_sql)
        #获取到目标后，将结果复制到目标文件夹并且转换成EXCEL格式
        if save_path.split('.')[-1] == 'xlsx':
            result_df = pd.read_csv(temp_path)
            result_df.to_excel(save_path,header= header,index=False)
            os.remove(temp_path)
        else: #如果不是保存的XLSX格式，就直接复制CSV结果到目标文件夹
            copyfile(temp_path,save_path)     

        print(save_path,'数据已保存')
    else:
        enter_exit(how,"不能识别该导出方式")
        
    conn.close()
    db.dispose()

def insert_update_table(engine_text,df,table_name):
    """检查表的主键字段，根据主键字段，采用update的方式更新数据"""
    db = create_engine(engine_text,poolclass=NullPool, isolation_level='AUTOCOMMIT')
    conn = db.connect()
    
    sql_unique_key = text("""
                        SELECT k.COLUMN_NAME
                        FROM information_schema.table_constraints t
                        LEFT JOIN information_schema.key_column_usage k
                        USING(constraint_name,table_schema,table_name)
                        WHERE t.constraint_type in ('UNIQUE','PRIMARY KEY') 
                            AND t.table_schema=DATABASE()
                            AND t.table_name='{0}';
        """.format(table_name))

    result = conn.execute(sql_unique_key).fetchall()

    #结果只有一列,去掉seq
    unique_columns = set([x.values()[0] for x in result if x.values()[0] !='seq'])

    if unique_columns :  #如果存在唯一性约束（包括主键约束）
        #需要更新的字段 去掉 唯一性约束字段 就是需要更新的字段
        update_columns = [x for x in df.columns if x not in unique_columns]
        #将数据写入临时表采用insert into on duplicate update的方式更新目的表
        sql_drop_temp = text("drop table if exists temp_insert;")
        
        sql_insert = text(""" create temporary table temp_insert as 
                              (select * from {0} );""".format(table_name))

        conn.execute(sql_drop_temp)
        conn.execute(sql_insert)

        df.to_sql('temp_insert',con=conn,if_exists='append',index=False)

        conn.execute(""" set sql_safe_updates = 0; """)

        sql_duplicate_update_list = [ ] 
        for u in update_columns: 
            sql_duplicate_update = """{0}.{1} = temp_insert.{1}""".format(table_name,u)
            sql_duplicate_update_list.append(sql_duplicate_update)

        sql_duplicate_update = ','.join(sql_duplicate_update_list)

        sql_insert_update =  text("""insert ignore into {0} 
                                   select * from temp_insert 
                                   on duplicate key update 
                                   {1};""".format(table_name,sql_duplicate_update))

        conn.execute(sql_insert_update)
        conn.execute("""drop table temp_insert;""")

    else: #如果不存在唯一性约束,直接写入
        df.to_sql(table_name,con=conn,if_exists='append',index=False)

    conn.close()
    db.dispose()
