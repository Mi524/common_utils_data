import re 
from collections import defaultdict, Counter
from common_utils.regex_functions import replace_punctuations , replace_re_special, get_keyword_pat
from common_utils.os_functions import enter_exit
import xlrd 

def convert_key2list(word_dict):
    word_list = []
    for k, v in word_dict.items():
        for w in v :
            word_list.append(w)
    return word_list

def get_keyword_dict(path_list):
    #保存每个关键词列所需颜色的文字
    keyword_dict = defaultdict(set)
    #保存每个关键词列 类别的数字
    keyword_format_dict = defaultdict(str)

    if type(path_list) != list:
        path_list = [ path_list ]

    for path in path_list:
        wb = xlrd.open_workbook(path)
        #sheet name传入颜色
        sheet_names = wb.sheet_names()
        for sn in sheet_names:
            ws = wb.sheet_by_name(sn)
            #表头,根据表头获取应该写入红色还是蓝色，还是粗体
            header_list = []
            try:
                for x in ws.row(0):
                    if type(x.value) == str and x.value.strip() != '':
                        header = x.value.strip()
                    elif (type(x.value) == float or type(x.value) == int) :
                        header = str(x.value).rstrip('0').rstrip('.').strip()
                    else:
                        #为了防止两列中间隔一个空的表头单元格
                        header = None

                    if header != None:
                        header_list.append(header)

                if not header_list:
                    enter_exit(f'Error when reading keywords:\n{path}-"{sn}" should have at least one table header(keyword column names).')
            except IndexError:
                    enter_exit(f'Error when reading keywords:\n{path}-"{sn}" should have at least one table header(keyword column names).')

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

            #记录将每个颜色对应的关键词类
            for h in header_list:
                if h != None :
                    keyword_format_dict[h] = sn.strip().lower() 

        wb.release_resources()

    return keyword_dict, keyword_format_dict

def get_stopword_list(stopwords_path):

    stopword_list = defaultdict(int)

    with  open(stopwords_path,'r') as file:
        stopwords = file.read().splitlines() 

    for s in stopwords:
        if s.strip() != '':
            stopword_list[s.strip()] = 1

    return stopword_list

def process_text_eng(text, keyword_list=[], stopword_list=[], count_keywords_only = False):
    #仅适用英文
    
    #需要确保每个单词两边有空格
    keyword_list = [ ' ' + k + ' ' for k in keyword_list ]

    #加密邮件和数字
    text = encript_email_pat(text)
    text = encript_number_pat(text)

    text = replace_punctuations(text, replace_to_symbol=' ', exclude=['@']).lower().strip()

    text_list = text.split()
    #split空格，去掉多空格，再重新组合，为了匹配单词的两边空格，text两边也要加上空格
    text = ' ' + ' '.join(text_list) + ' '

    #keyword_dict先加入本身就是单词的词组，再组合成regex里面的格式拆分
    if not count_keywords_only and keyword_list:
        keyword_list = keyword_list + text_list

    #英文的处理，关键词搜索两边加上空格确保搜到的是英文
    keyword_pat = get_keyword_pat(keyword_list)

    if count_keywords_only:
        text_list = re.findall(keyword_pat, text, flags=re.I)
    else:
        text_list = re.split(keyword_pat,text,flags=re.I)
        text_list = [ x.lower() for x in text_list if x.strip() != '' ] 

    text_list = [ t.strip() for t in text_list ]

    if stopword_list:
        text_list = remove_stopwords(stopword_list, text_list)

    text_list = remove_numbers(text_list)
    text_list = remove_one_letter(text_list)

    text_list = [x.capitalize() for x in text_list]

    return text_list

def get_word_freq_dict(text, keyword_list, stopword_list, count_keywords_only=False, word_num=200):

    text_list = process_text_eng(text,keyword_list,stopword_list, count_keywords_only=count_keywords_only )

    word_count = dict(Counter(text_list).most_common(word_num))

    return word_count

def remove_stopwords(stopword_list, word_list):

    #转成字典再处理,尽量避免用 in list 的方式搜索
    stopword_dict = { s :1 for s in stopword_list }
    new_word_list = [ ]
    for w in word_list:
        if stopword_dict.get(w,None) == None:
            new_word_list.append(w.lower().strip())

    return new_word_list

def remove_numbers(word_list):
    word_list = [ x for x in word_list if x.isdigit() == False]
    return word_list




def remove_one_letter(word_list):
    word_list = [x for x in word_list if len(x) >= 2 ]
    return word_list

def encript_email_pat(text):

    if type(text) == str and text.strip() != '':
        email_like_pat = '([a-z0-9]{5,30})(@[^\u4e00-\u9fa5]+\.[a-z0-9]{2,15})'

        while True:
            if_match = re.search(email_like_pat, string=text)

            if if_match != None:
                text = re.sub(email_like_pat,repl='*****\g<2>',string=text)
            else:
                break

    return text

def encript_number_pat(text):
    if type(text) == str and text.strip() != '':
        result = ""

        number_counter = 0 

        new_text = str(text)
        for c in new_text:
            if c.isnumeric() == True:
                number_counter += 1
                if number_counter > 3 :
                    result += '*'
                else:
                    result += c
            else:
                number_counter = 0 
                result += c   
                
        #判断是否纯数字，如果是，后面需要rstrip('.0')
        if type(text) != str :
            return result.rstrip('.0')
        else:
            return result 
    else:
        return text
