import re 
from collections import defaultdict 
from string import punctuation
import string 

from flashtext import KeywordProcessor

# keyword_processor = KeywordProcessor()
# for i in ['tests','testss','test','5G is(not','ok','100%','不可能吧','优势','在哪里','哪里']:
#     keyword_processor.add_keyword(i)

# print(keyword_processor.get_all_keywords())

# text = 'tests  100% are  do.ne testss/5G is(not ok'
# # text = '你觉得5G或者优势在哪里'
# kw_found = keyword_processor.extract_keywords(text)

# print(kw_found)

# exit()

def get_keyword_pat(keyword_list):
    keyword_list = sorted(set(keyword_list), key=len, reverse=True)

    keyword_pat = u'('+ '|'.join(keyword_list) + ')' 

    return keyword_pat

#数字模糊匹配函数,数字连着命中才算符合条件，即128对应的必须是128才算命中一个，命中12不算
def number_similarity(a,b, common_pattern):
    #该函数需要传入两者同时共存的内存模式,如果不填就简单地判断两个列表存在相同的匹配到的数字即可(y3 16G+128)可能会匹配到(3+16G)
    if common_pattern != '':
        a_match = re.search(common_pattern, a, flags=re.I)
        if a_match == None:
            return [ ]
        else:
            b_match = re.search(common_pattern, b, flags=re.I)
            #b是标准结果
            number_list_a = re.findall('\d+',a_match.group())
            number_list_b = re.findall('\d+',b_match.group())

            if len(number_list_a) > 0  :
                intersection = set(number_list_a) & set(number_list_b)
                if intersection and  set(number_list_b) == intersection:
                    return number_list_b

    return [ ] 
    
def check_syn_str_regex(string_a, string_b,special_syn_list):
    string_a = string_a.lower()
    string_b = string_b.lower()

    match_result_a = [ re.search(x, string_a, flags=re.I).group() for x in special_syn_list if re.search(x, string_a, flags=re.I) != None ]  
    match_result_b = [ re.search(x, string_b, flags=re.I).group() for x in special_syn_list if re.search(x, string_b, flags=re.I) != None ]

    intersection = set(match_result_a) & set(match_result_b)

    return len(intersection) == len(match_result_a)


def check_syn_str_regex_number(string_a, string_b, special_syn_list):
    #数字的不需要判断intersection,确认两者同时存在即可,即两者都存在 
    #x = check_syn_str_regex_number('(128G+8G)','8+28G',['(\d{1,4}[GB]?\+\d{1,4}[GB]?)|(\d{1,4}G{1}B?)']) -->True因为都符合
    string_a = string_a.lower()
    string_b = string_b.lower()

    match_result_a = [ True if re.search(x, string_a, flags=re.I) != None else False for x in special_syn_list ]  
    match_result_b = [ True if re.search(x, string_b, flags=re.I) != None else False for x in special_syn_list ]  

    if len(match_result_a) != len(match_result_b):
        return False 
    else:
        for a, b in zip(match_result_a,match_result_b):
            if a == True and b == True :
                return True
    return False


def strB2Q(ustring):
    """把字符串半角转全角"""
    ss = []
    for s in ustring:
        rstring = ""
        for uchar in s:
            inside_code = ord(uchar)
            # 全角空格直接转换
            if inside_code == 32:  
                inside_code = 12288
            # 全角字符（除空格）根据关系转化
            elif (inside_code >= 33 and inside_code <= 126):  
                inside_code += 65248
            rstring += chr(inside_code)
        ss.append(rstring)
    #顿号要转成逗号
    return ''.join(ss)


def strQ2B(ustring):
    """把字符串全角转半角"""
    ss = []
    for s in ustring:
        rstring = ""
        for uchar in s:
            inside_code = ord(uchar)
            if inside_code == 12288:  # 全角空格直接转换
                inside_code = 32
            elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
                inside_code -= 65248
            rstring += chr(inside_code)
        ss.append(rstring)

    return ''.join(ss)

def strQ2B(ustring):
    """把字符串全角转半角"""
    halfwidth_symbol = '!\"\"#$%&\'\'()*+,-./:;<=>?@[]_{|}~ '
    fullwidth_symbol = '！“”#$%&‘’（）*+，-。/：；《=》？@【】_{|}~ '
    translator = str.maketrans(fullwidth_symbol, halfwidth_symbol)

    ustring = ustring.translate(translator)
    return ustring


def strip_puntuations(input_string):
    #清空字符串两边的所有符号
    for x in punctuation:
        input_string = input_string.strip(x)
    return input_string

def replace_punctuations(input_string, replace_to_symbol=' ', exclude = [ ] ):
    #将字符串中的所有符号统一替换成空格
    if type(exclude) != list:
        exclude = [ exclude ]

    if type(input_string) != str:
        return input_string

    mapping_result = ''.join([replace_to_symbol if s not in exclude else s for s in string.punctuation])

    translator = str.maketrans(string.punctuation, mapping_result)
    input_string = input_string.translate(translator)
    return input_string 

def replace_multi_symbol(string, symbol):
    """把多个符号替换成单个，比如多个换行符 替换成 一个换行符,replace('\n\n','\n')并不能解决问题"""
    symbol_double = symbol + symbol
    while symbol_double in string:
        string = string.replace(symbol_double,symbol)
    return string

def symbol_to_spaces(string):
    string = replace_multi_symbol(replace_punctuations(strQ2B(string)).strip(),' ')
    return string

def normalize_punctuations(string):
    #用来做拆分（匹配是需要把符号全变成空格，拆分不能改变结果符号）
    #把string的所有符号标准化（全角全部统一转成半角，连续重复的符号变成单个,两边都不留空格或者换行符）
    #转成半角
    # string = strQ2B(string)
    # for s in punctuation:
    #     string = replace_multi_symbol(string, s)
    #     #确保两边没有特殊符号
    # string = string.strip()
    return string   

def replace_re_special(word):
    #注意\要写在前面，因为后面循环替换了\\进去
    for special_symbol in r'\-+()[]{}.*^$~|?^,':
        new_special_symbol = '\\' + special_symbol
        word = word.replace(special_symbol, new_special_symbol)
    return word


def search_en(combine_string):
    """提取出中英文混合字符串中的英文部分"""
    non_cn_pat = "[^\u4e00-\u9fa5]+"
    en_pat = ".*(\w)+.*"
    found_all = re.findall(non_cn_pat,combine_string)
    en_found_all = []

    if found_all :   #定位有英文的部分
        en_found_all = [re.search(en_pat,x).group() for x in found_all if re.search(en_pat,x) != None]

    if en_found_all :
        return en_found_all[0]
    return None

def partial_match_pct(short_str,long_str,special_syn=['pro','plus','max','youth']):
    """
    short_str : original string
    long_str : standard target string 
    匹配机型，不要放入两个一样的字符串，获取短字符串在长字符串中是否存在，并且占了多少个字符,
    从开头开始匹配,不替换空格不用in的方式查找，带有括号的机型匹配优先级最高，
    通常Y15S和Y15(4+64G)后者更容易被缩写成Y15"""
    #20200107 :检查长字符串是否包含有特殊字符
    def check_syn_str(new_str,special_syn):
        new_str = new_str.lower()
        match_syn_list = [x for x in special_syn if x in new_str]
        if match_syn_list:
            return match_syn_list[0]
        else:
            return 'NA'

    #暂时不懂怎么把IQOO 3 4G 优先匹配到 IQOO 3
    def match_sequence_str(short_string,long_string):
        #获取一个字符串对应另一个字符串匹配的字母
        # input 'iQOO 3 4G', IQOO 3 (5G)  --> IQOO 3
        # input 'iQOO 3 4G', IQOO 3 (4G)  --> IQOO 3 4
        #复制相同的一个
        long_counter = 0 
        temp_record = ''

        for s1 in short_string:
            for s2 in long_string[long_counter:] :
                if s1 == s2:
                    long_counter += 1 
                    temp_record += s2
                    break
        return temp_record 


    default_result = (0,long_str)

    if type(short_str) != str:
        return default_result

    short_str = short_str.strip().lower().replace('\n','').replace('  ',' ')
    new_long_str = long_str.strip().lower().replace('\n','').replace('  ',' ')

    #去掉换行符和多空格之后相等的话 直接返回长字符串
    if short_str == new_long_str:
        return (99,long_str)
    # #防止放入同一字符串
    # if short_str == new_long_str: 
    #   return default_result
    #括号和空格都要分割处理
    if '(' in new_long_str or '（' in new_long_str :
        new_long_str = new_long_str.replace('（','(').split('(')[0]
    # elif ' ' in new_long_str :
    #   new_long_str = new_long_str.split()[0]
    """ 匹配可能包含错误拼写，包括漏写，多写，错写的机型名, 机型名一般有NEX, S1， X3S, V1Max, V15Pro,
    允许的错别字条件是：不允许数字写错，不允许前面的字母写错，当字母大于等于3个时，允许漏写或者错写，多写2个字母,
    比如pro写成pr ,prou, max写成ma, V15Pro 写成 V15P（如果有V15P应该在之前就可以关联上，所以排除他是V15P的可能，
    更大可能是想写V15Pro）"""
    #从头开始匹配的时候，如果完整的short_str是准确拼写的，正常返回，如果有错别字，采用else以下的匹配方式
    match_short_str = ''
    #前两/三位字母相同,方便后面elif写条件
    first2_letter_equal = short_str[:2] == new_long_str[:3].split(' ')[0][:2]
    first3_letter_equal = short_str[:3] == new_long_str[:3].split(' ')[0][:3]
    #前3位字符串包含2个数字的情况 必须满足前3个字母相同,
    first3_letter_2num_short = re.search('[0-9]{2,}',short_str[:3]) != None
    first3_letter_2num_long =  re.search('[0-9]{2,}',new_long_str[:3]) != None

    first3_letter_less2num_short = not first3_letter_2num_short
    first3_letter_less2num_long = not first3_letter_2num_long

    #一个字符直接返回0,两个字符，去掉空格之后的前两个字符要完全相等,并且要确保长字符串里没有special_syn的字符
    if len(short_str) == 2 :  
        if not check_syn_str(new_long_str,special_syn):
            new_long_str = new_long_str.split(' ')[0].split('(')[0]
            if short_str == new_long_str:
                return (2/len(long_str),long_str)
            else:
                return default_result
        else:  #如果长字符串含有pro等字符 直接判断不匹配
            return default_result
    #至少出现3个字符,并且前两个字母(如果前两位是字母+数字，后面不再有数字),如果前3位包含2个数字,前3个字符要相同,规避掉V 15这种空格的情况 
    elif (len(short_str) >= 3 and first2_letter_equal) and \
         (
               (first3_letter_2num_short and first3_letter_2num_long and first3_letter_equal) \
            or (first3_letter_less2num_short and first3_letter_less2num_long and first2_letter_equal)
         ):

        for i in range(2,len(short_str)+1) :
            if short_str[:i].replace(' ','') in new_long_str.replace(' ',''):
                match_short_str = short_str[:i]
                continue        
        #优先计算匹配率大的字符串,并且为了实现区分V11i 优先匹配到V11而不是V11Pro的情况，而外加一个长字符串的比率
        #--计算结果相同(0.75, 'V11Pro')  (0.75, 'V11'),后面的sort比较难实现long_str的顺序排列
        if ' ' in long_str or '(' in long_str:
            long_str_bias = len(match_short_str)/len(long_str.split(' ')[0].split('(')[0])/100  #比例需要调小
            #如果去掉空格和( 符号之后的long_str_bias仍然相等，就将原来的标准机型全部去掉这些字符，对比整体全部的匹配度，做一个bias
            # long_str_bias += len(match_sequence_str(short_str,long_string)) / (len(short_str) + len(long_string)) / 1000  #比例更小
        else:  #没有出现空格和（ 的 不带pro的应该优先
            long_str_bias = len(match_short_str)/len(long_str)/100 + 0.00001 

        #确保短字符和长字符同时有或者同时没有special_syn_str 
        if check_syn_str(short_str,special_syn) == check_syn_str(new_long_str,special_syn):
            return (len(match_short_str)/len(short_str) + long_str_bias ,long_str)

    return default_result


def re_sub_replace(pattern,string,repace_symbols=('-','_'),group_name=1):
    """
    当re.sub(pattern,repl,string)内置的repl = "g<1>" 不能满足替换需求的时候,
    比如需要将group目标内的文字中的某个符号替换掉, 使用的时候要注意替换代码内的sub replace符号
    :param pattern : re pattern 
    :param string : original string 
    """
    def replace_match(matched):
        #re.sub会自动传入matched结果
        original_string = matched.group()
        #pattern支持填入group_name
        matched_string = matched.group(group_name)
        replace_string = original_string\
        .replace(matched_string,matched_string.replace(replace_symbols[0],replace_symbols[1]))
        return replace_string
    
    new_string = re.sub(pattern,replace_match,string,repace_symbols=('-','_'))
    return new_string


def re_findall_replace(pattern,string,replace_symbols=('-','_')):
    """
    当re.sub(pattern,repl,string)内置的repl = "g<1>" 不能满足替换需求的时候,
    比如需要将group目标内的文字中的某个符号替换掉,并且需要match多个group目标
    """
    matched_list = re.findall(pattern,string)
    new_string = string
    for mat in matched_list:
        new_string = new_string.replace(mat,mat.replace(replace_symbols[0],replace_symbols[1]))
    return new_string


def re_findall_sub(pattern,string,replace_symbols=('-','_')):
    """
    当re.sub(pattern,repl,string)内置的repl = "g<1>" 不能满足替换需求的时候,
    比如
    """
    matched_list = re.findall(pattern,string)
    new_string = string
    for mat in matched_list:
        new_string = new_string.replace(mat,mat.replace(replace_symbols[0],replace_symbols[1]))
    return new_string


def split_wrong_combine(pattern,sub_pattern,string):
    """
    需要处理爬虫换行符的问题, 抓取的时候把换行符去掉了，导致单词连接成错误的形式
    比如 axxBx, AxxxCONs, usagePROgreat, slow3.Great sunglass
     (注意 3app , 3PlayStore只能算是拼写错误，不需要拆分, LED3 拆不了，陷入无限循环)
    需要将两个错误合并的单词用换行符替换拆分,
    :param pattern : the original pattern that we want to find out
    :param sub_pattern : sub pattern to extract from pattern, 
    will be replaced with original + '\n' or '\n' + original  
    :param string : target string
    e.g
    pattern = '[A-Z]?[a-z]+[A-Z0-9]+[a-zA-Z]*' 
    sub_pattern = '[A-Z0-9]+[a-z]*'
    """
    #记录需要修改的部分
    new_string = string
    new_string_dict = defaultdict(str)
    matched_list = re.findall(pattern,new_string)
    if matched_list :
        for mat in matched_list:
            match_sub = re.search(sub_pattern,mat)
            #需要确保sub_pattern 能匹配出 pattern 匹配到的内容的部分目标
            if match_sub != None:
                match_sub = match_sub.group()
                replace_match = mat.replace(match_sub,'\n' + match_sub)
                #如果替换的是第一个单词，需要特殊处理。不要替换前面的符号为换行符，而是需要保持原来单词自带的 “空”
                #并且换行符 应该是加在第一个单词后面
                if [ x for x in replace_match].index('\n')  == 0:
                    replace_match = replace_match.replace('\n','')
                    replace_match  = mat.replace(match_sub, match_sub + '\n')

                replace_matched = split_wrong_combine(pattern,sub_pattern,replace_match)
                new_string_dict[mat] = replace_matched 
    else:
        return new_string

    for k,v in new_string_dict.items():
        new_string = new_string.replace(k,v)
        
    return new_string



#++++++++++++++++++以下废弃函数++++++++++++++++++++++++

# def standardize_country_by_cn_similarty(short_str, standard_str_list):
#     #处理中文国家缩写和完整国家名称无法匹配到的情况
#     standard_str_list = list(set([str(x).strip() for x in standard_str_list]))

#     standard_str_list = sorted(standard_str_list, key=len, reverse= False)
#     #通过前面字符串匹配 马来 -- > 马来西亚
#     standard_similarity_list = [ (s,1) if short_str in s else (s,0) for s in standard_str_list ]
#     if standard_similarity_list[0][1] > 0 :
#         return standard_similarity_list
#     else:
#         standard_similarity_list = [ ]
#         for ss in standard_str_list:
#             short_match_counter = 0
#             for ss_each_letter in ss:
#                 for s in short_str:
#                     if s == ss_each_letter:
#                         short_match_counter += 1

#             #至少能匹配上两个字
#             str_similarity =  short_match_counter / len(short_str) + short_match_counter / min([len(short_str),len(ss)])
#             if short_match_counter >= 2 :
#                 standard_similarity_list.append([ss,str_similarity])
#             else:
#                 standard_similarity_list.append([ss, 0 ])

#         standard_similarity_list = sorted(standard_similarity_list, key=lambda x:x[1],reverse=True)

#     return standard_similarity_list

