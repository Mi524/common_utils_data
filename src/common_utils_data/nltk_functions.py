from nltk.corpus import words as nltk_words
from collections import defaultdict

def gen_valid_wordlist(valid_word_len):
	"""
	创建有效的单词列表,全部返回小写
	"""
	word_list = [ x.lower() for x in nltk_words.words() if len(x) >= valid_word_len ]
	return word_list 


def convert_list2dict(word_list):
	"""
	把列表写成字典
	"""
	word_dict = defaultdict(int)
	for w in word_list:
		word_dict[w] = 1
	return word_dict


