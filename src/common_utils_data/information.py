import re  

country_map_dict={
"MY":"马来西亚",
"MM":"缅甸",
"TH":"泰国",
"IN":"印度",
"ID":"印尼",
"PH":"菲律宾",
"VN":"越南",
"CHN":"中国",
"PK":"巴基斯坦",
"KH":"柬埔寨",
"HK":"香港",
"LK":"斯里兰卡",
"RU":"俄罗斯",
"NP":"尼泊尔",
"BD":"孟加拉",
"TW":"台湾",
"SG":"新加坡",
"KZ":"哈萨克斯坦",
"MA":"摩洛哥",
"AU":"澳大利亚",
"AE":"阿联酋",
"OM":"阿曼",
"QA":"卡塔尔",
"KW":"科威特",
"YE":"也门",
"4C":"中东4C",
"LB":"黎巴嫩",
"JO":"约旦",
"NG":"尼日利亚",
"SA":"沙特阿拉伯",
"UA":"乌克兰",
"PL":"波兰",
"MO":"澳门",
"BT":"不丹",
"BN":"文莱",
"LA":"老挝",
"EG":"埃及",
"CH":"瑞士",
"KE":"肯尼亚"
}

engine_text = "mysql://root:00000000@localhost:3306/web_data?charset=utf8mb4"

# range_counter = 0 
# for i in range(0,50,10):
# 	range_counter += 1 
# 	print(i)
# 	if range_counter == 50 / 10 :
# 		if i != 50:
# 			print(i)