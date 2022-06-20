import json


class DfDict(dict):
    #辅助group_word_sum创建的类
    def sum(self ):
        return sum(self.values())

    def __add__(self,other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                result[k] = v + other[k]
            else:
                result[k] = v

        for k, v in other.items():
            if k not in self.keys():
                result[k] = v

        return result

    def __sub__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                result[k] = v - other[k]
            else:
                result[k] = v

        for k, v in other.items():
            if k not in self.keys():
                result[k] = -v

        return result

    def __mul__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                result[k] = v * other[k]
            else:
                result[k] = 0

        for k, v in other.items():
            if k not in self.keys():
                result[k] = 0

        return result

    def __truediv__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                if other[k] != 0:  #如果为0 不输出结果
                    result[k] = v / other[k]
            else:
                result[k] = float(0)

        for k, v in other.items():
            if k not in self.keys():
                result[k] = float(0)

        return result

    def __floordiv__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                if other[k] != 0:  #如果为0 不输出结果
                    result[k] = v // other[k]
            else:
                result[k] = float(0)

        for k, v in other.items():
            if k not in self.keys():
                result[k] = float(0)

        return result

    def __mod__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                if other[k] != 0:  #如果为0 不输出结果
                    result[k] = v % other[k]
            else:
                result[k] = 0

        for k, v in other.items():
            if k not in self.keys():
                result[k] = float(0)

        return result

    def __pow__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                if other[k] != 0:  #如果为0 不输出结果
                    result[k] = v ** other[k]
            else:
                result[k] = 0

        for k, v in other.items():
            if k not in self.keys():
                result[k] = 0

        return result

    def __and__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, 0) != 0 and v != 0 :
                result[k] = True
            else:
                result[k] = False

        for k, v in other.items():
            if k not in self.keys() :
                result[k] = False
            else:
            	result[k] == True

        return result

    def __or__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, 0) == 0 and k == 0 :
                result[k] = False
            else:
                result[k] = True

        for k, v in other.items():
            if k not in self.keys() and k == 0 :
                result[k] = False 
            else:
            	result[k] = True

        return result

    def __xor__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if (other.get(k, 0) == 0 and k == 0) or (other.get(k,0) != 0 and k != 0 ) :
                result[k] = True
            else:
                result[k] = False

        for k, v in other.items():
            if (k not in self.keys() and k == 0) :
                result[k] = False 
            else:
            	result[k] = True

        return result

    def __pow__(self, other):
        result = DfDict({ })
        for k, v  in self.items():
            if other.get(k, None) != None:
                if other[k] != 0:  #如果为0 不输出结果
                    result[k] = v ** other[k]
            else:
                result[k] = 0

        for k, v in other.items():
            if k not in self.keys():
                result[k] = 0

        return result

    def __repr__(self):
        return json.dumps(self)

    def __str__(self):
        return json.dumps(self)
