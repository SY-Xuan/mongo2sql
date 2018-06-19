"""
mongodb selector共有8类，详情见下面的链接
https://docs.mongodb.com/manual/reference/operator/query/#query-selectors
这里的 parse_condition 函数可以解析其中的两类：comparison和logical.
这两类包含了比较运算和逻辑连接符。
"""
import demjson
import re
from numbers import Number

def format_mongodb_value(v):
    if type(v)==str:
        if v=='$NULL':
            return 'NULL'
        return "'"+v+"'"
    elif isinstance(v, Number):
        return str(v)
    elif isinstance(v, list):
        return '('+', '.join([format_mongodb_value(vv) for vv in v])+')'
    else:
        raise ValueError('unknown type')

def dict_to_list(dt):
    dict_list = list()
    for k,v in dt.items():
        dict_list.append({k:v})
    return dict_list

comparison_op={
        '$eq':'=',
        '$gt':'>',
        '$gte': '>=',
        '$lt': '<',
        '$lte': '<=',
        '$ne': '!=',
        '$in': 'in',
        '$nin': 'not in',
        }


# condition是demjson.decode(...)返回的dict
# parse_condition的使用方法见下方的test_parse_condition函数
def parse_condition(condition, col_name=''):
    if len(condition) ==0:
        return 'TRUE'
    if len(condition) >1: # 多个条件，and
        v = ['(%s)' % parse_condition(cond, col_name) \
                for cond in dict_to_list(condition)]
        return ' AND '.join(v)
    head = list(condition.keys())[0] # 唯一的key

    if head in ['$and', '$not', '$nor', '$or']:
        logic = ' %s ' % head[1:]
        logic = logic.upper()
        v = ['(%s)' % parse_condition(cond, col_name) \
                for cond in condition[head]]
        return logic.join(v)
    elif head in comparison_op:
        value = condition[head]
        if col_name == '':
            raise ValueError('col_name must not be empty here')
        return col_name + ' '+ comparison_op[head].upper() +' '+ \
                format_mongodb_value(value)
    else: # head is col name.
        if type(condition[head])==dict:
            return parse_condition(condition[head], head)
        else:
            return  head + ' = '+ format_mongodb_value(condition[head])

def test_parse_condition():
    def pp(s):
        print(s)
        print(parse_condition(demjson.decode(s)))
    #pp( '''{name : "John", age: 4}''' )
    #pp( '''{$or : [{height: {$gt: 10}}, {age: {$lte: 4}}] }''' )
    #pp( '''{name : {$in : ["John", "Peter"]}}''' )
    or11 = '''{$or : [{height: {$gt: 10}}, {age: {$lte: 4}}] }''' 
    and11 = '''{name : "John", age: 4}'''
    and12 = '''{name : {$in : ["John", "Peter"]}} '''
    or2= '''{$or: [%s, %s, %s]}'''%(or11, and11, and12)
    pp(or2)

if __name__=='__main__':
    test_parse_condition()

