'''
@author myp
mongodb 有多种方法对表进行更新，UpdateParser.py解析了以下7种：
db.collection.updateOne(filter,update,options)
db.collection.updateMany(filter,update,options)
db.collection.update(query,update,options)
db.collection.replaceOne(filter,replacement,options)
db.collection.findOneAndReplace(filter,replacement,options)
db.collection.findOneAndUpdate(filter,replacement,options)
db.collection.findAndModify(document)
方法的使用详见：
https://docs.mongodb.com/manual/reference/update-methods/#additional-updates
'''

from Mongo2sqlParser import Mongo2sqlParser as Parser
import demjson
import re
from parse_condition import parse_condition
from parse_update_operators import parse_update_opeators


class UpdateParser(Parser):
    def __init__(self, mongoString):
        Parser.__init__(self, mongoString)

    # 该函数解析$setOnInsert,生成完整Sql语句
    def generateInsert(self, collection, sql_set, sql_where=''):
        # 提取operators返回的field和value
        sql_set_list = sql_set.split(';')
        set_fields = []
        set_values = []
        update_list = []  # 若冲突时，需要更新的field=value,要排除$setOnInsert中的field和value
        for set in sql_set_list:
            if "INSERT" in set:
                field_value_list = re.findall(r'[(](.*?)[)]', sql_set.replace(' ', ''))  # list,提取通过操作符得到的fields和values
                set_fields.extend(field_value_list[0].split(','))
                set_values.extend(field_value_list[1].split(','))
            elif "SET" in set:
                otherfield_values = set[3:].split(',')
                update_list = otherfield_values
                for otherfield_value in otherfield_values:
                    set_fields.append(otherfield_value.split('=')[0])
                    set_values.append(otherfield_value.split('=')[1])
        if len(update_list) == 0:
            return "Error: the input format in wrong."


        # 提取condition中的field和value
        where_feilds = []
        where_values = []
        result = ''
        if sql_where != 'TRUE':
            if '(' not in sql_where:
                sql_where = '(' + sql_where + ')'
            sql_wheres = re.findall(r'[(](.*?)[)]', sql_where.replace(' ', ''))  # list，提取解析出来的condition中括号内的内容

            for index in range(0, len(sql_wheres)):
                feild_value_list = re.findall(r'(\w*)[=](.*)', sql_wheres[index].replace(' ', ''))  # list,提取等号左右两边的列名和值
                if '' not in feild_value_list[0]:
                    where_feilds.append(feild_value_list[0][0])
                    where_values.append(feild_value_list[0][1])
            result = 'INSERT INTO {} ({},{}) VALUES ({},{}) ON CONFLICT({}) DO UPDATE SET {}'.format(collection, \
                                                                                                     ','.join(
                                                                                                         where_feilds), \
                                                                                                     ','.join(
                                                                                                         set_fields),
                                                                                                     ','.join(
                                                                                                         where_values),
                                                                                                     ','.join(
                                                                                                         set_values), \
                                                                                                     ','.join(
                                                                                                         where_feilds),
                                                                                                     ','.join(
                                                                                                         update_list))
        else:
            result = 'INSERT INTO {} ({}) VALUES ({})'.format(collection, ','.join(set_fields), ','.join(set_values))
        return result.replace('\"', '\'')

    # 该函数解析parse_update_operators()返回的带有'SET'的操作,生成完整Sql语句
    def generateUpdate(self, collection, sql_set, sql_where=''):
        result=''
        if sql_where != 'TRUE':
            if 'WHERE' in sql_set:
                result="UPDATE {} {} AND {} ".format(collection, sql_set, sql_where)

            else:
                result= "UPDATE {} {} WHERE {} ".format(collection, sql_set, sql_where)
        else:
            result= "UPDATE {} ".format(collection) + sql_set
        return result.replace('\"','\'')

    # 该函数针parse_update_operators()返回的带有'ALTER'的操作,生成完整Sql语句
    def generateAlter(self, collection, sql_set, sql_where=''):
        return sql_set.replace('collection', collection).replace('\"','\'')

    # 解析updateOne()
    def parse_updateOne(self, collection, arg):
        args = demjson.decode('[' + arg + ']')
        conditions = args[0]
        operators = args[1]
        if len(operators) == 0:
            return 'Error: nothing will happen!'

        if '$rename' in list(operators) or '$unset' in list(operators):
            return "Error:cannot use $rename or $unset in updateone()"

        sql_set = parse_update_opeators(demjson.encode(operators))
        sql_where = parse_condition(conditions)
        if 'Error' in sql_set:
            return sql_set
        if '$setOnInsert' not in list(operators):
            return self.generateUpdate(collection, sql_set, sql_where) + ' LIMIT 1 ;'
        elif len(args) == 3 and len(args[2]) == 1 and list(args[2])[0] == 'upsert' and args[2]['upsert'] is True \
                and len(sql_set.split(',')) != 1:
            return self.generateInsert(collection, sql_set, sql_where) + ';'
        else:
            return '''Error:the input format is wrong!'''

    # 解析updateMany()
    def parse_updateMany(self, collection, arg):
        args = demjson.decode('[' + arg + ']')
        conditions = args[0]
        operators = args[1]
        if len(operators) == 0:
            return 'Error: no operator will happen!'

        sql_set = parse_update_opeators(demjson.encode(operators))
        sql_where = parse_condition(conditions)
        if 'Error' in sql_set:
            return sql_set
        if 'ALTER' not in sql_set and 'INSERT' not in sql_set:
            return self.generateUpdate(collection, sql_set, sql_where) + ';'
        elif 'ALTER' in sql_set:
            if sql_where == 'TRUE':
                return self.generateAlter(collection, sql_set, sql_where) + ';'
            else:
                return "Error:when there is a $unset or $rename operator ,query should be empty."
        elif 'INSERT' in sql_set and len(args) == 3 and len(args[2]) == 1 and list(args[2])[0] == 'upsert' and args[2][
            'upsert'] is True \
                and len(sql_set.split(',')) != 1:
            return self.generateInsert(collection, sql_set, sql_where) + ';'
        else:
            return '''Error:the input format is wrong!'''

    # 解析update()
    def parse_update(self, collection, arg):
        args = demjson.decode('[' + arg + ']')
        conditions = args[0]
        operators = args[1]
        if len(operators) == 0:
            return 'Error: no operator will happen!'

        sql_set = parse_update_opeators(demjson.encode(operators))
        sql_where = parse_condition(conditions)
        result = ''
        if 'Error' in sql_set:
            return sql_set
        if 'ALTER' not in sql_set and 'INSERT' not in sql_set:
            if len(args) == 3 and 'multi' in list(args[2]) and args[2][
                'multi'] is True:
                return self.generateUpdate(collection, sql_set, sql_where) + ';'
            else:
                return self.generateUpdate(collection, sql_set, sql_where) + ' LIMIT 1;'

        elif 'ALTER' in sql_set and len(args) == 3 and 'multi' in list(args[2]) and args[2][
            'multi'] is True:
            if sql_where == 'TRUE':
                return self.generateAlter(collection, sql_set, sql_where) + ';'
            else:
                return "Error:when there is a $unset or $rename operator ,query should be empty."
        elif 'INSERT' in sql_set and len(args) == 3 and 'upsert' in list(args[2]) and args[2][
            'upsert'] is True and len(sql_set.split(',')) != 1:
            return self.generateInsert(collection, sql_set, sql_where)

        else:
            return '''Error:the input format is wrong!'''

    # 解析replaceOne()和findOneAndReplace（）
    def parse_replaceOne(self, collection, arg):
        args = demjson.decode('[' + arg + ']')
        conditions = args[0]
        operators = args[1]
        if len(operators) == 0:
            return 'Error: no operator will happen!'
        if '$currentDate' in list(operators) or '$inc' in list(operators) or '$min' in list(operators) or \
                '$max' in list(operators) or '$mul' in list(operators) or '$rename' in list(operators) or \
                '$set' in list(operators) or '$setOnInsert' in list(operators) or '$unset' in list(operators):
            return 'Error:replaceOne() cannot use update operators in mongodb.'
        else:
            sql_set = parse_update_opeators(demjson.encode(operators))
            sql_where = parse_condition(conditions)
            if 'Error' in sql_set:
                return sql_set
            if 'SET' in sql_set:
                return self.generateUpdate(collection, sql_set, sql_where) + ' LIMIT 1;'
            else:
                return 'Error!'

    def parse_findOneAndUpdate(self, collection, arg):
        args = demjson.decode('[' + arg + ']')
        operators = args[1]
        for operator in list(operators):
            if '$' not in operator:
                return 'Error: only update operators can be used in findOneAndUpdate()!'
        return self.parse_updateOne(collection, arg)

    def parse_findAndModify(self, collection, arg):
        args = arg.split(',')
        remove = 0
        update = 0
        upsert = 0
        sql_set = ''
        sql_where = ''
        for single_arg in args:
            index = single_arg.find(':')
            if 'query' in single_arg:
                sql_where = parse_condition(demjson.decode(single_arg[index + 1:]))
            if 'remove' in single_arg and single_arg[index + 1:] == 'true':
                remove = 1
            if 'update' in single_arg:
                sql_set = parse_update_opeators('{}'.format(single_arg[index + 1:]))
                update = 1
            if 'upsert' in single_arg:
                upsert = 1

        if remove == 1 and update == 0:
            if sql_where != '':
                result = 'DELETE FROM {} WHERE {} LIMIT 1'.format(collection, sql_where)
            else:
                result = 'DELETE FROM {} LIMIT 1'.format(collection)


        elif remove == 0 and update == 1 and sql_set != '':
            if 'Error' in sql_set:
                return sql_set
            if "ALTER" in sql_set:
                return '''Error:$unset and $rename cannot be used in findAndModify().'''
            elif 'Insert' in sql_set and upsert == 1:
                result = self.generateInsert(collection, sql_set, sql_where)
            else:
                result = self.generateUpdate(collection, sql_set, sql_where)

        return result + ';'

    def parse(self):
        re_updateOne = """^db.(\D\w*).updateOne\((\S+)\);?"""
        re_updateMany = """^db.(\D\w*).updateMany\((\S+)\);?"""
        re_update = """^db.(\D\w*).update\((\S+)\);?"""
        re_replaceOne = """^db.(\D\w*).replaceOne\((\S+)\);?"""
        re_findOneAndReplace = """^db.(\D\w*).findOneAndReplace\((\S+)\);?"""
        re_findOneAndUpdate = """^db.(\D\w*).findOneAndUpdate\((\S+)\);?"""
        re_findAndModify = """^db.(\D\w*).findAndModify\((\S+)\);?"""

        # 去掉字符串中的空格与换行
        string_need_parse = self.mongoString.replace(" ", "").replace("\n", "")
        if re.match(re_updateOne, string_need_parse):
            m = re.search(re_updateOne, string_need_parse)
            return self.parse_updateOne(m.group(1), m.group(2))
        elif re.match(re_updateMany, string_need_parse):
            m = re.search(re_updateMany, string_need_parse)
            return self.parse_updateMany(m.group(1), m.group(2))
        elif re.match(re_update, string_need_parse):
            m = re.search(re_update, string_need_parse)
            return self.parse_update(m.group(1), m.group(2))
        elif re.match(re_replaceOne, string_need_parse):
            m = re.search(re_replaceOne, string_need_parse)
            return self.parse_replaceOne(m.group(1), m.group(2))
        elif re.match(re_findOneAndReplace, string_need_parse):
            m = re.search(re_findOneAndReplace, string_need_parse)
            return self.parse_replaceOne(m.group(1), m.group(2))
        elif re.match(re_findOneAndUpdate, string_need_parse):
            m = re.search(re_findOneAndUpdate, string_need_parse)
            return self.parse_findOneAndUpdate(m.group(1), m.group(2))
        elif re.match(re_findAndModify, string_need_parse):
            m = re.search(re_findAndModify, string_need_parse)
            return self.parse_findAndModify(m.group(1), m.group(2))

        else:
            raise ValueError("re match failed:%s" % string_need_parse)


with open("update_input.txt") as f:
    with open("update_output.txt", "w") as wf:
        for line in f.readlines():
            if len(line.strip()) > 0:
                parser = UpdateParser(line)
                ss = parser.parse()
                print(ss)
                wf.write(ss)
                wf.write("\n")
