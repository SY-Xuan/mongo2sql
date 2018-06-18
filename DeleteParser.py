#coding:utf-8
from Mongo2sqlParser import Mongo2sqlParser as Parser
import demjson
import re

class DeleteParser(Parser):
    def __init__(self, mongoString):
        Parser.__init__(self, mongoString)
       
    def parse(self):
        regular_expression = "^db.(([a-z]|[A-Z])+\w*.(remove|deleteMany))\(\S*\)$"#使用正则检查是否有语法错误，可能需要修改
        string_need_parse = self.mongoString.replace(" ", "").replace("\n", "") #去掉字符串中的空格与换行
        print(string_need_parse)
        dbname = ""
        operation = ""
        if re.match(regular_expression, string_need_parse) == None:
            raise ValueError("input error")
        else:
			#输入正常开始解析
            string_array = string_need_parse.split(".")#db, dbname, operation, json_part, maybehaveone

            dbname = string_array[1]
            operation, json_part = string_array[2].split("(")

		    #得到中间的{ age: { $lt: 925 } }
            json_part = json_part[:-1] #去掉结尾的括号

            #json部分是两个，一个或者零个json对象分别处理
            json_part = "[" + json_part + "]" #将其变成json的数组便于处理
            try:
                json_object = demjson.decode(json_part)
            #此处异常抛出可能不大好捕获了demjson的异常又抛出ValueError异常
            except demjson.JSONDecodeError as err:
                raise ValueError("json object input error " + str(err))

            #分别处理json对象是0个，1个的情况
            if(len(json_object) == 0):
                return self.format_Sql(dbname, operation)#只处理中间数据 db.people.deleteMany({})
            elif(len(json_object) == 1):
                #有where情况最复杂
                #将中间的json部分进行解析
                where_dic = json_object[0]
                self.parse_where_dic(where_dic)
                return self.format_Sql(dbname, operation, where_dic=where_dic)

    #sql = "DELETE" 
    def format_Sql(self, dbname, operation, where_dic=None):
        global sql
        sql = "DELETE" 
        
        sql = sql+ " FROM " + dbname
        #有WHERE部分的处理
        if where_dic != None:
            sql += " WHERE "
            for key in where_dic.keys():
                #首先处理or情况
                if key == "$or":
                    or_object_array = where_dic[key]
                    if isinstance(or_object_array, list):
                        #应该是数组类型否则为输入错误
                        for i in range(len(or_object_array)):
                            for (key, value) in or_object_array[i].items():    
                                if not isinstance(value, list):
                                    if isinstance(value, str):
                                        sql += key + " = " + "\"" + value + "\""
                                    else:
                                        sql += key + " = " + str(value)
                                else:
                                    if len(value) == 1:
                                        sql += key + str(value[0])
                                    else:
                                        sql += "("
                                        for j in range(len(value)):
                                            sql += key
                                            if j == (len(value) - 1):
                                                sql += str(value[j]) + ")"
                                            else:
                                                sql += str(value[j]) + " AND "
                            if i != (len(or_object_array) - 1):
                                sql += " OR "                
                        sql += " AND "            
                    else:
                        raise ValueError("or表达式对象错误，应为数组")
                else:
                    value = where_dic[key]
                    if not isinstance(value, list):
                        if isinstance(value, str):
                            sql += key + " = " + "\"" + value + "\""
                        else:
                            sql += key + " = " + str(value)         
                    else:     
                        if len(value) == 1:
                            sql += key + str(value[0])
                        else:
                            sql += "("
                            for j in range(len(value)):
                                sql += key
                                if j == (len(value) - 1):
                                    sql += str(value[j]) + ")"
                                else:
                                    sql += str(value[j]) + " AND "
                    sql += " AND "
            sql = sql[:-5]
        return sql

    #得到中间的{ age: { $lt: 25 } }   { $or: [ { status: "A" } ,{ age: 50 } ] }  
    def parse_where_dic(self, where_dic):
        update_dic = {}
        for key in where_dic.keys():
            if key == "$or":
                or_object_array = where_dic[key]
                if isinstance(or_object_array, list):
                    #应该是数组类型否则为输入错误
                    for obj in or_object_array:
                        for objkey in obj.keys():
                            #处理为下面格式
                            #{age: [<20, >=50]}
                            #如果为jsonobject则是大于小于等情况
                            if isinstance(obj[objkey], dict):
                                operation_list = []
                                for (key, value) in obj[objkey].items():
                                    if objkey == "$gt":
                                        if isinstance(value, str):
                                            operation_list.append(" > " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" > " + str(value))
                                    elif objkey == "$lt":
                                        if isinstance(value, str):
                                            operation_list.append(" < " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" < " + str(value))
                                    elif objkey == "$gte":
                                        if isinstance(value, str):
                                            operation_list.append(" >= " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" >= " + str(value))
                                    elif objkey == "$lte":
                                        if isinstance(value, str):
                                            operation_list.append(" <= " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" <= " + str(value))
                                    elif objkey == "$ne":
                                        if isinstance(value, str):
                                            operation_list.append(" != " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" != " + str(value))
                                    else:
                                        raise ValueError("条件操作符错误")
                                obj[objkey] = operation_list
                else:
                    raise ValueError("or表达式对象错误，应为数组")
            else:
                if isinstance(where_dic[key], dict):
                    #条件操作符情况与上式同理 { age: { $lt: 25 } }   
                    operation_list = []
                    for (objkey, value) in where_dic[key].items():
                        if objkey == "$gt":
                            if isinstance(value, str):
                                operation_list.append(" > " + "\"" + value + "\"")
                            else:
                                operation_list.append(" > " + str(value))
                        elif objkey == "$lt":
                            if isinstance(value, str):
                                operation_list.append(" < " + "\"" + value + "\"")
                            else:
                                operation_list.append(" < " + str(value))
                        elif objkey == "$gte":
                            if isinstance(value, str):
                                operation_list.append(" >= " + "\"" + value + "\"")
                            else:
                                operation_list.append(" >= " + str(value))
                        elif objkey == "$lte":
                            if isinstance(value, str):
                                operation_list.append(" <= " + "\"" + value + "\"")
                            else:
                                operation_list.append(" <= " + str(value))
                        elif objkey == "$ne":
                            if isinstance(value, str):
                                operation_list.append(" != " + "\"" + value + "\"")
                            else:
                                operation_list.append(" != " + str(value))
                        else:
                            raise ValueError("条件操作符错误")
                    update_dic.update({ key:operation_list })
        for key, value in update_dic.items():
            where_dic[key] = value


# parser = SelectParser("db.people.find()")
# parser = SelectParser("db.people.findOne()")
# try:
# print(parser.parse())
# except ValueError as err:
#     print(str(err))
with open("mongodb_delete_input.txt") as f:
    with open("delete_output.txt", "w") as wf:
        for line in f.readlines():
            parser = DeleteParser(line)
            wf.write(parser.parse())
            wf.write("\n")
