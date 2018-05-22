from Mongo2sqlParser import Mongo2sqlParser as Parser
import demjson
import re
class SelectParser(Parser):
    def __init__(self, mongoString):
        Parser.__init__(self, mongoString)
       

    def parse(self):
        regular_expression = "^db.(([a-z]|[A-Z])+\w*.(find|aggregate|count|findOne|distinct))\(\S*\)(.(limit|explain|count|sort)\(\S*\))?(.skip\(\S\))?$"#使用正则检查是否有语法错误，可能需要修改
        string_need_parse = self.mongoString.replace(" ", "").replace("\n", "") #去掉字符串中的空格与换行
        print(string_need_parse)
        dbname = ""
        operation = ""
        
        if re.match(regular_expression, string_need_parse) == None:
            raise ValueError("input error")
        else:
            #输入正常开始解析
            string_array = string_need_parse.split(".")#db, dbname, operation, json_part, maybehaveone
            #此处没考虑json对象中可能出现"."的情况，以后再改进
            
            
            dbname = string_array[1]
            operation, json_part = string_array[2].split("(")
            if len(string_array) == 4:
                #count, limitone, skip, explain, sort
                #find count 情况
                if operation == "find" and string_array[3] == "count()":
                    operation = "count"
                #find limit 情况
                elif operation == "find" and string_array[3].split("(")[0] == "limit":
                    operation = string_array[3]
                #find explain情况
                elif operation == "find" and string_array[3] == "explain()":
                    operation = "explain"
                #find sort情况
                elif operation == "find" and string_array[3].split("(")[0] == "sort":
                    operation = string_array[3]
            elif len(string_array) == 5:
                #limit skip
                if operation == "find" and string_array[3].split("(")[0] == "limit" and string_array[4].split("(")[0] == "skip":
                    try:
                        skipnumber = int(string_array[4].split("(")[1][:-1])
                    except:
                        raise ValueError("skip para should be a number")
                    return self.format_Sql(dbname, string_array[3]) + " SKIP " + str(skipnumber)
                else:
                    raise ValueError("cannot handle operation")
            
            if operation == "distinct":
                return self.format_Sql(dbname, string_array[2])
            
                
            json_part = json_part[:-1] #去掉结尾的括号
            if operation == "aggregate":
                i = 0
                
                json_object = demjson.decode(json_part)
                if isinstance(json_object, list):
                    if len(json_object) == 1:
                        for (key, value) in json_object[0].items():
                            if key == "$group":
                                for (subkey,subvalue) in value.items():
                                    if i == 1:
                                        raise ValueError("group number can only be 1")
                                    if subkey == "_id":
                                        if subvalue[0] == "$":
                                            return self.format_Sql(dbname, "distinct(\"" + subvalue[1:] + "\"" + ")")
                                        else:
                                            raise ValueError("aggregate group value error")
                                    else:
                                        raise ValueError("aggregate group key should be _id")
                                    i += 1
                            else:
                                raise ValueError("aggregate key should be $group other condition cannot handle")
                        else:
                            raise ValueError("aggregate operation cannot handle")
                else:
                    raise ValueError("aggregate need a list")

            #json部分是两个，一个或者零个json对象分别处理
            json_part = "[" + json_part + "]" #将其变成json的数组便于处理
            try:
                json_object = demjson.decode(json_part)
            #此处异常抛出可能不大好捕获了demjson的异常又抛出ValueError异常
            except demjson.JSONDecodeError as err:
                raise ValueError("json object input error " + str(err))
            #分别处理json对象是0个，1个，2个的情况
            if(len(json_object) == 0):
                if operation == "explain":
                    return "EXPLAIN " + self.format_Sql(dbname, "find")
                return self.format_Sql(dbname, operation)
            elif(len(json_object) == 1):
                #有where情况最复杂
                where_dic = json_object[0]
                if operation == "count":
                    
                    return self.format_Sql(dbname, operation, where_dic=where_dic)
                elif operation.split("(")[0] == "limit":
                    raise ValueError("limit operation find cannot include jsonobject")
                else:
                    self.parse_where_dic(where_dic)
                    if operation == "explain":
                        return "EXPLAIN " + self.format_Sql(dbname, "find") 
                    return self.format_Sql(dbname, operation, where_dic=where_dic)
            elif(len(json_object) == 2):
                where_dic = json_object[0]
                select_dic = json_object[1]
                #处理where的情况
                #若where_dic为空则不处理
                if operation == "count":
                    raise ValueError("count should not have two jsonobject")
                elif operation.split("(")[0] == "limit":
                    raise ValueError("limit operation find cannot include jsonobject")
                if len(where_dic.keys()) == 0:
                    where_dic = None
                else:
                    self.parse_where_dic(where_dic)
                #_________________
                id_flag = True #该值决定是否有id
                select_array = []
                for key in select_dic.keys():
                    if key == "_id":
                        if select_dic[key] == 0:
                            id_flag = False
                        else:
                            #值只可以为0否则抛出异常
                            raise ValueError("_id has a wrong value")
                    else:
                        if select_dic[key] != 1:
                            raise ValueError("select value has a wrong value(only can be 1)")
                        else:
                            select_array.append(key)
                if id_flag:
                    select_array.append("id")
                if operation == "explain":
                    return "EXPLAIN " + self.format_Sql(dbname, "find")
                return self.format_Sql(dbname, operation, where_dic=where_dic, select_array=select_array)


    def format_Sql(self, dbname, operation, where_dic=None, select_array=[]):
        sql = "SELECT "
        sort_part = ""
        if operation.split("(")[0] == "sort":
            try:
                json_part = operation.split("(")[1][:-1]
            except IndexError:
                raise ValueError("sort part need jsonobject")
            i = 0
            try:
                json_object = demjson.decode(json_part)
            #此处异常抛出可能不大好捕获了demjson的异常又抛出ValueError异常
            except demjson.JSONDecodeError as err:
                raise ValueError("json object input error " + str(err))
            for (key, value) in json_object.items():
                if i == 1:
                    raise ValueError("sort part cannot has too many jsonobject")
                sort_part += " ORDER BY " + key
                if str(value) == "1":
                    sort_part += " ASC"
                elif str(value) == "-1":
                    sort_part += " DSEC"
                else:
                    raise ValueError("sort value should be 1 or -1")
                i += 1
            operation = "find"
                

        if operation == "find":
            if len(select_array) == 0:
                sql += "*"
            else:
                for i in range(len(select_array)):
                    sql += select_array[i]
                    if i != (len(select_array) - 1):
                        sql += ", "
            sql += " FROM " + dbname
            #WHERE部分有待补充
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
                                
                        
                        

        elif operation.split("(")[0] == "distinct":
            item = operation.split("(")[1][:-1]
            print(item)
            if (item[0] == "\'" or item[0] == "\"") and (item[-1] == "\'" or item[-1] == "\""):
                
                item = item[1:-1]
                if len(item) == 0:
                    raise ValueError("distinct is empty")
                else:
                    sql += " DISTINCT" + "(" + item + ")" + " FROM " + dbname  
            else:
                raise ValueError("distinct format error")                      

        elif operation == "findOne":
            sql += "* " + "FROM " + dbname + " LIMIT 1 "
        elif operation.split("(")[0] == "limit":
            number = operation.split("(")[1]
            if len(number) == 1:
                raise ValueError("limit must include a number")
            sql += "* " + "FROM " + dbname + " LIMIT " + operation.split("(")[1][:-1]  
        elif operation == "count":
            #此处有特殊情况暂未考虑
            if where_dic == None:
                sql += "COUNT(*) " + "FROM " + dbname
            else:
                
                is_exists = False
                for (key, value) in where_dic.items():
                    #若是$exist的情况则单独处理
                    if isinstance(value, dict):
                        for (inkey, invalue) in value.items():
                            if inkey == "$exists":
                                
                                if invalue:
                                    sql += "COUNT(" + key + ")" + " FROM " + dbname
                                    is_exists = True
                                else:
                                    raise ValueError("$exists value needs to be true")
                if not is_exists:
                    self.parse_where_dic(where_dic)
                    print(where_dic)
                    sql += "COUNT(*)" + "FROM " + dbname + " WHERE "
                    for (key, value) in where_dic.items():
                        
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


        return sql + sort_part
            
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
                                    if key == "$gt":
                                        if isinstance(value, str):
                                            operation_list.append(" > " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" > " + str(value))
                                    elif key == "$lt":
                                        if isinstance(value, str):
                                            operation_list.append(" < " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" < " + str(value))
                                    elif key == "$gte":
                                        if isinstance(value, str):
                                            operation_list.append(" >= " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" >= " + str(value))
                                    elif key == "$lte":
                                        if isinstance(value, str):
                                            operation_list.append(" <= " + "\"" + value + "\"")
                                        else:
                                            operation_list.append(" <= " + str(value))
                                    elif key == "$ne":
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
                    #条件操作符情况与上式同理
                    
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
with open("mongodb_input.txt") as f:
    with open("output.txt", "w") as wf:
        for line in f.readlines():
            parser = SelectParser(line)
            wf.write(parser.parse())
            wf.write("\n")
    
