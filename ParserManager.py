from DeleteParser import DeleteParser
from CreateAlterParser import CreateAlterParser
from SelectParser import SelectParser
from UpdateParser import UpdateParser
import re
class ParserManager(object):
    
    @staticmethod
    def parser(string_need_parse):
        string_need_parse = string_need_parse.replace(" ", "").replace("\n", "")
        select_regular_expression = "^db.(([a-z]|[A-Z])+\w*.(find|aggregate|count|findOne|distinct))\(\S*\)(.(limit|explain|count|sort)\(\S*\))?(.skip\(\S\))?$"
        
        re_create_collection = """^db.createCollection\(("\D\w*")\);?"""
        special_one_regular_expression = """^db.(\D\w*).updateMany\((\S+)\);?"""
        alter_regular_expression = """^db.(\D\w*).(insertOne|insertMany|createIndex|drop)\((\S*)\);?"""
        update_regular_expression = """^db.(\D\w*).(updateOne|update|replaceOne|findOneAndReplace|findOneAndUpdate|findAndModify)\((\S+)\);?"""
        delete_regular_expression = "^db.(([a-z]|[A-Z])+\w*.(remove|deleteMany))\(\S*\)$"
        if re.match(select_regular_expression, string_need_parse) != None:
            print("select")
            return SelectParser(string_need_parse)
        elif re.match(update_regular_expression, string_need_parse) != None:
            print("update")
            return UpdateParser(string_need_parse)
        elif re.match(delete_regular_expression, string_need_parse) != None:
            print("delete")
            return DeleteParser(string_need_parse)
        elif re.match(alter_regular_expression, string_need_parse) != None or re.match(re_create_collection, string_need_parse) != None:
            print("alter")
            return CreateAlterParser(string_need_parse)
        elif re.match(special_one_regular_expression, string_need_parse) != None:
            
            try:
                UpdateParser(string_need_parse).parse()
                return UpdateParser(string_need_parse)
            except BaseException:
                try:
                    CreateAlterParser(string_need_parse).parse()
                    return CreateAlterParser(string_need_parse)
                except BaseException as error:
                    raise ValueError(str(error))
                        
        else:
            raise ValueError("input format error: %s"%string_need_parse)


