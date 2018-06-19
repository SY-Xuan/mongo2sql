''' parser for mongodb create&alter statements.
支持的SQL数据类型 : number ,string, date

mongodb里的 insertOne 和 insertMany 在这里有完整的实现。
mongodb里的 updateMany 在这里实现了增加/删除列的功能，可以翻译到SQL的UPDATE语句（包含WHERE），但是不知道到UPDATE的翻译是否完整。
'''

from Mongo2sqlParser import Mongo2sqlParser as Parser
import demjson
import re
from numbers import Number
from parse_condition import parse_condition, format_mongodb_value

def mongo_type_name(v):
    if isinstance(v, bool):
        return 'boolean'
    elif isinstance(v, Number):
        return 'Number'
    elif isinstance(v, str):
        if v=='$$$DATE':
            return 'DATE'
        else:
            return 'Varchar(30)'
    else:
        raise ValueError()
        return 'UnknownType'

class CreateAlterParser(Parser):
    def __init__(self, mongoString):
        mongoString= re.sub(r'\s*new\s*Date\(\s*\)', '"$$$DATE"', mongoString)
        Parser.__init__(self, mongoString)
       

    def parseCreateCollection(self, arg):
        db_name =arg[1:-1]
        # 没有列名，不进行 create table
        return ''

    def generateCreateTable(self, db_name, document):
        cols_dict = document # {user_id:"abc123",age:55,status:"A"}
        if '_id' in cols_dict:
            del cols_dict['_id']
        sql_create_args = ["id MEDIUMINT NOT NULL AUTO_INCREMENT"]
        for k in cols_dict.keys():
            v=cols_dict[k]
            sql_create_args.append("%s %s" % (k, mongo_type_name(v)))
        sql_create_args.append("PRIMARY KEY (id)")
        statement = "CREATE TABLE IF NOT EXISTS %s (\n    " % db_name
        statement += ',\n    '.join(sql_create_args)
        statement += '\n);\n'
        return statement

    def generateInsertIntoOne(self, db_name, document):
        statement = "INSERT INTO %s(" % db_name
        statement += ', '.join(document.keys()) + ')\n'
        statement += "    VALUES ("
        statement += ', '.join(map(format_mongodb_value, \
            document.values())) + ');\n'
        return statement

    def generateInsertIntoMany(self, db_name, documents):
        values = list()
        for document in documents:
            values.append('(' + ', '.join(map(format_mongodb_value, \
                    document.values())) + ')')
        statement = "INSERT INTO %s(" % db_name
        statement += ', '.join(document.keys()) + ') VALUES\n    '
        statement += ', '.join(values) + ';\n'
        return statement

    def parseInsertOne(self, db_name, arg):
        '''
        arg: {user_id:"abc123",age:55,status:"A"},
             {writeConcern:{w:0|1|"majority",j:0|1,wtimeout:100}}

        CREATE TABLE IF NOT EXISTS test (
            the_id  int   PRIMARY KEY,
            name    text
        );
        INSERT INTO people(user_id, age, status)
            VALUES("bcd001", 45, "A");
        '''        
        json_obj = demjson.decode('['+arg+']')
        extra_param = None
        if len(json_obj)==0:
            raise ValueError()
        elif len(json_obj)==1:
            pass
        elif len(json_obj)==2:
            extra_param = json_obj[1]
        elif len(json_obj)>2:
            raise ValueError()
        cols_dict = json_obj[0] # {user_id:"abc123",age:55,status:"A"}

        statement_1 = self.generateCreateTable(db_name, cols_dict)
        statement_2 = self.generateInsertIntoOne(db_name, cols_dict)
        statement = statement_1 + statement_2
        return statement

    def parseInsertMany(self, db_name, arg):
        '''
        arg: [{user_id:"abc123",age:55,status:"A"},...], {w:, ...}

        CREATE TABLE IF NOT EXISTS test (
            the_id  int   PRIMARY KEY,
            name    text
        );
        '''        
        json_obj = demjson.decode('['+arg+']')
        extra_param = None
        if len(json_obj)==0:
            raise ValueError()
        elif len(json_obj)==1:
            pass
        elif len(json_obj)==2:
            # {writeConcern:{w:0|1|"majority",j:0|1,wtimeout:100}}
            extra_param = json_obj[1]
        elif len(json_obj)>2:
            raise ValueError()
        documents = json_obj[0]
        if len(documents)<1:
            raise ValueError('cannot insert zero document')

        statement_1 = self.generateCreateTable(db_name, documents[0])
        cols_dict = documents[0] # {user_id:"abc123",age:55,status:"A"}
        statement_2 = self.generateInsertIntoMany(db_name, documents)
        statement = statement_1 + statement_2
        return statement

    def generateAddColumn(self, db_name, col_name, col_type_name):
        return "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;\n"% \
                (db_name, col_name, col_type_name)
    def generateDropColumn(self, db_name, col_name):
        return "ALTER TABLE %s DROP COLUMN IF EXISTS %s;\n"% \
                (db_name, col_name,)
    def generateUpdate(self, db_name, condition, updates):
        stat = 'UPDATE %s\n' % db_name
        set_cols = []
        for col in updates:
            val = updates[col]
            if val != '$$$DATE':
                new_val = format_mongodb_value(val)
            else:
                new_val = 'CURRENT_DATE'
            set_cols.append( '%s=%s'%(col, new_val))
        stat += 'SET ' + ', '.join(set_cols)
        if len(condition)==0:
            stat += ';\n'
        else:
            stat += '\nWHERE ' + parse_condition(condition) + ';\n'
        return stat

    def parseUpdateMany(self, db_name, arg):
        '''
        arg: "{condition}, { $set: { join_date: new Date() } }"
        '''
        json_obj = demjson.decode('['+arg+']')
        if len(json_obj)!=2 and len(json_obj)!=3:
            raise ValueError('second argument cannot be empty')
        if len(json_obj)==3:
            extra_param = json_obj[2]
        else:
            extra_param = None
        condition = json_obj[0]
        sql_condition_str = parse_condition(condition)
        final_statement = ""
        if '$set' in json_obj[1]:
            updates = json_obj[1]['$set']
            for k in updates:
                v = updates[k]
                add_column_stat = self.generateAddColumn(db_name, k, \
                        mongo_type_name(v))
                final_statement += add_column_stat
            final_statement += self.generateUpdate(db_name, condition, updates)
        if '$unset' in json_obj[1]:
            if len(condition) > 0 : # set = NULL
                #updates = json_obj[1]['$unset']
                #updates_null = dict()
                #for k in updates:
                #    updates_null[k]='$NULL'
                #final_statement += self.generateUpdate(db_name, 
                #        condition, updates_null)
                raise ValueError('cannot drop column with conditions')
            elif len(condition) == 0: # drop column
                updates = json_obj[1]['$unset']
                for k in updates:
                    v = updates[k]
                    add_column_stat = self.generateDropColumn(db_name, k)
                    final_statement += add_column_stat
        return final_statement

    def parseCreateIndex(self, db_name, arg):
        '''
        arg: {user_id:1, age: -1, ... }
        '''
        json_obj = demjson.decode('['+arg+']')[0]
        indexes = list()
        index_name = "idx"
        for col_name in json_obj.keys():
            val = json_obj[col_name]
            if val>0:
                index_name += '_'+ col_name+'_asc'
                indexes.append(col_name)
            else:
                index_name += '_'+ col_name+'_desc'
                indexes.append('%s DESC'%col_name)
        stat = 'CREATE INDEX %s\n' % index_name
        stat += 'ON %s(%s);\n'%(db_name, ', '.join(indexes))
        return stat

    def parseDrop(self, db_name, arg):
        return 'DROP TABLE %s;\n'%db_name

    def parse(self):
        re_create_collection="""^db.createCollection\(("\D\w*")\);?"""
        re_insert_one = """^db.(\D\w*).insertOne\((\S+)\);?"""
        re_insert_many = """^db.(\D\w*).insertMany\((\S+)\);?"""
        re_update_many = """^db.(\D\w*).updateMany\((\S+)\);?"""
        re_create_index = """^db.(\D\w*).createIndex\((\S+)\);?"""
        re_drop = """^db.(\D\w*).drop\((\S*)\);?"""

        #去掉字符串中的空格与换行
        string_need_parse = self.mongoString.replace(" ", "").replace("\n", "") 
        if re.match(re_create_collection, string_need_parse):
            m = re.search(re_create_collection, string_need_parse)
            return self.parseCreateCollection(m.group(1))
        elif re.match(re_insert_one, string_need_parse):
            m = re.search(re_insert_one, string_need_parse)
            return self.parseInsertOne(m.group(1), m.group(2))
        elif re.match(re_insert_many, string_need_parse):
            m = re.search(re_insert_many, string_need_parse)
            return self.parseInsertMany(m.group(1), m.group(2))
        elif re.match(re_update_many, string_need_parse):
            m = re.search(re_update_many, string_need_parse)
            return self.parseUpdateMany(m.group(1), m.group(2))
        elif re.match(re_create_index, string_need_parse):
            m = re.search(re_create_index, string_need_parse)
            return self.parseCreateIndex(m.group(1), m.group(2))
        elif re.match(re_drop, string_need_parse):
            m = re.search(re_drop, string_need_parse)
            return self.parseDrop(m.group(1), m.group(2))
        else:
            raise ValueError("re match failed:%s"%string_need_parse)

            


def main():
    with open("mongodb_input_create_alter.txt") as f:
        with open("output_create_alter.txt", "w") as wf:
            for line in f.readlines():
                if len(line.strip())>0:
                    parser = CreateAlterParser(line)
                    ss = parser.parse()
                    print(ss)
                    wf.write(ss)
                    wf.write("\n")

if __name__=='__main__':
    main()
    
