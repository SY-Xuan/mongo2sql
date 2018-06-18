'''
@author myp
parse_update_operators.py
1.解析了9个mongodb field update operators，操作符含义详见：
https://docs.mongodb.com/manual/reference/operator/update-field/
2.解析了{feild:value}更新方式
'''

import demjson

operators = [
    '$currentDate',
    '$inc',
    '$min',
    '$max',
    '$mul',
    '$rename',
    '$set',
    '$setOnInsert',
    '$unset']


# operator—_json eg: { $inc: { quantity: -2, metrics: 1 } }

def parse_update_opeators(operator_json):  # 参数可直接是json格式的字符串
    operator_dict = demjson.decode(operator_json)  # json to dict
    operator_keys = list(operator_dict)  # 提取操作符,格式为list
    if len(operator_dict) == 0:
        return 'Error:no update operator! '

    if operator_keys[0] in operators:

        # 使用递归方式一个一个的处理操作符
        if len(operator_dict) == 1:
            operator = operator_keys[0]

            # parse fields need to be update
            field_dict = operator_dict[operator]  # 提取field：value，格式为字典。e.g.  { quantity: -2, metrics: 1 }
            if len(field_dict) == 0:
                return 'Error:no field will be updated!'
            else:
                field_list = list(field_dict.keys())  # 提取field，格式为list。e.g. ['quantity','metrics']
                for field in field_list:
                    if '.' in field:
                        return "Error:field must be atomic"



            # 针对具体的操作符，一个一个解析

            # {$currentDate: {lastModified: true，cancellation: { $type: "timestamp" }}}
            if operator == '$currentDate':
                field_line = []
                for field in field_list:
                    if field_dict[field] is True:
                        field_line.append('%s = CURRENT_DATE' % field)
                    elif len(list(field_dict[field])) == 1 and list(field_dict[field])[0] == '$type':
                        date_type = field_dict[field]['$type']
                        if date_type == 'date':
                            field_line.append('%s = CURRENT_DATE' % field)
                        elif date_type == 'timestamp':
                            field_line.append('%s = CURRENT_TIMESTAMP' % field)
                    else:
                        field_line.append('Error:your input must be legal!')
                return 'SET ' + ','.join(field_line)

            # {$inc: {quantity: -2, metrics: 1}}}
            if operator == '$inc':
                field_line = ['{} = {}+({}) '.format(field, field, field_dict[field]) \
                              for field in field_list]
                return 'SET ' + ','.join(field_line)

            # { $min: { lowScore: 250 } }
            if operator == '$min':
                field_line = ['SET {} = {} WHERE ({} > {}) '.format(field, field_dict[field], field, field_dict[field]) \
                              for field in field_list]
                return ';'.join(field_line)

            # { $max: {highScore: 870}}
            if operator == '$max':
                field_line = ['SET {} = {} WHERE ({} < {}) '.format(field, field_dict[field], field, field_dict[field]) \
                              for field in field_list]
                return ';'.join(field_line)

            # { $mul: {qty: 2}}
            if operator == '$mul':
                field_line = ['{} = {} * ({})'.format(field, field, field_dict[field]) \
                              for field in field_list]
                return 'SET ' + ','.join(field_line)

            # { $rename: { 'nickname': 'alias', 'cell': 'mobile' } }
            # ALTER TABLE collection RENAME column_name to new_column_name;
            if operator == '$rename':
                field_line = ['RENAME {} TO {}'.format(field, field_dict[field]) \
                              for field in field_list]
                return 'ALTER TABLE collection ' + ' ' + ','.join(field_line)

            # {$set: { "details.make": "zzz" }}
            if operator == '$set':
                for field in field_list:
                    if '{}'.format(field_dict[field]).isdigit() == False:
                        field_dict[field] = "\"" + field_dict[field] + "\""
                field_line = ['{} = {}'.format(field, field_dict[field]) \
                              for field in field_list]
                return 'SET ' + ','.join(field_line)

            # $setOnInsert: {defaultQty: 100}
            #INSERT INTO collection (_id,field1) VALUES(1,value1) CONFLICT(_id) DO UPDATE SET …
            #or do nothing
            if operator == '$setOnInsert':
                values = ['{}'.format(field_dict[field]) for field in field_list]
                field_value=['{}={}'.format(field, field_dict[field]) for field in field_list]
                return ' INSERT INTO  collection('  + ','.join(field_list) + \
                       ') VALUES('  + ','.join(values) + ') ON CONFLICT(query) DO UPDATE SET '+','.join(field_value)

            # { $unset: { quantity: "", instock: "" }
            # ALTER TABLE collection DROP column_name;
            if operator == '$unset':
                field_line = ['DROP {}'.format(field) \
                              for field in field_list]
                return 'ALTER TABLE collection ' + ','.join(field_line)
        if len(operator_dict) > 1:
            result = []
            for operator_key in operator_keys:
                json_str = demjson.encode({operator_key: operator_dict[operator_key]})
                result.append(parse_update_opeators(json_str))

            # 将结果列表中操作都为set的以逗号分割合并为一行
            mark_index = len(result)
            for index in range(0, len(result)):
                if 'Error' in result[index]:
                    return result[index]
                    break
                if 'SET' in result[index] and mark_index == len(result) and 'INSERT' not in result[index]:
                    mark_index = index
                    continue
                if 'SET' in result[index] and mark_index != len(result) and 'INSERT' not in result[index]:
                    result[mark_index] = '{},{}'.format(result[mark_index],result[index][3:])
                    del result[index]
                    continue

            return ';'.join(result)

    #解析{field:value}
    elif '$' not in operators:
        field_line = []
        for operator_key in operator_keys:
            if '{}'.format(operator_dict[operator_key]).isdigit() == False:
                operator_dict[operator_key] = "\"" + operator_dict[operator_key] + "\""
            field_line.append('{} = {}'.format(operator_key, operator_dict[operator_key]))
        return 'SET ' + ','.join(field_line)
    else:
        return 'Error: update operator is illegal!'


def test_parse_update_operators():
    def pp(s):
        print(parse_update_opeators(s))

    #pp('''{$currentDate: {lastModified:true,cancellation: { $type: "timestamp" },
    #ccccellation: { $type: "date" }}, $mul: {qty: 2},$inc: {quantity: -2, metrics: 1}}''')
    #pp('''{$inc: {quantity: -2, metrics: 1}}''')
    #pp('''{ $min: { "lowScorefe": 250 ,highScore: 870} }''')
    #pp('''{ $max: {highScore: 870}}''')
    #pp('''{ $mul: {qty: 2}}''')
    pp('''{ $rename: { 'nickname': 'alias', 'cell': 'mobile' } }''')
    pp('''{ $set: { "sizeuom": "in", status: "P" }, $currentDate: { lastModified: true } }''')
    pp('''{$set: { "sizeuom": "cm", status: "P" },$setOnInsert: { lastModified: true },field:"value"}''')
    pp('''{ $unset: { quantity: "", instock: "" }}''')
    pp('''{ lowScore: 250, quantity: 'jh'} ''')


if __name__ == '__main__':
    test_parse_update_operators()
