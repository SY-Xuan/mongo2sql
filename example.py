'''
@the input files include many MongoDB operation command
the result of convert is in the output.txt  

'''


from ParserManager import ParserManager
import re
# parser = CreateAlterParser("db.students.updateMany( {}, { $rename: { \"nmae\": \"name\" } } )")
# print(parser.parse())
with open("mongodb_input.txt") as f:
    with open("output.txt", "w") as wf:
        for line in f.readlines():
            try:
                parser = ParserManager.parser(line)
                ss = parser.parse()
                print(ss)
                wf.write(ss)
                wf.write("\n")
            except ValueError as error:
                print(error)

with open("update_input.txt") as f:
    with open("output.txt", "a") as wf:
        for line in f.readlines():
            if len(line.strip()) > 0:
                print(line)
                try:
                    parser = ParserManager.parser(line)
                    ss = parser.parse()
                    print(ss)
                    wf.write(ss)
                    wf.write("\n")
                except ValueError as error:
                    print(error)

with open("mongodb_input_create_alter.txt") as f:
    with open("output.txt", "a") as wf:
        for line in f.readlines():
            if len(line.strip())>0:
                try:
                    parser = ParserManager.parser(line)
                    ss = parser.parse()
                    print(ss)
                    wf.write(ss)
                    wf.write("\n")
                except ValueError as error:
                    print(error)

with open("mongodb_delete_input.txt") as f:
    with open("output.txt", "a") as wf:
        for line in f.readlines():
            try:
                parser = ParserManager.parser(line)
                ss = parser.parse()
                print(ss)
                wf.write(ss)
                wf.write("\n")
            except ValueError as error:
                print(error)