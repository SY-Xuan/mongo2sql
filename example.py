'''
@the input files include many MongoDB operation command
the result of convert is in the output.txt  

'''


from ParserManager import ParserManager
import re

def parse_file(input_file_name, output_file_name, output_mode):
    with open(input_file_name) as f:
        with open(output_file_name, output_mode) as wf:
            for line in f.readlines():
                if len(line.strip())>0:
                    try:
                        parser = ParserManager.parser(line)
                        ss = parser.parse()
                        print(ss)
                        wf.write(ss)
                        wf.write("\n")
                    except ValueError as error:
                        raise

parse_file("mongodb_input.txt", "output.txt", "w")
parse_file("update_input.txt", "output.txt", "a")
parse_file("mongodb_input_create_alter.txt", "output.txt", "a")
parse_file("mongodb_delete_input.txt", "output.txt", "a")

