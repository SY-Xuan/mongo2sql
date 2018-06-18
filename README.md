# mongo2sql
## Introduction
1. Need to install demjson
2. Convert MongoDB operation to sql
## How To Use
```python
#use manage object to parse 
#manage object is a factory object
#call manage object class funtion parser to get parser
#call parser function parse to get sql
parser = ParserManager(mongoDB_string)
sql_string = parse.parse()
```
### see example in example.py

