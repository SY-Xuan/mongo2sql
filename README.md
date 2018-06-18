# mongo2sql
## Introduction
1. Need to install demjson
2. Convert MongoDB operation to sql
## How To Use
```python
#manager object is a factory object
#call manager object class funtion parser to get parser
#call parser function parse to get sql
parser = ParserManager.parser(mongoDB_string)
sql_string = parse.parse()
```
### see example in example.py

