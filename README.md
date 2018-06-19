# mongo2sql

 Convert MongoDB operation to sql in a simple way
## Requirements
### language version
 * python3

### package

 * demjson
```
pip install demjson
 ```
## How To Use
* manager object is a factory object
* call manager object static method **parser** to get parser
* call parser method **parse** to get sql
```python

parser = ParserManager.parser(mongoDB_string)
sql_string = parser.parse()
```
## see example in example.py

