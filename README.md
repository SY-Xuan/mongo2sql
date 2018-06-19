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
try:
    parser = ParserManager.parser(mongoDB_string)
    sql_string = parser.parse()
    #if no error happen the result is in the sql_string
except ValueError as error:
    print(error)
```
## see example in example.py

* there are many MangoDB command in the input file
* in example.py read the inputfile and convert the command to the sql
* the result is in the **output.txt**

