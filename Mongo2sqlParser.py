
class Mongo2sqlParser(object):
    def __init__(self, mongoString):
        #将需要解析的语句在初始化时传入

        self.mongoString = mongoString

    #该方法返回解析得到的sql语句
    #在子类中重写该方法
    def parse(self):
        pass
        
