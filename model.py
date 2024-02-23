import redis
import json
import numpy as np
import datetime

rd = redis.StrictRedis(host='localhost', port=9092)
class groupModel:
    """ matching instance;
    first submit -> input(json body) -> parsing, redis set
    after submit -> input(int pk) -> redis get json body, parsing

    add; add(), addWait(), addHope(), addFeat()
    read; getWait(), getHope(), getFeat(), getTransaction(), getKeys(), getPk()
    update; updateHope(), updateFeat()
    delete; del(), delWait(), delHope(), delFeat(), delTransaction()
    """
    _pk = 0
    subTime = ""
    userCap = 0
    userNum = 0
    gender = True
    _keys = []
    users = []

    def Validation(self, body):
        if body["group"]["userNum"] == body["group"]["user"].len():
            return True
        else:
            raise ValueError("invalid 'user' objects num, incorrect with 'userNum'.")

    def __init__(self, body):
        if type(body) is 'int':
            self.pk = body

        self.subTime = body["subTime"]
        groupBody = body["group"]

        self.Validation(body)
        self.pk = groupBody["pk"]
        self.userCap = groupBody["userCap"]
        self.userNum = groupBody["userNum"]
        self.gender = groupBody["gender"]
        
        for userbody in groupBody["user"]:
            user = user(userbody)
            self.users.append(user)
    
    def getPk(self):
        return self._pk
    
    # get transaction, or objects.get, objects.filter 고안 필요

class userModel:
    userID = ""
    _keys = {}

    def Validation(self, body):
        pass

    def __init__(self, body):
        self.body = body
        self.userID = body["userID"]
        self.gender = body["gender"]
        
        self.hope = body["hope"]
        self.feat = body["feat"]

        self._keys["hope"] = f"match:hope:{self.userID}"
        self._keys["feat"] = f"match:feat:{self.userID}"

    def addHope(self):
        rd.set(self._keys["hope"], self.hope)
    
    def addFeat(self):
        rd.set(self._keys["feat"], self.feat)

    def getHope(self):
        rd.get(self._keys["hope"])
    
    def getFeat(self):
        rd.get(self._keys["feat"])
        
    def updateHope(self):
        try:
            rd.execute_command('SET', self._keys["hope"], self.hope, 'XX')
        except:
            raise ValueError("hope vector not exist.")
    
    def updateFeat(self):
        try:
            rd.execute_command('SET', self._keys["feat"], self.hope, 'XX')
        except:
            raise ValueError("feat vector not exist.")
        

class transaction:

    def __init__(self, male, female):
        pass