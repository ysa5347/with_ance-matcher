import redis
import json
import numpy as np
import datetime

rd = redis.StrictRedis(host='localhost', port=9092)
class groupModel:
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
    
    def 

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
        rd.execute_command()
        
        

class transaction:

    def __init__(self, male, female):
        pass