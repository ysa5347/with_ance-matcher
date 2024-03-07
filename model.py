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
    type = ""
    subTime = ""
    body = ""
    userCap = 0
    userNum = 0
    gender = True
    _keys = {}
    users = []

    def __init__(self, body):
        if type(body) is 'int':
            self.pk = body
            self._keys["front"] = f"front:match:wait:{self.pk}"
            self.body = self.getWait()
        elif type(body) is "str":
            self.body = json.loads(body)
        elif type(body) is "list":
            pass
        else:
            raise ValueError(f"not valid json body; the body type is {type(body)}")

        # <------ type(body) = list, type(self.body) = str --------->    
        self.Validation(body)

        self.subTime = body["subTime"]
        self.type = body["type"]
        groupBody = body["group"]

        self.pk = groupBody["pk"]
        self.userCap = groupBody["userCap"]
        self.userNum = groupBody["userNum"]
        self.gender = groupBody["gender"]
        
        self._keys["front"] = f"front:match:wait:{self.pk}"
        self._keys["wait"] = f"match:wait:{self.type}:{self.userCap}:{self.gender}:{self.pk}"
        self._keys["transaction"] = f"match:wait:transaction:group:{self.pk}"

        for userbody in groupBody["user"]:
            user = user(userbody)
            self.users.append(user)
    
    def Validation(self, body):
        pass

    def getPk(self):
        return self._pk
    
    def addWait(self):
        dump = json.dumps(self.body)
        rd.set(self._keys["front"], dump)
        rd.set(self._keys["wait"], dump)

    def getWait(self, mode=False):
        """ get json body from wait list.
        if mode = False -> get it from front waitlist
        if mode = True -> get it from backend waitlist.
        """
        if mode:
            return rd.get(self._keys["wait"])
        else:
            return rd.get(self._keys["front"])
    
    def addHope(self):
        for user in self.users:
            user.addHope()

    def addFeat(self):
        for user in self.users:
            user.addFeat()

    def getHope(self):
        l = []
        for user in self.users:
            l.append(user.getHope())
        return l
    
    def getFeat(self):
        l = []
        for user in self.users:
            l.append(user.getFeat())
        return l
    

    # get transaction, or objects.get, objects.filter 고안 필요

class userModel:
    """ under group model.
    created by group, having CRUD methods
    Create; add(), addHope(), addFeat()
    Read; getHope(), getFeat(),
    Update; putHope(), putFeat()
    Delete; del(), delHope(), delFeat()
    
    """
    group = 0
    userID = ""
    _keys = {}

    def Validation(self, body):
        pass

    def __init__(self, pk, body):
        self.group = pk
        self.body = body
        self.userID = body["userID"]
        self.gender = body["gender"]
        
        self.hope = body["hope"]
        self.feat = body["feat"]

        self._keys["hope"] = f"match:hope:{self.userID}"
        self._keys["feat"] = f"match:feat:{self.userID}"

    def add(self):
        self.addHope(), self.addFeat()

    def addHope(self):
        rd.set(self._keys["hope"], self.hope)
    
    def addFeat(self):
        rd.set(self._keys["feat"], self.feat)

    def getHope(self):
        return rd.get(self._keys["hope"])
    
    def getFeat(self):
        return rd.get(self._keys["feat"])
        
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
    """ transaction; distinguished with UUID.
    in redis, transaction score were store in ~:transaction:score to zset.
    and the table about between transaction and group were store in ~:transaction:find:G{group} or ~:T{UUID} to set.
    
    if group want to withdraw waiting, groupModel search transaction:find:G{group} to find transaction UUID what participating and del it.
    """
    tList = ["user", "group"]
    def __init__(self, point, type, *groups):
        if type(point) is not "float":
            raise ValueError(f"not valid parameter for transaction. the [point, {type(point)}] is not int.")
        self.point = point
        if type not in self.tList:
            raise ValueError(f"not valid parameter for transaction. the [type, {type}, {type(type)}] is not invalid.")
            self.type = type
        for group in groups:
            if type(group) is not "int":
                raise ValueError(f"not valid parameter for transaction. the [group {group}, {type(group)}] is not int.")
        self.groups = groups

        self._key = f"match:transaction:{self.type}:"
        self._mapKey = ""
        for group in self.groups:
            self._mapKey = self._mapKey + str(group) + "-"
        self._mapKey = self._mapKey[:-1]

    def add(self):
        rd.zadd(self._key, [self._mapKey, self.point])

        

        

        