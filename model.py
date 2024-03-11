import redis
import json
import numpy as np
import datetime
import uuid

rd = redis.StrictRedis(host='localhost', port=9092)
class groupModel:
    """ matching instance;
    first submit -> input(json body) -> parsing, redis set
    after submit -> input(int pk) -> redis get json body, parsing

    add; add(), addWait(), addHope(), addFeat()
    read; getWait(), getHope(), getFeat(), getTransaction(), getKeys(), getPk()
    update; updateHope(), updateFeat()
    delete; delete(), delWait(), delHope(), delFeat(), delTransaction()
    """
    pk = 0
    type = ""
    subTime = ""
    body = ""
    userCap = 0
    userNum = 0
    gender = bool()
    _keys = {}
    users = []
    transactions = []

    def __init__(self, body):
        if type(body) is 'int':
            self.pk = body
            self.body = self.getWait()
            self._keys["front"] = f"front:match:wait:{self.pk}"
            return
        elif type(body) is "str":
            self.body = json.loads(body)
        elif type(body) is "list":
            pass
        else:
            raise ValueError(f"not valid json body; the body type is {type(body)}")

        # <------ type(body) = list, type(self.body) = str --------->    
        self.Validation(body)
        self.load()
    
    def load(self, mode = False):
        self.subTime = self.body["subTime"]
        self.type = self.body["type"]
        groupBody = self.body["group"]

        self.pk = groupBody["pk"]
        self.userCap = groupBody["userCap"]
        self.userNum = groupBody["userNum"]
        self.gender = groupBody["gender"]
        
        self._keys["front"] = f"front:match:wait:{self.pk}"
        self._keys["wait"] = f"match:wait:{self.type}:{self.userCap}:{self.gender}:{self.pk}"
        self._keys["transaction"] = f"match:wait:{self.type}:transaction:"

        for userbody in groupBody["user"]:
            user = userModel(self.pk, userbody)
            self.users.append(user)

        if mode:
            self.loadTransactions()

    def loadTransactions(self, mode = False):
        """
        mode = True   --> type(self.transactions) = object<transaction>
        mode = False  --> type(self.transactions) = str(hex)
        """
        self.transactions = rd.smembers(self._keys["transaction"] + "G" + self.pk)
        if mode:
            l = []
            for member in self.transactions:
                l.append(transaction(member))
            self.transactions = l

    def Validation(self, body):
        pass

    def getPk(self):
        return self.pk
    
    def add(self):
        self.addWait(), self.addHope(), self.addFeat()

    def addWait(self):
        dump = json.dumps(self.body)
        rd.set(self._keys["front"], self.pk)
        rd.set(self._keys["wait"], dump)

    def addHope(self):
        for user in self.users:
            user.addHope()

    def addFeat(self):
        for user in self.users:
            user.addFeat()

    def addTransaction(self, UUID):
        key = []
        key.append(self._keys["transaction"] + "G" + self.pk)
        key.append(self._keys["transaction"] + "T" + UUID)
        if rd.sismember(key[1], self.pk):
            rd.sadd(key[0], UUID)
        else:
            raise ValueError(f"the group '{self.pk}' is not in transaction '{UUID}'.")

    def getWait(self, mode=True):
        """ get json body from wait list.
        if mode = False -> get it from front waitlist
        if mode = True -> get it from backend waitlist.
        """
        if mode:
            return rd.get(self._keys["wait"])
        else:
            return rd.get(self._keys["front"])

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
    
    def delete(self):
        self.delFeat()
        self.delHope()
        self.delWait()

    def delFeat(self):
        for user in self.users:
            user.delFeat()

    def delHope(self):
        for user in self.users:
            user.delHope()

    def delWait(self):
        rd.delete(self._keys["wait"])
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
        # 만약, 처음 생성된 것이 아니라면 최초 생성 group인지 확인해야한다.
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
    groups = []

    def __init__(self, UUID = None, type = None, point = None, groups = None):
        """how to call "transaction"
        1. uuid
            input uuid, type.
        2. create
            input point, groups.
        """
        if not type:
            raise ValueError("you must input 'type'")
        elif type not in self.tList:
            raise ValueError(f"not valid parameter for transaction. the [type, {type}, {type(type)}] is not valid.")
        else:
            self.type = type

        if UUID:
            self.uuid = uuid.UUID(UUID)
            self.load()

        elif point and groups:
            if not (type(point) == "float"):
                raise ValueError(f"not valid parameter for transaction. the [point, {type(point)}] is not int.")
            self.point = point

            if not (type(groups) == "list" or type(groups) == "set"):
                raise ValueError(f"not valid parameter for transaction. the [groups, {groups}, {type(groups)}] is not list.")
            for group in groups:
                if type(group) is not "int":
                    raise ValueError(f"not valid parameter for transaction. the [group {group}, {type(group)}] is not int.")
                self.groups.append(groupModel(group))
            self.create()
        else:
            raise ValueError(f"not valid parameter for transaction.")
            
        self._key = f"match:wait:{self.type}:transaction:"
        self._mapKey = ""

    def load(self, mode = False):
        """
        mode = True     --> type(self.groups) = Objects<groupModel>
        mode = False    --> type(self.groups) = list(int)
        """
        self.score = rd.zscore(self._key + "score", self.uuid)
        self.groups = rd.smembers(self._key+"T"+self.uuid.hex)
        if mode:
            l = []
            for member in self.groups:
                l.append(groupModel(member))
            self.groups = l

    def create(self):
        self.uuid = uuid.uuid1()
        self.add()

    def add(self):
        rd.zadd(self._key + "score", [self.uuid, self.point])
        for group in self.groups:
            rd.sadd(self._key + "T" + self.uuid, group.getPk())
            rd.sadd(self._key + "G" + group.getPk(), self.uuid)
    
    def isInGroup(self, num):
        if type(self.groups) == "list":
            if num in self.groups:
                return True
        else:
            for group in self.groups:
                if group.getPk() == num:
                    return True
        return False
    
    def getGroups(self):
        if type(self.groups) == "list":
            self.load(True)
        return self.groups
    


        

        

        