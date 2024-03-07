import enum

class ctlEnum(enum.Enum):
    
    UMC2 = enum.auto()
    UMC3 = enum.auto()
    UMC4 = enum.auto()
    UMC5 = enum.auto()
    GMC1 = enum.auto()
    GMC2 = enum.auto()
    GMC3 = enum.auto()
    GMC4 = enum.auto()
    GMC5 = enum.auto()
    
    @classmethod
    def getValue(cls, t, cap):
        ch = ""
        if t == "group":
            ch = "GMC"
        elif t == "user":
            ch = "UMC"
        ch = ch + str(cap)
        for e in cls:
            if ch == e.name:
                return e.value
        return 0
    
    @classmethod
    def getName(cls, t, cap):
        ch = ""
        if t == "group" and cap >= 1 and cap <= 5:
            ch = "GMC"
        elif t == "user" and cap >= 2 and cap <= 5:
            ch = "UMC"
        else:
            return 0
        ch = ch + str(cap)
        return ch

print(ctlEnum.getValue("group", 5))
print(ctlEnum.getName("group", 5))