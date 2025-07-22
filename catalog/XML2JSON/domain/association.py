class Association:
    def __init__(self):
        self.ID = ""
        self.name = ""
        
        self.nameClassFrom = ""
        self.nameFrom = ""
        self.idFrom = ""
        self.mulFromMin = ""
        self.mulFromMax = ""
        
        self.nameClassTo = ""
        self.nameTo = ""
        self.idTo = ""
        self.mulToMin = ""
        self.mulToMax = ""
        
        
    def getID(self) -> str:
        return self.ID
    def setID(self, id: str) -> None:
        self.ID = id


    def getName(self) -> str:
        return self.name
    def setName(self, name: str) -> None:
        self.name = name


    def getNameClassFrom(self) -> str:
        return self.nameClassFrom
    def setNameClassFrom(self, nameClassFrom: str) -> None:
        self.nameClassFrom = nameClassFrom


    def getNameFrom(self) -> str:
        return self.nameFrom
    def setNameFrom(self, nameFrom: str) -> None:
        self.nameFrom = nameFrom


    def getIdFrom(self) -> str:
        return self.idFrom
    def setIdFrom(self, idFrom: str) -> None:
        self.idFrom = idFrom


    def getMulFromMin(self) -> str:
        return self.mulFromMin
    def setMulFromMin(self, mulFromMin: str) -> None:
        self.mulFromMin = mulFromMin


    def getMulFromMax(self) -> str:
        return self.mulFromMax
    def setMulFromMax(self, mulFromMax: str) -> None:
        self.mulFromMax = mulFromMax


    def getNameClassTo(self) -> str:
        return self.nameClassTo
    def setNameClassTo(self, nameClassTo: str) -> None:
        self.nameClassTo = nameClassTo


    def getNameTo(self) -> str:
        return self.nameTo
    def setNameTo(self, nameTo: str) -> None:
        self.nameTo = nameTo


    def getIdTo(self) -> str:
        return self.idTo
    def setIdTo(self, idTo: str) -> None:
        self.idTo = idTo


    def getMulToMin(self) -> str:
        return self.mulToMin
    def setMulToMin(self, mulToMin: str) -> None:
        self.mulToMin = mulToMin


    def getMulToMax(self) -> str:
        return self.mulToMax
    def setMulToMax(self, mulToMax: str) -> None:
        self.mulToMax = mulToMax