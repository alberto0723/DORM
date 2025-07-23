class Attribute:
    def __init__(self):
        self.ID = ""
        self.name = ""
        self.datatype = ""
        self.size = ""
        self.distinctvals = ""
        self.identifier = False

    def getID(self) -> str:
        return self.ID

    def setID(self, id: str) -> None:
        self.ID = id

    def getName(self) -> str:
        return self.name

    def setName(self, name: str) -> None:
        self.name = name

    def getDatatype(self) -> str:
        return self.datatype

    def setDatatype(self, datatype: str) -> None:
        self.datatype = datatype

    def getSize(self) -> str:
        return self.size
    def setSize(self, size: str) -> None:
        self.size = size

    def getDistinctVals(self) -> str:
        return self.distinctvals

    def setDistinctVals(self, distinctvals: str) -> None:
        self.distinctvals = distinctvals

    def getIdentifier(self) -> bool:
        return self.identifier

    def setIdentifier(self, identifier: bool) -> None:
        self.identifier = identifier