from attribute import Attribute

class ClassUML:
    def __init__(self):
        self.ID: str = ""
        self.name: str = ""
        self.count: str = ""
        self.attributes: list[Attribute] = []

    def getListAttributes(self) -> list[Attribute]:
        return self.attributes

    def setListAttributes(self, llista: list[Attribute]):
        self.attributes = llista

    def getID(self) -> str:
        return self.ID

    def setID(self, id: str) -> None:
        self.ID = id

    def getName(self) -> str:
        return self.name

    def setName(self, name: str) -> None:
        self.name = name
        
    def getCount(self) -> str:
        return self.count

    def setCount(self, count: str) -> None:
        self.count = count
