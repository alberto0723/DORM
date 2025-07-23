class Set:
    def __init__(self):
        self.ID: str = ""
        self.name: str = ""
        self.elements = []

    def getElements(self):
        return self.elementos

    def setElements(self, llista):
        self.elementos = llista

    def getID(self) -> str:
        return self.ID

    def setID(self, id: str) -> None:
        self.ID = id

    def getName(self) -> str:
        return self.name

    def setName(self, name: str) -> None:
        self.name = name
