from typing import List

class Set:
    def __init__(self):
        self.ID: str = ""
        self.name: str = ""
        self.namesElements = []
        self.elements = []


    def getElements(self):
        return self.elementos
    def setElements(self, llista):
        self.elementos = llista
        
    def getNamesElements(self):
        return self.namesElements
    def setNamesElements(self, llista):
        self.namesElements = llista


    def getID(self) -> str:
        return self.ID
    def setID(self, id: str) -> None:
        self.ID = id


    def getName(self) -> str:
        return self.name
    def setName(self, name: str) -> None:
        self.name = name