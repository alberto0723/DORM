from typing import List

class ClassUML:
    def __init__(self):
        self.ID: str = ""
        self.name: str = ""


    def getID(self) -> str:
        return self.ID
    def setID(self, id: str) -> None:
        self.ID = id


    def getName(self) -> str:
        return self.name
    def setName(self, name: str) -> None:
        self.name = name