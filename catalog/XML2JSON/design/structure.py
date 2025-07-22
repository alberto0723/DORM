from typing import List

class Structure:
    def __init__(self):
        self.ID = ""
        self.name = ""
        self.anchors: List[str] = []
        self.elements: List[str] = []

    def getID(self) -> str:
        return self.ID
    def setID(self, id: str) -> None:
        self.ID = id


    def getName(self) -> str:
        return self.name
    def setName(self, name: str) -> None:
        self.name = name


    def getAnchors(self) -> List[str]:
        return self.anchors
    def setAnchors(self, anchors: List[str]) -> None:
        self.anchors = anchors
    def addAnchor(self, anchor: str) -> None:
        self.anchors.append(anchor)


    def getElements(self):
        return self.elements
    
    def setElements(self, elements) -> None:
        self.elements = elements
        
    def addElement(self, element) -> None:
        self.elements.append(element)
