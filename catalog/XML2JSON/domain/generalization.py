class Generalization:
    def __init__(self):
        self.ID = ""
        self.name = ""
        
        self.nameParent = ""
        self.idParent = ""
        
        self.namesChildren = []
        
        self.disjoint = False
        self.complete = False
        self.discriminator = ""
        

    def getID(self) -> str:
        return self.ID

    def setID(self, ID: str) -> None:
        self.ID = ID

    def getName(self) -> str:
        return self.name

    def setName(self, name: str) -> None:
        self.name = name

    def getNameParent(self) -> str:
        return self.nameParent

    def setNameParent(self, nameParent: str) -> None:
        self.nameParent = nameParent
        
    def getIdParent(self) -> str:
        return self.idParent

    def setIdParent(self, idParent: str) -> None:
        self.idParent = idParent

    def addNameChild(self, nameChild: str) -> None:
        self.namesChildren.append(nameChild)

    def getNamesChildren(self) -> list[str]:
        return self.namesChildren

    def setNamesChildren(self, namesChildren: list[str]) -> None:
        self.namesChildren = namesChildren

    def getDisjoint(self) -> bool:
        return self.disjoint

    def setDisjoint(self, disjoint: bool) -> None:
        self.disjoint = disjoint

    def getComplete(self) -> bool:
        return self.complete

    def setComplete(self, complete: bool) -> None:
        self.complete = complete

    def getDiscriminator(self) -> str:
        return self.discriminator

    def setDiscriminator(self, discriminator: str) -> None:
        self.discriminator = discriminator
        


class Generalization_single:
    def __init__(self):
        self.ID = ""
        self.name = ""
        
        self.nameParent = ""
        self.idParent = ""
        
        self.nameChild = ""
        self.idChild = ""
        
        
    def getID(self) -> str:
        return self.ID

    def setID(self, id: str) -> None:
        self.ID = id

    def getName(self) -> str:
        return self.name

    def setName(self, name: str) -> None:
        self.name = name

    def getNameParent(self) -> str:
        return self.nameParent

    def setNameParent(self, nameParent: str) -> None:
        self.nameParent = nameParent

    def getIdParent(self) -> str:
        return self.idParent

    def setIdParent(self, idParent: str) -> None:
        self.idParent = idParent

    def getNameChild(self) -> str:
        return self.nameChild

    def setNameChild(self, nameChild: str) -> None:
        self.nameChild = nameChild

    def getIdChild(self) -> str:
        return self.idChild

    def setIdChild(self, idChild: str) -> None:
        self.idChild = idChild
