from typing import List, Optional, Union
import xml.etree.ElementTree as ET
from tkinter import Tk
from tkinter.filedialog import askopenfilename

from set import Set
from structure import Structure
from classUML import ClassUML

Node = Union[Set, Structure, ClassUML]


class TxParsing:
    def __init__(self):
        self.componentsList = []
        self.mapComponents: dict[str, Node] = {}
        self.mapComponentsParent: dict[str, Optional[Node]] = {}
        
    def getComponents(self):
        return reversed(self.componentsList)
    
    def getMapComponents(self):
        return self.mapComponents


    def loadComponents(self, root: str):
        if not root:
            raise FileNotFoundError("No file was selected.")

        try:
            tree = ET.parse(root)
            root_elem = tree.getroot()
        except Exception as e:
            raise RuntimeError(f"Error parsing XML '{root}': {e}")
    
        self.loadModels(root_elem)
        self.loadAssociacions(root_elem)


    def loadModels(self, root: ET.Element):
        models_xml = root.find('Models')
        for e in models_xml.findall('Model'):
            self.loadModel(e, None)
            
    
    def loadModel(self, elem: ET.Element, parent: Optional[Node]):
        iden = elem.get('Id','')
        name = elem.get('Name', '').replace(' ', '_')
        
        stereotype = elem.find('Stereotypes')
        if stereotype is None:
            raise ValueError(f"Package {name} with ID {iden} has no stereotypes")
        
        
        for st in stereotype.findall('Stereotype'):
            if st.get('Name', '').lower() == 'set':
                s = Set()
                s.setID(iden)
                s.setName(name)
                
                self.mapComponents[iden] = s
                self.mapComponentsParent[iden] = parent
                self.componentsList.append(iden)
                
                model_children = elem.find('ModelChildren') 
                if model_children is not None:
                    sub_elements = []
                    for sub_elem in model_children.findall('Model'):
                        sub_elements.append(self.loadModel(sub_elem, s))
                    s.setElements(sub_elements)
                
                return name
            
            elif st.get('Name', '').lower() == 'struct':
                s = Structure()
                s.setID(iden)
                s.setName(name)
                
                self.mapComponents[iden] = s
                self.mapComponentsParent[iden] = parent
                self.componentsList.append(iden)
                
                model_children = elem.find('ModelChildren') 
                if model_children is not None:
                    sub_elements = []
                    for sub_elem in model_children.findall('Model'):
                        sub_elements.append(self.loadModel(sub_elem, s))
                    
                    
                    anchors = []
                    for classe in model_children.findall('Class'):
                        self.processClass(classe, s, sub_elements, anchors)
                        
                    s.setElements(sub_elements)
                    s.setAnchors(anchors)
                
                return name
            
        raise ValueError(f"Package {name} with ID {iden} has no stereotypes")
            
        
    def processClass(self, e: ET.Element, parent: Structure, sub_elements, anchors):
        c = ClassUML()
        c.setName(e.get('Name', '').replace(" ", "_"))
        c.setID(e.get('Id', ''))
        stereotype = e.find('Stereotypes')
        if stereotype is None:
            raise ValueError(f"The class {c.getName()} with identifier {c.getID()} has no stereotypes")
        
        has_stereotype = False
        for st in stereotype.findall('Stereotype'):
            if st.get('Name', '').lower() == 'phantom':     #Association class with phantom stereotype
                sub_elements.append(c.getName())
                has_stereotype = True
                break
            elif st.get('Name', '').lower() == 'anchor':    #Anchor class
                anchors.append(c.getName())
            
                model_children = e.find('ModelChildren') 
                if model_children is not None:
                    for elem in model_children.findall('Attribute'):
                        sub_elements.append(elem.get('Name', '  ').replace(' ', '_'))
                
                has_stereotype = True
                break

        
        self.mapComponents[c.getID()] = c
        self.mapComponentsParent[c.getID()] = parent
        
        if not has_stereotype:
            raise ValueError(f"The class {c.getName()} with identifier {c.getID()} has no stereotypes")
    
    def loadAssociacions(self, root: ET.Element):
        models = root.find('Models')

        rels_cont = models.find("ModelRelationshipContainer[@Name='relationships']")
        if rels_cont is None:
            return
        assoc_cont = rels_cont.find("ModelChildren/ModelRelationshipContainer[@Name='Association']")
        if assoc_cont is None:
            return
        assoc_list = assoc_cont.find('ModelChildren')
        if assoc_list is None:
            return
        
        for assoc in assoc_list.findall(".//Association"):  

            id_from = assoc.get('EndRelationshipFromMetaModelElement')
            id_to   = assoc.get('EndRelationshipToMetaModelElement')

            name = assoc.get('Name')

            n1 = self.mapComponents.get(id_from)
            n2 = self.mapComponents.get(id_to)
            if not n1 or not n2 or not name:
                continue

            path1 = self.getPathToRoot(n1.getID())
            path2 = self.getPathToRoot(n2.getID())
            lca = None

            for anc in path2:
                if anc in reversed(path1):
                    lca = anc
                    break
            if not lca:
                continue
            nodo_lca = self.mapComponents[lca]
            nodo_lca.getName()

            is_anchor = False

            stereotype = assoc.find('Stereotypes')
            if stereotype is not None:
                for st in stereotype.findall('Stereotype'):
                    if st.get('Name', '').lower() == 'anchor':
                        nodo_lca.addAnchor(name)
                        is_anchor = True
                        break
            
            if not is_anchor: nodo_lca.addElement(name)
        
    def getPathToRoot(self, node_id: str) -> List[str]:

        res = []
        current = node_id
        while current is not None:
            res.append(current)
            mapComponentsParent_node = self.mapComponentsParent.get(current)
            if mapComponentsParent_node is None:
                break
            current = mapComponentsParent_node.getID()
        return res