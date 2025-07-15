from typing import List
import xml.etree.ElementTree as ET

from classUML import ClassUML
from attribute import Attribute
from association import Association
from generalization import Generalization, Generalization_single


class TxParsing:
    def __init__(self):
        self.ListClasses: List[ClassUML] = []
        self.ListAssociations: List[Association] = []
        self.ListGeneralizations: List[Generalization] = []

    
        
    def getClasses(self):
        return self.ListClasses
    def getAssociations(self):
        return self.ListAssociations
    def getGeneralizations(self):
        return self.ListGeneralizations


    def loadElements(self, root: str):
        if not root:
            raise FileNotFoundError("No file has been selected.")
        try:
            tree = ET.parse(root)
            root_elem = tree.getroot()
        except Exception as e:
            raise RuntimeError(f"Error parsing the XML '{root}': {e}")
    
        self.loadClasses(root_elem)
        self.loadAssociations(root_elem)
        self.loadGeneralizations(root_elem)



    def loadClasses(self, root: str):
        models_xml = root.find('Models')
        classes: List[ClassUML] = []
        for e in models_xml.findall('Class'):
            c = ClassUML()
            name, count = self.getNameCount(e.get('Name', ''))
            
            c.setName(name)
            c.setCount(count)
            c.setID(e.get('Id', ''))
            c.setListAttributes(self.importAttributesClass(e))
            classes.append(c)
            
        self.ListClasses = classes
    
    def importAttributesClass(self, class_elem: ET.Element) -> List[Attribute]:
        atributs_list: List[Attribute] = []
        
        model_children = class_elem.find('ModelChildren') 
        if model_children is None:
            return []
        
        for elem in model_children.findall('Attribute'):
            at = Attribute()
            at.setID(elem.get('Id', ''))
            at.setName(elem.get('Name', '').replace(' ', '_'))
            at.setSize(elem.get('TypeModifier', ''))
            at.setDistinctVals(elem.get('Multiplicity', ''))
            identifier_flag = elem.get('IsID', 'false').lower() == 'true'
            at.setIdentifier(identifier_flag)
            
            at_type = None
            dt = elem.find('./Type/DataType')
            if dt is not None and dt.get('Name'):
                at_type = dt.get('Name')
            elif elem.get('Type'):
                at_type = elem.get('Type')   
            else:
                name_attr = elem.get('Name', '')
                id_attr = elem.get('Id', '')
                raise ValueError(f"The attribute '{name_attr}' (Id={id_attr}) does not have a defined type in <DataType Name='...'/>.")
            
            at.setDatatype(at_type)
            
            atributs_list.append(at)
            
        return atributs_list
    
    
    
    def loadAssociations(self, root: str):
        models = root.find('Models')
        assocs: List[Association] = []

        rels_cont = models.find("ModelRelationshipContainer[@Name='relationships']")
        if rels_cont is None:
            return []
        assoc_cont = rels_cont.find("ModelChildren/ModelRelationshipContainer[@Name='Association']")
        if assoc_cont is None:
            return []
        assoc_list = assoc_cont.find('ModelChildren')
        if assoc_list is None:
            return []

        for assoc in assoc_list.findall('Association'):
                i = Association()
                i.setID(   assoc.get('Id', '')     )
                i.setName(  assoc.get('Name', '')   )

                # FromEnd
                fe = assoc.find('FromEnd/AssociationEnd')
                if fe is not None:
                    i.setIdFrom(   fe.get('Id', '')    )
                    i.setNameFrom(  fe.get('Name', '')  )
                    i.setNameClassFrom(self.getClassEnd(fe))
                    minf, maxf = self.getMultiplicities(fe.get('Multiplicity', ''))
                    i.setMulFromMin(minf)
                    i.setMulFromMax(maxf)

                # ToEnd
                te = assoc.find('ToEnd/AssociationEnd')
                if te is not None:
                    i.setIdTo(     te.get('Id', '')    )
                    i.setNameTo(    te.get('Name', '')  )
                    i.setNameClassTo(self.getClassEnd(te))
                    mint, maxt = self.getMultiplicities(te.get('Multiplicity', ''))
                    i.setMulToMin(mint)
                    i.setMulToMax(maxt)

                assocs.append(i)

        self.ListAssociations = assocs
        
        
    def loadGeneralizations(self, root: str):
        models = root.find('Models') or []

        
        rels = models.find("ModelRelationshipContainer[@Name='relationships']")
        if rels is None:
            return []
        
        gen_cont = rels.find("ModelChildren/ModelRelationshipContainer[@Name='Generalization']")
        if gen_cont is None:
            return []
        
        children = gen_cont.find('ModelChildren')
        if children is None:
            return []
        
        gen_list = children.findall('Generalization')
        
        
        temp_inds = []
        for gen in gen_list:
            temp_inds.append(self.generateSingleGeneralization(root, gen))
        
        generals = {}
        for gi in temp_inds:
            self.joinGeneralizations(generals, gi)

        result = list(generals.values())
        self.ListGeneralizations = result



    def generateSingleGeneralization(self, root: ET.Element, gen: ET.Element) -> Generalization_single:
        gi = Generalization_single()
        gi.setID(gen.get('Id', ''))
        gi.setName(gen.get('Name', ''))

        parent_id = gen.get('From', '')
        child_id = gen.get('To', '')
        gi.setIdParent(parent_id)
        gi.setNameParent(self.getClassID(root, parent_id))
        gi.setIdChild(child_id)
        gi.setNameChild(self.getClassID(root, child_id))

        for st in gen.findall('Stereotypes/Stereotype'):
            nm = st.get('Name', '').lower()
            if nm == 'disjoint':
                gi.setDisjoint(True)
            elif nm == 'complete':
                gi.setComplete(True)

        self.asignarDiscriminator(root, gi)
        
        return gi

    def asignarDiscriminator(self, root: ET.Element, gi: Generalization_single) -> None:
        class_elem = root.find(f".//Class[@Id='{gi.getIdParent()}']")
        if class_elem is None:
            raise ValueError(f"The parent class with Id={gi.getIdParent()} does not exist.")
        found = False
        
        model_children = class_elem.find('ModelChildren')
        if model_children is None:
            return []
        
        for at in model_children.findall('Attribute'):
            for st in at.findall('Stereotypes/Stereotype'):
                if st.get('Name', '').lower() == 'discriminant':
                    gi.setDiscriminator(at.get('Name', ''))
                    found = True
                    break
            if found:
                break
        if not found:
            raise ValueError(
                f"No discriminator found in parent class Id={gi.getIdParent()}"
            )

    def joinGeneralizations(self, generales: dict[str, Generalization], gi: Generalization_single) -> None:
        key = gi.getNameParent()
        if key not in generales:
            g = Generalization()
            g.setName(gi.getName())
            g.setNameParent(gi.getNameParent())
            g.setDisjoint(gi.getDisjoint())
            g.setComplete(gi.getComplete())
            g.setDiscriminator(gi.getDiscriminator())
            g.setNamesChildren([gi.getNameChild()])
            generales[key] = g
        else:
            g = generales[key]


            if (g.getDisjoint() != gi.getDisjoint() or
                g.getComplete() != gi.getComplete() or
                g.getDiscriminator() != gi.getDiscriminator()):
                raise ValueError(
                    f"Conflict in the generalizations of parent '{key}'"
                )
            
            fills = g.getNamesChildren()
            fills.append(gi.getNameChild())
            g.setNamesChildren(fills)




    def getMultiplicities(self, mult: str) -> (str, str):
        if '..' in mult:
            return mult.split('..', 1)
        else:
            return mult, mult

    def getClassEnd(self, assoc_end: ET.Element) -> str:
        cls = assoc_end.find('Type/Class')
        if cls is not None:
            name, count = self.getNameCount(cls.get('Name', ''))
            return name
        else: return ''

    def getClassID(self, root: ET.Element, id: str) -> str:
        c = root.find(f".//Class[@Id='{id}']")
        if c:
            name, count = self.getNameCount(c.get('Name', ''))
            return name
        else: raise ValueError(f"The class with Id={id} does not exist.")
        
        
    def getNameCount(self, namecount: str) -> (str, str):
        name = ""
        count = ""
    
        if '#' in namecount:
            name, count = namecount.split('#', 1)
            if not count.isdigit():
                raise ValueError(f"Instance count must be an integer, got '{count}'")
        else:
            name = namecount
            
        return name, count