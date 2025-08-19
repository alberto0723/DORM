import xml.etree.ElementTree as ET
from pathlib import Path

from .classUML import ClassUML
from .attribute import Attribute
from .association import Association
from .generalization import Generalization, Generalization_single


class TxParsing:
    def __init__(self):
        self.ListClasses: list[ClassUML] = []
        self.ListAssociations: list[Association] = []
        self.ListGeneralizations: list[Generalization] = []

    def getClasses(self) -> list[ClassUML]:
        return self.ListClasses

    def getAssociations(self) -> list[Association]:
        return self.ListAssociations

    def getGeneralizations(self) -> list[Generalization]:
        return self.ListGeneralizations

    def loadElements(self, root: Path):
        if not root or not root.exists():
            raise FileNotFoundError(f"File not found: '{root}'")
        try:
            tree = ET.parse(root)
            root_elem = tree.getroot()
        except Exception as e:
            raise RuntimeError(f"Error parsing the XML '{root}': {e}")
    
        self.ListClasses = self.loadClasses(root_elem)
        self.ListAssociations = self.loadAssociations(root_elem)
        self.ListGeneralizations = self.loadGeneralizations(root_elem)

    def loadClasses(self, root: ET.Element) -> list[ClassUML]:
        models_xml = root.find('Models')
        classes: list[ClassUML] = []

        for e in models_xml.findall('Class'):
            c = ClassUML()
            name, count = self.getNameCount(e.get('Name', ''))
            
            c.setName(name)
            c.setCount(count)
            c.setID(e.get('Id', ''))
            c.setListAttributes(self.importAttributesClass(e))
            classes.append(c)
        return classes

    def importAttributesClass(self, class_elem: ET.Element) -> list[Attribute]:
        atributs_list: list[Attribute] = []
        
        model_children = class_elem.find('ModelChildren')
        if model_children is None:
            return []

        for elem in model_children.findall('Attribute'):
            at = Attribute()
            at.setID(elem.get('Id', ''))
            at.setName(elem.get('Name', '').replace(' ', '_'))
            at.setSize(elem.get('TypeModifier', None))
            at.setDistinctVals(elem.get('Multiplicity', None))
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
    
    def loadAssociations(self, root: ET.Element) -> list[Association]:
        models = root.find('Models')
        assocs: list[Association] = []

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
            i.setID(assoc.get('Id', ''))
            i.setName(assoc.get('Name', ''))

            # FromEnd
            fe = assoc.find('FromEnd/AssociationEnd')

            if fe is not None:
                i.setIdFrom(fe.get('Id', ''))
                i.setNameFrom(fe.get('Name', None))
                i.setNameClassFrom(self.getClassEnd(fe))
                minf, maxf = self.getMultiplicities(fe.get('Multiplicity', ''))
                i.setMulFromMin(minf)
                i.setMulFromMax(maxf)

            # ToEnd
            te = assoc.find('ToEnd/AssociationEnd')
            if te is not None:
                i.setIdTo(te.get('Id', ''))
                i.setNameTo(te.get('Name', None))
                i.setNameClassTo(self.getClassEnd(te))
                mint, maxt = self.getMultiplicities(te.get('Multiplicity', ''))
                i.setMulToMin(mint)
                i.setMulToMax(maxt)

            assocs.append(i)
        return assocs

    def loadGeneralizations(self, root: ET.Element) -> list[Generalization]:
        models = root.find('Models')
        if models is None:
            return []

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

        temp_inds = {}
        for gen in gen_list:
            iden = gen.get('Id', '')
            temp_inds[iden] = self.generateSingleGeneralization(root, gen)

        return self.joinGeneralizations(root, temp_inds)

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
        
        return gi

    def asignarDiscriminator(self, root: ET.Element, gi: Generalization) -> None:
        class_elem = root.find(f".//Class[@Id='{gi.getIdParent()}']")
        if class_elem is None:
            raise ValueError(f"The parent class with Id={gi.getIdParent()} does not exist.")
        found = False
        
        model_children = class_elem.find('ModelChildren')
        if model_children is None:
            return
        
        for at in model_children.findall('Attribute'):
            for st in at.findall('Stereotypes/Stereotype'):
                if st.get('Name', '') == gi.getName():
                    gi.setDiscriminator(at.get('Name', ''))
                    found = True
                    break
            if found:
                break
        if not found:
            raise ValueError(
                f"No discriminator found in parent class Id={gi.getIdParent()}"
            )

    def joinGeneralizations(self, root: ET.Element, generales: dict[str, Generalization_single]) -> list[Generalization]:
        models_xml = root.find('Models')
        
        generalizations_list = []
        
        for gen_set in models_xml.findall('GeneralizationSet'):
            gens = gen_set.find('Generalizations')
            if gens is None:
                continue
            g = Generalization()
            
            g.setID(gen_set.get('Id', ''))
            g.setName(gen_set.get('Name', ''))
            
            id_parent = None
            
            for gen_single in gens.findall('Generalization'):
                iden = gen_single.get('Idref')
                gen = generales[iden]
                if id_parent is None:
                    id_parent = gen.getIdParent()
                    g.setIdParent(id_parent)
                    g.setNameParent(gen.getNameParent())
                    
                elif id_parent != gen.getIdParent():
                    raise ValueError(f"Children classes in GeneralizationSet '{g.getID()}' do not have the same parent class")
                
                g.addNameChild(gen.getNameChild())

            if gen_set.get('Covering') == 'true':
                g.setComplete(True)
            if gen_set.get('Disjoint') == 'true':
                g.setDisjoint(True)
            self.asignarDiscriminator(root, g)

            generalizations_list.append(g)
        
        return generalizations_list

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
        else:
            return ''

    def getClassID(self, root: ET.Element, id: str) -> str:
        c = root.find(f".//Class[@Id='{id}']")
        if c is not None:
            name, count = self.getNameCount(c.get('Name', ''))
            return name
        else:
            raise ValueError(f"The class with Id={id} does not exist.")

    def getNameCount(self, namecount: str) -> (str, str):
        count = "null"
    
        if '#' in namecount:
            name, count = namecount.split('#', 1)
            if not count.isdigit():
                raise ValueError(f"Instance count must be an integer, got '{count}'")
        else:
            name = namecount
            
        return name, count
