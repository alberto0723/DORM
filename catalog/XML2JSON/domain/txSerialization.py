from classUML import ClassUML
from association import Association
from generalization import Generalization

class TxSerialization:
    def __init__(self):
        self.ListClasses: list[ClassUML] = []
        self.ListAssociations: list[Association] = []
        self.ListGeneralizations: list[Generalization] = []

    def setClasses(self, list_classes):
        self.ListClasses = list_classes

    def setAssociations(self, list_interrelacions):
        self.ListAssociations = list_interrelacions

    def setGeneralitzacions(self, list_generalitzacions):
        self.ListGeneralizations = list_generalitzacions

    def createJSON(self) -> str:
        classes = self.ListClasses
        assocs = self.ListAssociations
        generals = self.ListGeneralizations

        lines: list[str] = []
        lines.append('{')

        if classes:
            classes_str = ['  "classes": [']
            classes_str.extend(self.createJSON_Classes(classes))
            lines.extend(classes_str)
            
        if assocs:
            assocs_str = ['']
            assocs_str.append('    ],')
            assocs_str.append('  "associations": [')
            assocs_str.extend(self.createJSON_Associations(assocs))
            lines.extend(assocs_str)
            
        if generals:
            generals_str = ['    ],']
            generals_str.append('  "generalizations": [')
            generals_str.extend(self.createJSON_Generalitzacions(generals))
            lines.extend(generals_str)

        lines.append('    ]')
        lines.append('  }')
        return "\n".join(lines)

    def createJSON_Classes(self, clases: list[ClassUML]) -> list[str]:
        lines = []
        class_strs: list[str] = []
        for c in clases:
            cls_block: list[str] = []
            cls_block.append('    {')    
            cls_block.append(f'      "name": "{c.getName()}",')
            cls_block.append(f'      "prop": {{ "Count": {c.getCount()} }},')
            cls_block.append(f'      "attr": [')
            attr_lines = self.createJSON_Atributs(c)
            cls_block.append(attr_lines)
            cls_block.append('        ]')
            cls_block.append('      }')
            class_strs.append("\n".join(cls_block))
        # TODO: Jo diria que "lines" realment te nomes un element, no? Potser millor retornar un unic strig (igual que a la seguent funcio) per a no confondre
        lines.append(',\n'.join(class_strs))
        return lines

    def createJSON_Atributs(self, c: ClassUML) -> str:
        lines: list[str] = []
        for at in c.getListAttributes():
            prop = (
                f'        {{ "name": "{at.getName()}", '
                f'"prop": {{"DataType": "{at.getDatatype()}", '
                f'"Size": {at.getSize()}, '
                f'"DistinctVals": {at.getDistinctVals()}, '
                f'"Identifier": {str(at.getIdentifier()).lower()}}}}}'
            )
            lines.append(prop)
        return ",\n".join(lines)

    def createJSON_Associations(self, assocs: list[Association]) -> list[str]:
        lines = []
        assoc_strs: list[str] = []
        for ir in assocs:
            ends = self.getEndsJSON(ir)
            block = ['    {']
            block.append(f'      "name": "{ir.getName()}",')
            block.append(f'      "ends": [')
            
            block.extend(ends)
            block.append('        ]')
            block.append('      }')
            assoc_strs.append("\n".join(block))
        # TODO: De nou, peso que la llista nomes pot tenir un element
        lines.append(',\n'.join(assoc_strs))
        return lines

    def getEndsJSON(self, ir: Association) -> list[str]:
        lines: list[str] = []
        
        lines.append("        {")
        cls_from = ir.getNameClassFrom()
        name_from = ir.getNameFrom()
        min_from = ir.getMulFromMin()
        max_from = ir.getMulFromMax()
        
        lines.append(f'          "class": "{cls_from}",')
        line_prop = (
            f'          "prop": {{"End_name": "{name_from}", '
            f'"MultiplicityMin": {min_from}, '
            f'"MultiplicityMax": {max_from} }}'
        )
        lines.append(line_prop)
        lines.append("          },")

        lines.append("        {")
        cls_to = ir.getNameClassTo()
        name_to = ir.getNameTo()
        min_to = ir.getMulToMin()
        max_to = ir.getMulToMax()
        
        lines.append(f'          "class": "{cls_to}",')
        line_prop = (
            f'          "prop": {{"End_name": "{name_to}", '
            f'"MultiplicityMin": {min_to}, '
            f'"MultiplicityMax": {max_to} }}'
        )
        lines.append(line_prop)
        lines.append("          }")

        return lines

    def createJSON_Generalitzacions(self, generals: list[Generalization]) -> list[str]:
        lines = []
        gen_strs: list[str] = []
        for g in generals:
            subclass_lines = self.getChildrenJSON(g)
            block = ["      {"]
            block.append(f'        "name": "{g.getName()}",')
            block.append(f'        "prop": {{')
            block.append(f'          "Disjoint": {str(g.getDisjoint()).lower()},')
            block.append(f'          "Complete": {str(g.getComplete()).lower()}')
            block.append('        },')
            block.append(f'        "superclass": "{g.getNameParent()}",')
            block.append('        "subclasses": [')
            block.extend(subclass_lines)
            block.append('        ]')
            block.append('      }')
            gen_strs.append("\n".join(block))
        # TODO: la llista nomes pot tenir un element
        lines.append(',\n'.join(gen_strs))
        return lines

    def getChildrenJSON(self, g: Generalization) -> list[str]:
        lines: list[str] = []
        child_strs: list[str] = []
        for child in g.getNamesChildren():
            constraint = f"{g.getDiscriminator()}='{child.lower()}'"
            block = ['          {']
            block.append(f'            "class": "{child}",')
            block.append('            "prop": {')
            block.append(f'              "Constraint": "{constraint}"')
            block.append('            }')
            block.append('          }')
            child_strs.append("\n".join(block))
        # TODO: La llista nomes pot tenir un element
        lines.append(',\n'.join(child_strs))
        return lines
