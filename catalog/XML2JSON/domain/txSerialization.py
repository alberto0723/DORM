from .classUML import ClassUML
from .association import Association
from .generalization import Generalization

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
            lines.append('  "classes": [')
            lines.append(self.createJSON_Classes(classes))
            lines.append('    ],')
        if assocs:
            lines.append('  "associations": [')
            lines.append(self.createJSON_Associations(assocs))
            lines.append('    ],')
        if generals:
            lines.append('  "generalizations": [')
            lines.append(self.createJSON_Generalitzacions(generals))
            lines.append('    ]')
        lines.append('  }')
        return "\n".join(lines)

    def createJSON_Classes(self, clases: list[ClassUML]) -> str:
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
        return ',\n'.join(class_strs)

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

    def createJSON_Associations(self, assocs: list[Association]) -> str:
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
        return ',\n'.join(assoc_strs)

    def getEndJSON(self, cls, name, min, max) -> list[str]:
        lines: list[str] = []

        lines.append("        {")
        lines.append(f'          "class": "{cls}",')
        if name is None or len(name) == 0:
            lines.append(f'          "prop": {{"End_name": null, "MultiplicityMin": {min}, "MultiplicityMax": {max} }}')
        else:
            lines.append(f'          "prop": {{"End_name": "{name}", "MultiplicityMin": {min}, "MultiplicityMax": {max} }}')
        lines.append("          }")
        return lines

    def getEndsJSON(self, ir: Association) -> list[str]:
        lines: list[str] = []

        lines.extend(self.getEndJSON(ir.getNameClassFrom(), ir.getNameFrom(), ir.getMulFromMin(), ir.getMulFromMax()))
        lines.append('          ,')
        lines.extend(self.getEndJSON(ir.getNameClassTo(), ir.getNameTo(), ir.getMulToMin(), ir.getMulToMax()))
        return lines

    def createJSON_Generalitzacions(self, generals: list[Generalization]) -> str:
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
            block.append(subclass_lines)
            block.append('        ]')
            block.append('      }')
            gen_strs.append("\n".join(block))

        return ',\n'.join(gen_strs)

    def getChildrenJSON(self, g: Generalization) -> str:
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
            
        return ',\n'.join(child_strs)
