from typing import List, Union

from set import Set
from structure import Structure
from classUML import ClassUML

Node = Union[Set, Structure, ClassUML]

class TxSerialization:
    def __init__(self):
        self.componentsList = []
        self.mapComponents: dict[str, Node] = {}

    def setComponents(self, components):
        self.componentsList = list(components)

    def setMapComponents(self, map_components):
        self.mapComponents = dict(map_components)

    def createJSON(self) -> str:    
        lines: List[str] = []
        lines.append('{')

        lines.append('    "hyperedges": [')

        components_strs: List[str] = []
        for comp in self.componentsList:
            comp_element = self.mapComponents[comp]
            components_strs.append(self.createJSON_Components(comp_element))
            
        lines.append(",\n".join(components_strs))
        
        lines.append('    ]')
        lines.append('}')
        return "\n".join(lines)

    def createJSON_Components(self, comp) -> str:
        comp_strs: List[str] = []
            
        if isinstance(comp, Structure):
            comp_strs.append(f'    {{"kind": "Struct"')
            comp_strs.append(f'        "name": "{comp.getName()}"')
            anchor_strs: List[str] = []
            elem_strs: List[str] = []
            
            anchor_list = comp.getAnchors()
            for anchor in anchor_list:
                anchor_strs.append('"' + anchor + '"')
                    
            element_list = comp.getElements()
            for elem in element_list:
                elem_strs.append('"' + elem + '"')
                    
            comp_strs.append(f'        "anchor": [{",".join(anchor_strs)}]')
            comp_strs.append(f'        "elements": [{",".join(elem_strs)}]}}')
            
            
        elif isinstance(comp, Set):
            comp_strs.append(f'    {{"kind": "Set"')
            comp_strs.append(f'        "name": "{comp.getName()}"')
            elem_strs: List[str] = []
            
            element_list = comp.getElements()
            for elem in element_list:
                    elem_strs.append('"' + elem + '"')
                    
            comp_strs.append(f'        "elements": [{",".join(elem_strs)}]}}')
        
        return ",\n".join(comp_strs)
