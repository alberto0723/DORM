# UML to JSON Translator

This Python program parses UML models exported as XML from Visual Paradigm and converts them into a specific JSON domain schema. It handles the following elements:
- **Classes**, including instance count and attributes
- **Binary Associations** between two classes
- **Generalizations**, between a parent class and its children classes

---

## Project Structure

The project consists of the following eight Python modules:

1. **classUML.py**
   - Defines `ClassUML`, representing a UML class with:
     - `ID`: Class ID
     - `name`: Class name
     - `count`: Number of instances
     - `attributes`: Attributes of the class

2. **attribute.py**
   - Defines `Attribute`, representing a UML attribute with:
     - `ID`
     - `name`
     - `datatype`
     - `size`
     - `distinctvals`
     - `identifier`

3. **association.py**
   - Defines `Association`, representing a binary relationship between two classes, with:
     - `ID`
     - `name`
     - `nameClassFrom`: Source class name
     - `nameFrom`: Source name
     - `mulFromMin`. minimum multiplicity of the source class
     - `mulFromMax`. maximum multiplicity of the source class

     - `nameClassTo`: Target class name
     - `nameTo`: Target name
     - `mulToMin`. minimum multiplicity of the target class
     - `mulToMax`. maximum multiplicity of the target class

4. **generalization.py**
   - Defines `Generalization`, grouping one parent class with all its child classes.
   - Defines `GeneralizationSingle`, representing a single parent–child pair, with:
     - `ID`
     - `name`
     - `nameParent`
     - `nameChild`
     - `disjoint`: indicates if the generalization is disjoint
     - `complete`: indicates if the generalization is complete
     - `discriminator`: name of the discriminator attribute

5. **txParsing.py**
   - Contains `TxParsing`, responsible for:
     - Loading and parsing the XML model
     - Extracting data on classes, attributes, associations, and generalizations
     - Validating required metadata (e.g., discriminants, stereotypes)

6. **txSerialization.py**
   - Contains `TxSerialization`, responsible for:
     - Taking parsed data from `TxParsing`
     - Building the target JSON structure

7. **translator.py**
   - The main entry point. Defines:
     - `def translate(root: str) -> str:`:
       - Loads the XML file at `xml_path`
       - Invokes `TxParsing` and `TxSerialization`
       - Returns the resulting JSON

---

## Installation & Usage

1. **Prerequisites**: Python 3.7 or newer.  
2. **Run the translator:**:
   ``` bash
   python main.py
   ```

---

## UML Schema Requirements in Visual Paradigm

To ensure all elements map correctly, configure your Visual Paradigm UML model with the following conventions:

### 1. Class Instance Count

- Append `#<number>` to the UML **class name** to indicate its instance count.  
  - Example: `Book#4000` ⇒ 4000 instances of the `Book` class.

### 2. Attribute Metadata

- **Size**: Enter the `size` value in the **Type Modifier** field of the attribute.  
- **Distinct Values**: Enter the `distinctvals` value in the **Multiplicity** field.  
- **Identifier**: In the attribute’s **Specification** dialog, check the **ID** box if it is a primary identifier.

### 3. Generalizations

For related generalizations, create a **GeneralizationSet** that groups all individual generalization lines sharing the same parent.

- **Disjoint**: in the GeneralizationSet’s properties, you may check **Disjoint** to ensure child sets do not overlap.  
- **Complete (Covering)**: in the same dialog, you may check **Covering** so that the children collectively cover all instances of the parent.  

On the **parent class**, add a **stereotype** whose name exactly matches the GeneralizationSet name and apply it to the attribute that will serve as the discriminant.  

- In JSON output, the discriminant attribute’s value will be the child’s class name for each instance.

---

## Example JSON Output
``` bash
{
  "classes": [
    {
      "name": "People",
      "prop": { "Count": 4000 },
      "attr": [
        { "name": "P_ID", "prop": {"DataType": "Integer", "Size": 4, "DistinctVals": 4000, "Identifier": true}},
        { "name": "P_name", "prop": {"DataType": "String", "Size": 20, "DistinctVals": 3900, "Identifier": false}},
        { "name": "P_occupation", "prop": {"DataType": "String", "Size": 10, "DistinctVals": 2, "Identifier": false}}
        ]
      },
    {
      "name": "Students",
      "prop": { "Count": 2000 },
      "attr": [
        { "name": "S_university", "prop": {"DataType": "String", "Size": 20, "DistinctVals": 100, "Identifier": false}}
        ]
      },
    {
      "name": "Workers",
      "prop": { "Count": 2000 },
      "attr": [
        { "name": "W_company", "prop": {"DataType": "String", "Size": 50, "DistinctVals": 100, "Identifier": false}}
        ]
      }
    ],
  "associations": [
    {
      "name": "Friends",
      "ends": [
        {
          "class": "People",
          "prop": {"End_name": "Friends_People", "MultiplicityMin": 0, "MultiplicityMax": 50 }
          },
        {
          "class": "Workers",
          "prop": {"End_name": "Friends_Workers", "MultiplicityMin": 0, "MultiplicityMax": 50 }
          }
        ]
      }
    ],
  "generalizations": [
      {
        "name": "G1",
        "prop": {
          "Disjoint": true,
          "Complete": true
        },
        "superclass": "People",
        "subclasses": [
          {
            "class": "Students",
            "prop": {
              "Constraint": "P_occupation='students'"
            }
          },
          {
            "class": "Workers",
            "prop": {
              "Constraint": "P_occupation='workers'"
            }
          }
        ]
      }
    ]
  }
```
