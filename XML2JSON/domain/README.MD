# UML to JSON Translator

This Python program parses UML models exported as XML from Visual Paradigm and converts them into a specific JSON domain schema. It handles the following elements:
- **Classes**, including instance count and attributes
- **Binary Associations** between two classes
- **Generalizations**, between a parent class and its children classes


---

## Installation & Usage

1. **Prerequisites**: 
 - `Python`: 3.7 or newer.  
 - `Visual Paradigm`: 17.2 

2. **Run the translator**:
   ``` bash
   python main.py
   ```

---

## Exporting XML from Visual Paradigm

To use this translator with Visual Paradigm, it is important to export the UML schema in **Simple XML format**. 
The translator is not compatible with Visual Paradigm's traditional XML export format. 
To ensure compatibility, select the *Simple* export format in Visual Paradigm  (i.e., choose *Simple* under *XML Structure* in the *Export to XML* dialog) before running the tool on the generated XML file.

---

## Project Structure

The project consists of the following eight Python modules:

1. **classUML.py**
  - Represents a UML class with its defining properties.
    - `ID`: Unique identifier of the UML class.
    - `name`: Name of the UML class.
    - `count`: Number of instances of the class.
    - `attributes`: List of `Attribute` objects representing each attribute of the class.

2. **attribute.py**
  - Represents a property of a UML class.
    - `ID`: Unique identifier of the attribute.
    - `name`: Name of the attribute.
    - `datatype`: Data type of the attribute.
    - `size`: Maximum allowed length or size of the attribute.
    - `distinctvals`: Number of distinct values that this attribute can take.
    - `identifier`: Boolean flag; if `True`, this attribute serves as an identifier (primary key).

3. **association.py**
  - Represents a UML association between two classes.
    - `ID`: Unique identifier of the association.
    - `name`: Name of the association.

    - `nameFrom`: Role name at the source end of the association.
    - `nameClassFrom`: Name of the source class in the association.
    - `idFrom`: Unique identifier of the source class.
    - `mulFromMin`: Minimum multiplicity at the source end.
    - `mulFromMax`: Maximum multiplicity at the source end.

    - `nameTo`: Role name at the target end of the association.
    - `nameClassTo`: Name of the target class in the association.
    - `idTo`: Unique identifier of the target class.
    - `mulToMin`: Minimum multiplicity at the target end.
    - `mulToMax`: Maximum multiplicity at the target end.

4. **generalization.py**
  - Defines two classes for modeling UML‑style generalizations (inheritance relationships) in your data:

  - `Generalization`: represents a single parent class together with _multiple_ child classes.
    - `ID`: Unique identifier of the individual generalization.
    - `name`: Name of the generalization.
    - `nameParent`: Name of the parent class.
    - `idParent`: Unique identifier of the parent class.
    - `namesChildren`: List of the names of all child classes under this generalization.
    - `disjoint`: Boolean flag; if `True`, no two child classes overlap.
    - `complete`: Boolean flag; if `True`, every possible subclass must appear in `namesChildren`.
    - `discriminator`: Name of the attribute used to distinguish which child class an instance belongs to.

  - `GeneralizationSingle`: represents one specific parent–child pair (a single branch of a generalization).
    - `ID`: Unique identifier of the individual generalization.
    - `name`: Name of the generalization.
    - `nameParent`: Name of the parent class.
    - `idParent`: Unique identifier of the parent class.
    - `nameChild`: Name of the child class.
    - `idChild`: Unique identifier of the child class.

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
