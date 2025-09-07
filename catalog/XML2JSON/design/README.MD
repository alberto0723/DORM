# UML to JSON Translator

This Python program parses UML models exported as XML from Visual Paradigm and converts them into a specific JSON design schema. It handles the following elements:
- **Structures**
- **Sets**

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

2. **structure.py**
  - Represents a UML structure grouping anchors and elements.
    - `ID`: Unique identifier of the structure.
    - `name`: Name of the structure.
    - `anchors`: List of names for anchor points within the structure.
    - `elements`: List of names for elements that compose the structure.

3. **set.py**
  - Represents a collection of elements grouped under a common name.
    - `ID`: Unique identifier of the set.
    - `name`: Name of the set.
    - `elements`: List of names for elements contained in the set.

4. **txParsing.py**
   - Contains `TxParsing`, responsible for:
     - Loading and parsing the XML model
     - Extracting data on classes, attributes, associations, and generalizations
     - Validating required metadata (e.g., discriminants, stereotypes)

5. **txSerialization.py**
   - Contains `TxSerialization`, responsible for:
     - Taking parsed data from `TxParsing`
     - Building the target JSON structure

6. **translator.py**
   - The main entry point. Defines:
     - `def translate(root: str) -> str:`:
       - Loads the XML file at `xml_path`
       - Invokes `TxParsing` and `TxSerialization`
       - Returns the resulting JSON

---

## UML Schema Requirements in Visual Paradigm

To ensure all elements map correctly, configure your Visual Paradigm UML model with the following conventions:

### 1. Creating the elements in Visual Paradigm
- **Structures** and **Sets** must be created using Visual Paradigm’s element called **Model** (using packets will not work).  
- **Classes** must be created using the **Class** element.  
- **Associations** must be created using the **Association** element and needs to be assigned between two classes.
- **Association Classes**
  - To create an association class:
    1. Create the base **Class**.
    2. Create the **Association** between the two target classes.
    3. Use the **Association Class Link** element to link your new class to that association.

### 2. Stereotypes for Identification
To distinguish element types at runtime, define and assign these stereotypes (names must match exactly):
- `Set` – assign to Models representing sets.
- `Struct` – assign to Models representing structures.
- `Anchor` – assign to Classes or Associations that should be treated as anchor points rather than standard elements.

### 3. Domain Reference Configuration
To link your domain file in Visual Paradigm:
1. Right‑click the diagram background and select **Diagram Specification**.
2. Go to the **References** section.
3. Add your domain file and enter **domain** (all lowercase) in the **Description** field.

### 4. Associations Outside a Common Struct
If two classes share no common structure parent, and you need to place their association in a specific structure:
- Create a stereotype named exactly as the target structure.
- Assign this stereotype to the association; it will then appear under that structure in the resulting JSON.
- It could also happen that one of the classes does not exist in the design (this may be because it is a superclass and we decided to explicitely store only its subclasses).
If this is the case, just point the association to any of the subclasses in the design (as soon as the domain points to the right one, this does not matter).

---

## Example JSON Output
``` bash
{
    "domain": "..\\..\\domains\\artist-record.json",
    "hyperedges": [
    {"kind": "Struct",
        "name": "Artists_type",
        "anchor": ["Artist"],
        "elements": ["A_name"]},
    {"kind": "Set",
        "name": "Artists_table",
        "elements": ["Artists_type"]},
    {"kind": "Struct",
        "name": "Records_type",
        "anchor": ["Record"],
        "elements": ["R_name"]},
    {"kind": "Set",
        "name": "Records_table",
        "elements": ["Records_type"]},
    {"kind": "Struct",
        "name": "Made_type",
        "anchor": ["Made"],
        "elements": []},
    {"kind": "Set",
        "name": "Made_table",
        "elements": ["Made_type"]}
]
}
```
