from pathlib import Path
from sqlglot import parse
from sqlglot.expressions import Create, Table, Schema

base_path = Path("data")

# Some empty design must be generated, type names updated, XML exported and type IDs taken from that XML to the following dictionary
type_id = {
    "BIGINT": "tpfnXhmFYHAgAQes",
    "FLOAT": "NpfnXhmFYHAgAQeq",
    "TEXT": "tpfnXhmFYHAgAQev",
}
type_size = {
    "BIGINT": "8",
    "FLOAT": "8",
    "TEXT": "100",
}


# Read SQL from file
with open(base_path.joinpath("sdss_relational2.sql"), "r") as input:
    sql = input.read()

# Parse SQL into an AST (Abstract Syntax Tree)
statements = parse(sql)
attr_id = 0

# Print parsed expressions (AST)
for statement in statements:
    if isinstance(statement, Create) and statement.kind == "TABLE":
        schema = statement.this
        table = schema.this
        table_name = str(table.this)
        print("Table name: ", table_name)
        with open(base_path.joinpath(table_name + ".xml"), "w") as output:
            output.write("<ModelChildren>\n")
            columns = schema.expressions
            # print(statement.args) # This shows all the content in the statement, with the corresponding nested structure
            for col in columns:
                attr_id += 1
                if col.this.quoted:
                    col_name = str(col.this)[1:-1]
                else:
                    col_name = str(col.this)
                col_type = str(col.kind)
                #output.write(table_name+"_"+col_name+" "+col_type+" "+type_size[col_type]+"\n")
                output.write(f'<Attribute Abstract="false" Aggregation="None" AllowEmptyName="false" BacklogActivityId="0" ConnectToCodeModel="1" Derived="false" DerivedUnion="false" Documentation_plain="" HasGetter="false" HasSetter="false" Id="{str(attr_id)}" IsID="false" Leaf="false" Multiplicity="Unspecified" Name="{table_name+"_"+col_name}" PmAuthor="alberto.abello" PmCreateDateTime="2025-08-08T14:22:03.680" PmLastModified="2025-08-08T14:23:41.457" QualityReason_IsNull="true" QualityScore="-1" ReadOnly="false" Scope="instance" TypeModifier="{type_size[col_type]}" UserIDLastNumericValue="0" UserID_IsNull="true" Visibility="private" Visible="true">\n')
                output.write('	<Type>\n')
                output.write(f'		<DataType Idref="{type_id[col_type]}" Name="{col_type}"/>\n')
                output.write('	</Type>\n')
                output.write('</Attribute>\n')
            output.write("</ModelChildren>\n")

# The content of each file needs to be embedded inside the corresponding class element in the exported XML
# Then, the XML can be imported back in VP
# Identifiers of each class and any other relevant information needs to be manually added a posteriory from Visual Paradigm GUI