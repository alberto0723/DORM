from sqlglot import parse
from sqlglot.expressions import Create, Table, Schema


type_size = {
    "BIGINT": 8,
    "DOUBLE": 8,
    "TEXT": 100,
}

tables = {}

# Read SQL from file
with open("data/sdss_relational2.sql", "r") as f:
    sql = f.read()

# Parse SQL into an AST (Abstract Syntax Tree)
statements = parse(sql)

# Print parsed expressions (AST)
for statement in statements:
    if isinstance(statement, Create) and statement.kind == "TABLE":
        schema = statement.this
        table = schema.this
        table_name = str(table.this)
        print("------------------------------- Table name: ", table_name)
        columns = schema.expressions
        #print(statement.args)
        attributes = []
        for col in columns:
            if col.this.quoted:
                col_name = str(col.this)[1:-1]
            else:
                col_name = str(col.this)
            col_type = str(col.kind)
            print(table_name+"_"+col_name, col_type, type_size[col_type])
            attributes.append({"name": col_name, "type": col_type, "size": type_size[col_type]})
        tables[table_name] = attributes
print(tables)
