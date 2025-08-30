from pathlib import Path
from sqlglot import parse
from sqlglot.expressions import Create, Table, Schema

base_path = Path("data")

rename_id = {
    "galspecextra_specobjid": "specobjall_specobjid",
    "galspecindx_specobjid": "specobjall_specobjid",
    "specobj_specobjid": "specobjall_specobjid",
    "zoospec_specobjid": "specobjall_specobjid",
    "photoprimary_objid": "photoobjall_objid",
    "photoobj_objid": "photoobjall_objid",
    "photoz_objid": "photoobjall_objid"
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
        with open(base_path.joinpath("insertions_"+table_name + ".sql"), "w") as output:
            output.write(f"INSERT INTO dorm_edbt_baseline.{table_name}(")
            columns = schema.expressions
            # print(statement.args) # This shows all the content in the statement, with the corresponding nested structure
            source_cols = []
            target_cols = []
            for col in columns:
                attr_id += 1
                if col.this.quoted:
                    col_name = str(col.this)[1:-1]
                else:
                    col_name = str(col.this)
                source_cols.append(col_name)
                target_col = table_name+"_"+col_name
                if target_col in rename_id:
                    target_col = rename_id[target_col]
                target_cols.append(target_col)
            output.write(", ".join(target_cols))
            output.write(")\nSELECT ")
            output.write(", ".join(source_cols))
            output.write(f" \nFROM relational2.{table_name};")
