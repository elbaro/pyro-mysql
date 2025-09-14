"""Generate documentation for pyro_mysql with type alias support."""

from pathlib import Path

import pdoc
import pdoc.doc
import pdoc.render

# Import the actual module
import pyro_mysql

# Configure pdoc to parse Google-style docstrings
pdoc.render.configure(docformat="google")


def main():
    # Create the module documentation object
    doc = pdoc.doc.Module(pyro_mysql)

    # Create Variable documentation objects for type aliases
    # These will appear as module-level variables in the documentation

    # Add Value type alias
    value_doc = pdoc.doc.Variable(
        modulename="pyro_mysql",
        qualname="Value",
        taken_from=("pyro_mysql", "Value"),
        docstring="""Type alias for the purpose of documenation.

These Python types can be converted to MySQL values:
- None
- bool
- int
- float  
- str
- bytes
- bytearray
- tuple[Any, ...]
- list[Any]
- dict[str, Any]
- datetime.datetime
- datetime.date
- datetime.time
- datetime.timedelta
- time.struct_time
- decimal.Decimal
""",
        annotation="type[None | bool | int | float | str | bytes | bytearray | tuple[Any, ...] | list[Any] | dict[str, Any] | datetime.datetime | datetime.date | datetime.time | datetime.timedelta | time.struct_time | decimal.Decimal]",
        default_value=pdoc.doc.empty,
    )

    # Add Params type alias
    params_doc = pdoc.doc.Variable(
        modulename="pyro_mysql",
        qualname="Params",
        taken_from=("pyro_mysql", "Params"),
        docstring="""Type alias for the purpose of documenation.

Parameters that can be passed to query execution methods:
- None: No parameters
- tuple[Value, ...]: Positional parameters for queries with ? placeholders
- list[Value]: List of parameters for queries with ? placeholders  
- dict[str, Value]: Named parameters for queries with named placeholders

Examples:
No parameters:

    await conn.exec("SELECT * FROM users")

Positional parameters:

    await conn.exec("SELECT * FROM users WHERE id = ?", (123,))

Multiple positional parameters:

    await conn.exec("SELECT * FROM users WHERE age > ? AND city = ?", (18, "NYC"))

Named parameters:

    await conn.exec("SELECT * FROM users WHERE age > :age AND city = :city", dict(age=18, name="NYC"))
""",
        annotation="type[None | tuple[Value, ...] | list[Value] | dict[str, Value]]",
        default_value=pdoc.doc.empty,
    )

    # Add the type aliases to the module's members
    # First, ensure the variables are added to the module's members dict
    if not hasattr(doc, "members"):
        doc.members = {}

    doc.members["Value"] = value_doc
    doc.members["Params"] = params_doc

    # Generate HTML documentation
    all_modules = {"pyro_mysql": doc}

    # Render as HTML
    html = pdoc.render.html_module(module=doc, all_modules=all_modules)

    # Write to file
    output_file = "./docs.html"
    with open(output_file, "w") as f:
        f.write(html)

    print(f"Documentation generated at {output_file}")


if __name__ == "__main__":
    main()
