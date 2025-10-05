"""Generate documentation for pyro_mysql with type alias support."""

from pathlib import Path

import pdoc
import pdoc.doc
import pdoc.render

# Configure pdoc to parse Google-style docstrings
output_directory = Path("./docs")
modules = ["pyro_mysql"]
pdoc.render.configure(docformat="google")


value_doc = pdoc.doc.Variable(
    modulename="pyro_mysql",
    qualname="Value",
    taken_from=("pyro_mysql", "Value"),
    docstring="""Type alias for the purpose of documenation.

These Python types can be converted to MySQL values:
- `None`
- `bool`
- `int`
- `float  `
- `str`
- `bytes`
- `bytearray`
- `tuple[Any, ...]`
- `list[Any]`
- `dict[str, Any]`
- `datetime.datetime`
- `datetime.date`
- `datetime.time`
- `datetime.timedelta`
- `time.struct_time`
- `decimal.Decimal`
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
- `None`: No parameters
- `tuple[Value, ...]`: Positional parameters for queries with ? placeholders
- `list[Value]`: List of parameters for queries with ? placeholders  
- `dict[str, Value]`: Named parameters for queries with named placeholders

Examples:
No parameters:

    `await conn.exec("SELECT * FROM users")`

Positional parameters:

    `await conn.exec("SELECT * FROM users WHERE id = ?", (123,))`

Multiple positional parameters:

    `await conn.exec("SELECT * FROM users WHERE age > ? AND city = ?", (18, "NYC"))`

Named parameters:

    `await conn.exec("SELECT * FROM users WHERE age > :age AND city = :city", dict(age=18, name="NYC"))`
""",
    annotation="type[None | tuple[Value, ...] | list[Value] | dict[str, Value]]",
    default_value=pdoc.doc.empty,
)


def main():
    all_modules: dict[str, pdoc.doc.Module] = {}
    for module_name in pdoc.extract.walk_specs(modules):
        all_modules[module_name] = pdoc.doc.Module.from_name(module_name)

    all_modules["pyro_mysql"].members["Value"] = value_doc
    all_modules["pyro_mysql"].members["Params"] = params_doc

    for module in all_modules.values():
        if module.fullname == "pyro_mysql":
            name = module.fullname
        else:
            name = f"pyro_mysql.{module.fullname}"
        out = pdoc.render.html_module(module, all_modules)
        outfile = output_directory / f"{name.replace('.', '/')}.html"
        outfile.parent.mkdir(parents=True, exist_ok=True)
        outfile.write_bytes(out.encode())

    assert output_directory

    index = pdoc.render.html_index(all_modules)
    if index:
        (output_directory / "index.html").write_bytes(index.encode())

    search = pdoc.render.search_index(all_modules)
    if search:
        (output_directory / "search.js").write_bytes(search.encode())


if __name__ == "__main__":
    main()
