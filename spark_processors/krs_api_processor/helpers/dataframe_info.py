from typing import Any, NamedTuple


class DFInfo(NamedTuple):
    table_name:str
    table_filepath:str
    table_dataframe:any