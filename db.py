from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import json
from dataclasses_json import dataclass_json
import db_api
import db_table
import datetime as dt

dic_types = {'int': int, 'str': str, 'datetime': dt.datetime}


def get_matching_rows(all_table_rows, row_to_match, fields_to_join_by):
    relevent_rows = []
    for i, file_rows in enumerate(all_table_rows):

        relevent_rows.append([])
        for row in file_rows:
            criteria = [db_api.SelectionCriteria(field, '=', row_to_match[field]) for field in fields_to_join_by]
            if db_table.is_relevent_row(row, criteria):
                relevent_rows[i].append(row)

    return relevent_rows


def merge_matching_rows(first_table_rows, other_table_rows, fields_to_join_by):
    joined_rows = []
    for i in range(len(first_table_rows)):
        for k in range(len(other_table_rows[i])):
            for j in range(len(other_table_rows[i][k])):
                for field in fields_to_join_by:
                    del other_table_rows[i][k][j][field]
                    first_table_rows[i].update(other_table_rows[i][k][j])
                    joined_rows.append(first_table_rows[i])

    return joined_rows


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        try:
            db_table.table_list = json.load(open(f'{db_api.DB_ROOT}/metadata.json'))

        except FileNotFoundError:
            db_table.write_to_json(f'{db_api.DB_ROOT}/metadata.json', {})

    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str) -> db_api.DBTable:
        if table_name in db_table.table_list:
            raise Exception

        table = db_table.DBTable(table_name, fields, key_field_name)
        db_table.table_list.update({table_name: {'name': table.name,
                                                 'fields': [(field.name, field.type.__name__) for field in fields],
                                                 'key_field_name': table.key_field_name,
                                                 'amount': table.amount}})

        db_table.write_to_json(f'{db_api.DB_ROOT}/metadata.json', db_table.table_list)
        return table

    def num_tables(self) -> int:
        return len(db_table.table_list)

    def get_table(self, table_name: str) -> db_api.DBTable:
        if table_name not in db_table.table_list:
            raise Exception

        table = db_table.table_list[table_name]
        return db_table.DBTable(table['name'],
                                [db_api.DBField(field[0], dic_types[field[1]]) for field in table['fields']],
                                table['key_field_name'],
                                table['amount'])

    def delete_table(self, table_name: str) -> None:
        if table_name not in db_table.table_list:
            raise Exception

        if db_table.table_list[table_name]['amount']:
            for i in range(db_table.table_list[table_name]['amount']):
                path = Path(f'{db_api.DB_ROOT}/{table_name}_{i+1}.csv')
                path.unlink()

        del db_table.table_list[table_name]
        db_table.write_to_json(f'{db_api.DB_ROOT}/metadata.json', db_table.table_list)

    def get_tables_names(self) -> List[Any]:
        return list(db_table.table_list.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:

        queried_rows = self.get_queried_tables(tables, fields_and_values_list)
        rows_to_join = [get_matching_rows(queried_rows[1:], row, fields_to_join_by) for row in queried_rows[0]]

        return merge_matching_rows(queried_rows[0], rows_to_join, fields_to_join_by)

    def get_queried_tables(self, tables, fields_and_values_list) -> List[List[Dict[str, Any]]]:
        return [self.get_table(tables[i]).query_table(fields_and_values_list[i]) for i in range(len(tables))]
