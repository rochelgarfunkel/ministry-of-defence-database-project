from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import csv
import json
import sys
import db_api
from collections import defaultdict
from dataclasses_json import dataclass_json
import datetime as dt

table_list = {}


def get_num_of_records(types):
    types_sum = sum([sys.getsizeof(type) for type in types])
    return 4000 // types_sum


def add_to_end(file_name, values):
    with open(file_name, "a+", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        row = [values[val] for val in values]
        csv_writer.writerow(row)


def get_columns(fields):
    return {field.name: field.type for field in fields}


def is_valid_input(fields, values):
    if len(fields) != len(values):
        return False

    columns = get_columns(fields)
    for val in values:
        if val not in columns or type(values[val]) != columns[val]:
            return False
    return True


def delete_file(file):
    path = Path(file)
    path.unlink()


def reorder_columns(fields, values):
    return {field.name: values[field.name] for field in fields}


def is_relevent_row(row, criteria):
    operators = {'<': lambda x, y: x < y, '>': lambda x, y: x > y, '=': lambda x, y: x == y, '>=': lambda x, y: x >= y, '<=': lambda x, y: x <= y,}
    for c in criteria:
        if not operators[c.operator](row[c.field_name], c.value):
            return False
    return True


def cast(var, type):
    return type(var) if type != dt.datetime else dt.datetime.strptime(var, '%Y-%m-%d %H:%M:%S')


def row_as_dict(row, fields):
    return {field.name: item for item, field in zip(row, fields)}


def read_from_csv(file):
    with open(file, "r", newline='') as csv_file:
        return list(csv.reader(csv_file))


def write_to_csv(file, rows):
    with open(file, "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for row in rows:
            csv_writer.writerow(row)


def write_to_json(file, data):
    with open(file, "w") as json_file:
        json.dump(data, json_file)


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[db_api.DBField]
    key_field_name: str
    amount: int

    def __init__(self, name, fields, key, amount=0):
        self.name = name
        self.fields = fields
        self.key_field_name = key
        self.amount = amount
        self.num_of_records = get_num_of_records([field.type for field in fields])
        self.create_index(key)
        self.key_index = json.load(open(f'{db_api.DB_ROOT}/{self.name}_{key}_index.json'))

    def count(self) -> int:
        if self.amount == 0:
            return 0

        lines = (self.amount - 1) * self.num_of_records
        rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv')
        return len(rows) + lines

    def insert_record(self, values: Dict[str, Any]) -> None:
        if not is_valid_input(self.fields, values) or values[self.key_field_name] in self.key_index:
            raise ValueError

        if not self.count() % self.num_of_records:
            self.update_amount(1)

        add_to_end(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv', reorder_columns(self.fields, values))
        self.update_key_index(values[self.key_field_name], (self.amount, self.count() % self.num_of_records - 1))

    def delete_record(self, key: Any) -> None:
        if key not in self.key_index:
            raise ValueError

        self.delete_from_file(key)

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        columns = get_columns(self.fields)
        key_col = [i for i, field in enumerate(self.fields) if field.name == self.key_field_name][0]

        for column in [c.field_name for c in criteria]:
            if column not in columns:
                raise ValueError

        keys_to_delete = [row[key_col] for row in self.get_relevent_rows(criteria)]
        for key in keys_to_delete:
            self.delete_record(key)

    def get_record(self, key: Any) -> Dict[str, Any]:
        if key not in self.key_index:
            raise ValueError

        data = self.key_index[key][0]
        rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{data[0]}.csv')
        return row_as_dict(rows[data[1]], self.fields)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        if key not in self.key_index:
            raise ValueError

        relevent_fields = [field for field in self.fields if field.name in values]
        if not is_valid_input(relevent_fields, values):
            raise ValueError

        self.update_file(key, reorder_columns(relevent_fields, values))

    def query_table(self, criteria: List[db_api.SelectionCriteria]) -> List[Dict[str, Any]]:
        columns = get_columns(self.fields)
        for column in [c.field_name for c in criteria]:
            if column not in columns:
                raise ValueError

        rows = self.get_relevent_rows(criteria)
        return [row_as_dict(row, self.fields) for row in rows]

    def create_index(self, field_to_index: str) -> None:
        if field_to_index not in [field.name for field in self.fields]:
            raise ValueError

        column = [i for i, field in enumerate(self.fields) if field.name == field_to_index][0]
        index = defaultdict(list)

        for i in range(self.amount):
            rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{i + 1}.csv')
            for j, row in enumerate(rows):
                index[row[column]].append((i+1, j))

        write_to_json(f'{db_api.DB_ROOT}/{self.name}_{field_to_index}_index.json', index)

    def update_amount(self, amount):
        global table_list
        self.amount += amount
        table_list[self.name]['amount'] = self.amount
        write_to_json(f'{db_api.DB_ROOT}/metadata.json', table_list)

    def update_key_index(self, key, value):
        self.key_index[key] = [value]
        write_to_json(f'{db_api.DB_ROOT}/{self.name}_key_index.json', self.key_index)

    def delete_key_index(self, key):
        del self.key_index[key]
        write_to_json(f'{db_api.DB_ROOT}/{self.name}_key_index.json', self.key_index)

    def delete_from_file(self, key):
        column = [i for i, field in enumerate(self.fields) if field.name == self.key_field_name][0]
        data = self.key_index[key][0]

        if self.count() == 1 and self.amount == 1:
            delete_file(f'{db_api.DB_ROOT}/{self.name}_1.csv')
            self.update_amount(-1)
            self.delete_key_index(key)
            return

        rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv')
        last_row = rows.pop(-1)
        last_row = [cast(item, field.type) for item, field in zip(last_row, self.fields)]

        if self.count() % self.num_of_records == 1:
            delete_file(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv')
            self.update_amount(-1)

        else:
            write_to_csv(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv', rows)

        if last_row[column] == key:
            self.delete_key_index(key)
            return

        rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{data[0]}.csv')
        rows[data[1]] = last_row
        write_to_csv(f'{db_api.DB_ROOT}/{self.name}_{data[0]}.csv', rows)

        self.update_key_index(last_row[column], data)
        self.delete_key_index(key)

    def update_file(self, key, values):
        data = self.key_index[key][0]

        rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{data[0]}.csv')
        new_row = rows[data[1]]
        for i, field in enumerate(self.fields):
            if field.name in values:
                new_row[i] = values[field.name]
        rows[data[1]] = new_row

        write_to_csv(f'{db_api.DB_ROOT}/{self.name}_{self.amount}.csv', rows)

    def get_relevent_rows(self, criteria):
        rows_to_return = []

        for i in range(self.amount):
            rows = read_from_csv(f'{db_api.DB_ROOT}/{self.name}_{i + 1}.csv')
            for row in rows:
                row = [cast(r, field.type) for r, field in zip(row, self.fields)]
                if is_relevent_row(row_as_dict(row, self.fields), criteria):
                    rows_to_return.append(row)

        return rows_to_return
