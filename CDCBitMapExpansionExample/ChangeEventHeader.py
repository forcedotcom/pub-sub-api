"""
ChangeEventHeader.py

This class provides the utility method to decode the bitmap fields (eg: ChangedEvents)  and return the avro schema field values represented by the bitmap.
"""

from avro.schema import Schema
from bitstring import BitArray


def process_bitmap(avro_schema: Schema, changed_fields: list):
    if len(changed_fields) != 0:
        # replace top field level bitmap with list of fields
        if changed_fields[0].startswith("0x"):
            bitmap = changed_fields[0]
            changed_fields.insert(0, get_fieldnames_from_bitstring(bitmap, avro_schema))
            changed_fields.remove(bitmap)
        # replace parentPos-nested NulledBitMap with list of fields too
        if "-" in changed_fields[-1]:
            changed_field_iterator = iter(changed_fields)
            while True:
                try:
                    changed_field = next(changed_field_iterator)
                    if changed_field is not None and "-" in changed_field:
                        bitmap_strings = changed_field.split("-")
                        # interpret the parent field name from mapping of parentFieldPos -> childFieldbitMap
                        parent_field = get_fields(avro_schema)[int(bitmap_strings[0])]
                        child_schema = get_value_schema(parent_field.type)
                        # make sure we're really dealing with compound field
                        if child_schema.type is not None and child_schema.type == 'record':
                            nested_size = len(get_fields(child_schema))
                            parent_field_name = parent_field.get_prop('name')
                            # interpret the child field names from mapping of parentFieldPos -> childFieldbitMap
                            full_field_names = get_fieldnames_from_bitstring(bitmap_strings[1],
                                                                             child_schema)
                            full_field_names = append_parent_name(parent_field_name, full_field_names)
                            if len(full_field_names) > 0:
                                changed_fields.remove(changed_field)
                                # when all nested fields under a compound got nulled out at once by customer, we recognize the top level field instead of trying to list every single nested field
                                if len(full_field_names) == nested_size:
                                    changed_fields.append(0, parent_field_name)
                                else:
                                    changed_fields.insert(0, full_field_names)
                except StopIteration:
                    break

    return changed_fields


def convert_hexbinary_to_bitset(bitmap):
    bit_array = BitArray(hex=bitmap[2:])
    binary_string = bit_array.bin
    return binary_string[::-1]


def append_parent_name(parent_field_name, full_field_names):
    for index in range(len(full_field_names)):
        full_field_names[index] = parent_field_name + "." + full_field_names[index]
    return full_field_names


def get_fieldnames_from_bitstring(bitmap, avro_schema: Schema):
    changed_field_name = []
    fields_list = list(avro_schema.fields_dict)
    binary_string = convert_hexbinary_to_bitset(bitmap)
    indexes = find('1', binary_string)
    for index in indexes:
        changed_field_name.append(fields_list[index])
    return changed_field_name


# Get the value type of an "optional" schema, which is a union of [null, valueSchema]
def get_value_schema(parent_field):
    if parent_field.type == 'union':
        schemas = parent_field.schemas
        if len(schemas) == 2 and schemas[0].type == 'null':
            return schemas[1]
        if len(schemas) == 2 and schemas[0].type == 'string':
            return schemas[1]
        if len(schemas) == 3 and schemas[0].type == 'null' and schemas[1].type == 'string':
            return schemas[2]
    return parent_field


# Find the positions of 1 in the bit string
def find(to_find, binary_string):
    return [i for i, x in enumerate(binary_string) if x == to_find]


def get_fields(schema: Schema):
    return schema.get_prop('fields')
