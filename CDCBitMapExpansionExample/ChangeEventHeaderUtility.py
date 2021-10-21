"""
ChangeEventHeaderUtility.py

This class provides the utility method to decode the bitmap fields (eg: ChangedEvents)  and return the avro schema field values represented by the bitmap.
"""

from avro.schema import Schema
from bitstring import BitArray


def process_bitmap(avro_schema: Schema, bitmap_fields: list):
    if len(bitmap_fields) != 0:
        # replace top field level bitmap with list of fields
        if bitmap_fields[0].startswith("0x"):
            bitmap = bitmap_fields[0]
            bitmap_fields.insert(0, get_fieldnames_from_bitstring(bitmap, avro_schema))
            bitmap_fields.remove(bitmap)
        # replace parentPos-nested Nulled BitMap with list of fields too
        if "-" in bitmap_fields[-1]:
            bitmap_field_iterator = iter(bitmap_fields)
            while True:
                try:
                    bitmap_field = next(bitmap_field_iterator)
                    if bitmap_field is not None and "-" in bitmap_field:
                        bitmap_strings = bitmap_field.split("-")
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
                                bitmap_fields.remove(bitmap_field)
                                # when all nested fields under a compound got nulled out at once by customer, we recognize the top level field instead of trying to list every single nested field
                                if len(full_field_names) == nested_size:
                                    bitmap_fields.append(0, parent_field_name)
                                else:
                                    bitmap_fields.insert(0, full_field_names)
                except StopIteration:
                    break

    return bitmap_fields


def convert_hexbinary_to_bitset(bitmap):
    bit_array = BitArray(hex=bitmap[2:])
    binary_string = bit_array.bin
    return binary_string[::-1]


def append_parent_name(parent_field_name, full_field_names):
    for index in range(len(full_field_names)):
        full_field_names[index] = parent_field_name + "." + full_field_names[index]
    return full_field_names


def get_fieldnames_from_bitstring(bitmap, avro_schema: Schema):
    bitmap_field_name = []
    fields_list = list(avro_schema.fields_dict)
    binary_string = convert_hexbinary_to_bitset(bitmap)
    indexes = find('1', binary_string)
    for index in indexes:
        bitmap_field_name.append(fields_list[index])
    return bitmap_field_name


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
