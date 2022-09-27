package utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

/**
 * A utility class used to generate the field names from bitmap encoded values.
 *
 * @author pozil
 */
public class EventParser {

    private Schema schema;
    private DatumReader<GenericRecord> datumReader;

    public EventParser(Schema schema) {
        this.schema = schema;
        this.datumReader = new GenericDatumReader<GenericRecord>(schema);
    }

    /**
     * Retrieves the list of fields from a bitmap encoded value.
     *
     * @param schema
     * @param eventHeader
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static List<String> getFieldListFromBitmap(Schema schema, Record eventHeader, String fieldName)
            throws IOException {
        @SuppressWarnings("unchecked")
        Array<Utf8> utf8Values = (Array<Utf8>) eventHeader.get(fieldName);
        List<String> values = new ArrayList<>();
        for (Utf8 utf8Value : utf8Values) {
            values.add(utf8Value.toString());
        }
        expandBitmap(schema, values);
        return values;
    }

    /**
     * Translate a bitmap-compressed field list into its expanded representation as
     * a list of field names
     */
    public static void expandBitmap(Schema schema, List<String> val) {
        if (val != null && !val.isEmpty()) {
            // replace top field level bitmap with list of fields
            if (val.get(0).startsWith("0x")) {
                String bitMap = val.get(0);
                val.addAll(0, fieldNamesFromBitmap(schema, bitMap));
                val.remove(bitMap);
            }
            // replace parentPos-nestedNulledBitMap with list of fields too
            if ((val.get(val.size() - 1)).contains("-")) {
                for (ListIterator<String> itr = val.listIterator(); itr.hasNext();) {

                    String[] bitmapMapStrings = (itr.next()).split("-");
                    if (bitmapMapStrings.length < 2)
                        continue; // that's the first top level field bitmap;

                    // interpret the parent field name from mapping of parentFieldPos ->
                    // childFieldbitMap
                    Schema.Field parentField = schema.getFields().get(Integer.valueOf(bitmapMapStrings[0]));
                    Schema childSchema = getValueSchema(parentField.schema());

                    if (childSchema.getType().equals(Schema.Type.RECORD)) { // make sure we're really dealing with
                        // compound field
                        int nestedSize = childSchema.getFields().size();
                        String parentFieldName = parentField.name();

                        // interpret the child field names from mapping of parentFieldPos ->
                        // childFieldbitMap
                        List<String> fullFieldNames = new ArrayList<>();
                        fieldNamesFromBitmap(childSchema, bitmapMapStrings[1]).stream()
                                .map(col -> parentFieldName + "." + col).forEach(fullFieldNames::add);
                        if (fullFieldNames.size() > 0) {
                            itr.remove();
                            // when all nested fields under a compound got nulled out at once by customer,
                            // we recognize the top level field instead of trying to list every single
                            // nested field
                            if (fullFieldNames.size() == nestedSize) {
                                itr.add(parentFieldName);
                            } else {
                                fullFieldNames.stream().forEach(itr::add);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Convert bitmap representation into list of fields based on Avro schema
     *
     * @param schema schema
     * @param bitmap bitmap of nulled fields
     * @return list of fields corresponding to bitmap
     */
    private static List<String> fieldNamesFromBitmap(Schema schema, String bitmap) {
        BitSet bitSet = convertHexStringToBitSet(bitmap);
        List<String> fieldList = new ArrayList<>();
        bitSet.stream().mapToObj(pos -> schema.getFields().get(pos).name())
                .forEach(fieldName -> fieldList.add(fieldName));
        return fieldList;
    }

    /**
     * Converts a hexadecimal string into a BitSet
     *
     * @param hex
     * @return BitSet
     */
    private static BitSet convertHexStringToBitSet(String hex) {
        // Parse hex string as bytes
        String s = hex.substring(2); // Strip out 0x prefix
        int len = s.length();
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            // using left shift operator on every character
            bytes[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        // Reverse bytes
        len /= 2;
        byte[] reversedBytes = new byte[len];
        for (int i = 0; i < len; i++) {
            reversedBytes[i] = bytes[len - i - 1];
        }
        // Return value as BitSet
        return BitSet.valueOf(reversedBytes);
    }

    /**
     * Get the value type of an "optional" schema, which is a union of [null,
     * valueSchema]
     *
     * @param schema
     * @return value schema or the original schema if it does not look like optional
     */
    private static Schema getValueSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes();
            if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
                // Optional is a union of (null, <type>), return the underlying type
                return types.get(1);
            } else if (types.size() == 2 && types.get(0).getType() == Type.STRING) {
                // for required Switchable_PersonName
                return schema.getTypes().get(1);
            } else if (types.size() == 3 && types.get(0).getType() == Type.NULL
                    && types.get(1).getType() == Type.STRING) {
                // for optional Switchable_PersonName
                return schema.getTypes().get(2);
            }
        }
        return schema;
    }
}