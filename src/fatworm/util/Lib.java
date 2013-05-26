package fatworm.util;

public class Lib {
    /**
     * Convert a short into its little-endian byte string representation.
     *
     * @param	array	the array in which to store the byte string.
     * @param	offset	the offset in the array where the string will start.
     * @param	value	the value to convert.
     */
    public static void bytesFromShort(byte[] array, int offset, short value) {
        array[offset + 0] = (byte) ((value >> 0) & 0xff);
        array[offset + 1] = (byte) ((value >> 8) & 0xff);
    }

    /**
     * Convert an int into its little-endian byte string representation.
     *
     * @param	array	the array in which to store the byte string.
     * @param	offset	the offset in the array where the string will start.
     * @param	value	the value to convert.
     */
    public static void bytesFromInt(byte[] array, int offset, int value) {
        array[offset + 0] = (byte) ((value >> 0) & 0xff);
        array[offset + 1] = (byte) ((value >> 8) & 0xff);
        array[offset + 2] = (byte) ((value >> 16) & 0xff);
        array[offset + 3] = (byte) ((value >> 24) & 0xff);
    }

    /**
     * Convert an int into its little-endian byte string representation, and
     * return an array containing it.
     *
     * @param	value	the value to convert.
     * @return	an array containing the byte string.
     */
    public static byte[] bytesFromInt(int value) {
        byte[] array = new byte[4];
        bytesFromInt(array, 0, value);
        return array;
    }

    /**
     * Convert an int into a little-endian byte string representation of the
     * specified length.
     *
     * @param	array	the array in which to store the byte string.
     * @param	offset	the offset in the array where the string will start.
     * @param	length	the number of bytes to store (must be 1, 2, or 4).
     * @param	value	the value to convert.
     */
    public static void bytesFromInt(byte[] array, int offset,
            int length, int value) {

        switch (length) {
            case 1:
                array[offset] = (byte) value;
                break;
            case 2:
                bytesFromShort(array, offset, (short) value);
                break;
            case 4:
                bytesFromInt(array, offset, value);
                break;
        }
    }

    /**
     * Convert to a short from its little-endian byte string representation.
     *
     * @param	array	the array containing the byte string.
     * @param	offset	the offset of the byte string in the array.
     * @return	the corresponding short value.
     */
    public static short bytesToShort(byte[] array, int offset) {
        return (short) ((((short) array[offset + 0] & 0xff) << 0) |
                (((short) array[offset + 1] & 0xff) << 8));
    }

    /**
     * Convert to an unsigned short from its little-endian byte string
     * representation.
     *
     * @param	array	the array containing the byte string.
     * @param	offset	the offset of the byte string in the array.
     * @return	the corresponding short value.
     */
    public static int bytesToUnsignedShort(byte[] array, int offset) {
        return (((int) bytesToShort(array, offset)) & 0xffFF);
    }

    /**
     * Convert to an int from its little-endian byte string representation.
     *
     * @param	array	the array containing the byte string.
     * @param	offset	the offset of the byte string in the array.
     * @return	the corresponding int value.
     */
    public static int bytesToInt(byte[] array, int offset) {
        return (int) ((((int) array[offset + 0] & 0xff) << 0)  |
                (((int) array[offset + 1] & 0xff) << 8)  |
                (((int) array[offset + 2] & 0xff) << 16) |
                (((int) array[offset + 3] & 0xff) << 24));
    }

    /**
     * Convert to an int from a little-endian byte string representation of the
     * specified length.
     *
     * @param	array	the array containing the byte string.
     * @param	offset	the offset of the byte string in the array.
     * @param	length	the length of the byte string.
     * @return	the corresponding value.
     */
    public static int bytesToInt(byte[] array, int offset, int length) {

        switch (length) {
            case 1:
                return array[offset];
            case 2:
                return bytesToShort(array, offset);
            case 4:
                return bytesToInt(array, offset);
            default:
                return -1;
        }
    }

    /**
     * Convert to a string from a possibly null-terminated array of bytes.
     *
     * @param	array	the array containing the byte string.
     * @param	offset	the offset of the byte string in the array.
     * @param	length	the maximum length of the byte string.
     * @return	a string containing the specified bytes, up to and not
     *		including the null-terminator (if present).
     */
    public static String bytesToString(byte[] array, int offset, int length) {
        int i;
        for (i = 0; i < length; ++i) {
            if (array[offset + i] == 0) {
                break;
            }
        }
        return new String(array, offset, i);
    }

}
