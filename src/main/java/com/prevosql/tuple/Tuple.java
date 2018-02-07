package com.prevosql.tuple;

/**
 * Internal class for representing tuples
 */
public class Tuple {
    private final String[] array;

    /**
     * Builds tuple from list of string arguments
     *
     * @param args Input strings for tuple values
     */
    public Tuple(String... args) {
        array = args;
    }

    /**
     * Builds tuple from comma-separated value string
     *
     * @param args Comma-delimited string
     */
    public Tuple(String args) {
        array = args.split(",");
    }

    /**
     * Builds a tuple from two input tuples
     *
     * @param t1 Input tuple
     * @param t2 Input tuple
     */
    public Tuple(Tuple t1, Tuple t2) {
        String[] newArray = new String[t1.array.length + t2.array.length];
        System.arraycopy(t1.array, 0, newArray, 0, t1.array.length);
        System.arraycopy(t2.array, 0, newArray, t1.array.length, t2.array.length);
        array = newArray;
    }

    /**
     * Gets a tuple value from an index
     *
     * @param i Input index
     * @return Value of tuple at index
     */
    public String get(int i) {
        return array[i];
    }

    /**
     * String representation of tuple
     *
     * @return String representing tuple
     */
    @Override
    public String toString() {
        return String.join(",", array);
    }

    /**
     * Checks if two tuples are equivalent, used in
     * unit testing
     *
     * @param other Other tuple to check
     * @return Whether or not these tuples are equivalent
     */
    public Boolean equals(Tuple other) {
        if (other.array.length != array.length) {
            return false;
        }
        for (int i = 0; i < array.length; i++) {
            if (!get(i).equals(other.get(i))) {
                return false;
            }
        }
        return true;
    }

    public int length() {
        return array.length;
    }
}
