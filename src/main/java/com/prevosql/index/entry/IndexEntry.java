package com.prevosql.index.entry;

/**
 * Models index entries
 */
public class IndexEntry {
    private int before;
    private int after;

    /**
     * Default constructor
     */
    public IndexEntry() {

    }

    /**
     * Constructs index entry with all parameters
     *
     * @param before Key before this entry
     * @param after Key after this entry
     */
    public IndexEntry(int before, int after) {
        this.before = before;
        this.after = after;
    }

    /**
     * @return Key before this entry
     */
    public int getBefore() {
        return before;
    }

    /**
     * Sets key before this entry
     *
     * @param before Key to set to
     */
    public void setBefore(int before) {
        this.before = before;
    }

    /**
     * @return Key after this entry
     */
    public int getAfter() {
        return after;
    }

    /**
     * Sets key after this entry
     *
     * @param after Key to set to
     */
    public void setAfter(int after) {
        this.after = after;
    }
}
