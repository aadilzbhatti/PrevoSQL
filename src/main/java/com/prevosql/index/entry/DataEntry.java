package com.prevosql.index.entry;

import java.util.List;

/**
 * Models data entries in the index
 */
public class DataEntry {
    private int searchKey;
    private List<RecordId> recordIds;

    /**
     * Default constructor
     */
    public DataEntry() {

    }

    /**
     * Constructs data entry with all fields
     *
     * @param searchKey Value of indexed search key
     * @param recordIds RecordIds associated with this search key
     */
    public DataEntry(int searchKey, List<RecordId> recordIds) {
        this.searchKey = searchKey;
        this.recordIds = recordIds;
    }

    /**
     * @return Value of indexed search key
     */
    public int getSearchKey() {
        return searchKey;
    }

    /**
     * Sets search key
     *
     * @param searchKey value to set search key to
     */
    public void setSearchKey(int searchKey) {
        this.searchKey = searchKey;
    }

    /**
     * @return List of record IDs associated with this search key
     */
    public List<RecordId> getRecordIds() {
        return recordIds;
    }

    /**
     * Sets list of record IDs
     *
     * @param recordIds Record IDs to set list to
     */
    public void setRecordIds(List<RecordId> recordIds) {
        this.recordIds = recordIds;
    }

    @Override
    public String toString() {
        return "<" + searchKey + ":" + recordIds + ">";
    }

    /**
     * Checks if this data entry is equivalent to another,
     * implemented so we can hash them
     *
     * @param other Other data entry to check against
     * @return Whether or not this data entry is equal to the other
     */
    public boolean equals(DataEntry other) {
        if (searchKey != other.searchKey) {
            return false;
        }

        if (recordIds.size() != other.recordIds.size()) {
            return false;
        }

        for (int i = 0; i < recordIds.size(); i++) {
            if (!(recordIds.get(i).equals(other.recordIds.get(i)))) {
                return false;
            }
        }

        return true;
    }
}
