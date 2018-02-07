package com.prevosql.index.entry;

/**
 * Models record IDs in the index
 */
public class RecordId {
    private int pageId;
    private int tupleId;

    /**
     * Constructs a record ID
     *
     * @param pageId Page number of record ID
     * @param tupleId Tuple offset of record ID
     */
    public RecordId(int pageId, int tupleId) {
        this.pageId = pageId;
        this.tupleId = tupleId;
    }

    /**
     * @return The page offset for this record ID
     */
    public int getPageId() {
        return pageId;
    }

    /**
     * @return The tuple ID for this record ID
     */
    public int getTupleId() {
        return tupleId;
    }

    @Override
    public String toString() {
        return "(" + pageId + ", " + tupleId + ")";
    }

    public boolean equals(RecordId other) {
        return pageId == other.pageId && tupleId == other.tupleId;
    }
}
