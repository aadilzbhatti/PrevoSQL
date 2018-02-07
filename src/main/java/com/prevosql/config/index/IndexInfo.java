package com.prevosql.config.index;

/**
 * Models key index information
 */
public class IndexInfo {
    private final String tableName;
    private final String searchKey;
    private final int numLeafPages;
    private final boolean isClustered;

    /**
     * Constructs index information
     *
     * @param tableName Name of table corresponding to index
     * @param searchKey Search key that is indexed
     * @param numLeafPages Number of leaf pages in the index
     * @param isClustered Whether or not the index is clustered
     */
    IndexInfo(String tableName, String searchKey, int numLeafPages, boolean isClustered) {
        this.tableName = tableName;
        this.searchKey = searchKey;
        this.numLeafPages = numLeafPages;
        this.isClustered = isClustered;
    }

    /**
     * @return Name of table corresponding to index
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Search key that is indexed
     */
    public String getSearchKey() {
        return searchKey;
    }

    /**
     * @return Whether or not the index is clustered
     */
    public boolean isClustered() {
        return isClustered;
    }

    /**
     * @return Number of leaf pages in the index
     */
    public int getNumLeafPages() {
        return numLeafPages;
    }
}
