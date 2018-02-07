package com.prevosql.config.index;

/**
 * Similar to IndexInfo but also models reduction factors,
 * low keys and high keys
 */
public class IndexResult {
    private int lowkey;
    private int highkey;
    private String searchKey;
    private double reductionFactor;
    private boolean isClustered;

    /**
     * Constructs an IndexResult
     *
     * @param lowkey Lower bound of the search range
     * @param highkey Upper bound of the search range
     * @param searchKey Key that is indexed
     * @param reductionFactor Fraction of tuples that fall in the range
     * @param isClustered Whether or not the indexed is clustered
     */
    public IndexResult(int lowkey, int highkey, String searchKey, double reductionFactor, boolean isClustered) {
        this.lowkey = lowkey;
        this.highkey = highkey;
        this.searchKey = searchKey;
        this.reductionFactor = reductionFactor;
        this.isClustered = isClustered;
    }

    /**
     * @return Lower bound of the search range
     */
    public int getLowkey() {
        return lowkey;
    }

    /**
     * @return Upper bound of the search range
     */
    public int getHighkey() {
        return highkey;
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

    public void setLowkey(int lowkey) {
        this.lowkey = lowkey;
    }

    public void setHighkey(int highkey) {
        this.highkey = highkey;
    }

    public void setSearchKey(String searchKey) {
        this.searchKey = searchKey;
    }

    public void setClustered(boolean clustered) {
        isClustered = clustered;
    }

    public double getReductionFactor() {
        return reductionFactor;
    }
}
