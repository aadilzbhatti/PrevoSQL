package com.prevosql.operator.physical.unary.sort;

import com.prevosql.config.catalog.DBCatalog;
import com.prevosql.config.Configuration;
import com.prevosql.interpreter.query.plan.visitor.PhysicalPlanVisitor;
import com.prevosql.operator.physical.PhysicalOperator;
import com.prevosql.operator.physical.binary.join.JoinPhysicalOperator;
import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import com.prevosql.tuple.io.writer.TupleWriter;
import com.prevosql.tuple.io.writer.TupleWriterFactory;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Performs an external sort of the child operator
 */
public class ExternalSortPhysicalOperator extends SortPhysicalOperator {
    private final int numTuples;
    private Tuple[] buffer;
    private String tempDirPath;
    private int attributes = 0;
    private final boolean binary;
    private TupleReader reader;
    private Path finalFilePath;

    private static final int PAGE_SIZE = 4096;

    /**
     * Constructs an ExternalSortPhysicalOperator. Will read in tuples into
     * numPages-sized blocks in memory, sort them, and then write them to temp
     * files to be merged.
     *
     * @param child Child operator to sort
     * @param orderByElements Elements we are sorting on
     * @param numPages Number of internal buffer pages to use
     * @param binary Whether or not we are writing the temp files in binary format or ASCII format
     */
    public ExternalSortPhysicalOperator(PhysicalOperator child, List<OrderByElement> orderByElements, int numPages, boolean binary) {
        super(child, orderByElements);
        this.numTuples = (numPages * PAGE_SIZE) / (4 * child.getNumAttributes());
        this.buffer = new Tuple[numTuples];
        this.binary = binary;
        if (binary) {
            attributes = child.getNumAttributes();
        }

        String globalTempDir = Configuration.getInstance().getTempDirPath();
        try {
            Path tempDir = Files.createTempDirectory(Paths.get(globalTempDir), child.getTableName());
            tempDirPath = tempDir.toString();
        } catch (IOException e) {
            LOG.fatal(e);
            System.err.println("Failed to create temporary directory for sorting: " + e.getMessage());
        }

        if (child instanceof JoinPhysicalOperator) {
            ((JoinPhysicalOperator) child).createJoinedTable();
            catalog = ((JoinPhysicalOperator) child).getCatalog();
        } else {
            catalog = DBCatalog.getInstance();
        }

        sortFile();
    }

    /**
     * Constructs a binary ESPO
     *
     * @param child Child to sort
     * @param elements Elements to sort by
     * @param numPages Number of internal buffer pages to use
     */
    public ExternalSortPhysicalOperator(PhysicalOperator child, List<OrderByElement> elements, int numPages) {
        this(child, elements, numPages, true);
    }

    /**
     * @return A sorted tuple of the child operator
     */
    @Override
    public Tuple getNextTuple() {
        return reader.readNextTuple();
    }

    @Override
    public void reset() {
        reader.reset();
    }

    @Override
    public void accept(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void reset(int index) {
        reader.reset(index);
    }

    /**
     * Reads tuples into numPages-sized blocks in memory, sorts them,
     * and writes to temp files which are then merged into one sorted file.
     */
    private void sortFile() {
        LOG.info("Sorting relation " + child.getTableName());
        boolean lastPass = false;
        int fileNumber = 0;

        while (!lastPass) {
            for (int i = 0; i < numTuples; i++) {
                Tuple t = child.getNextTuple();
                if (t == null) {
                    lastPass = true;
                    break;
                }
                buffer[i] = t;
            }

            Arrays.sort(buffer, this::compareTuplesExact);

            String tempFilePath = Paths.get(tempDirPath, "tmp" + fileNumber).toAbsolutePath().toString();
            TupleWriter tw = TupleWriterFactory.getWriter(tempFilePath, attributes);
            for (Tuple t : buffer) {
                if (t == null) {
                    break;
                }
                tw.writeTuple(t);
            }
            tw.flush();
            fileNumber++;

            buffer = new Tuple[numTuples];
            LOG.info("Reading a new block of tuples into memory: " + fileNumber);
        }

        String[] arr = new String[fileNumber];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = "tmp" + i;
        }

        String finalPath = (mergeRuns(arr))[0];

        finalFilePath = Paths.get(tempDirPath, finalPath).toAbsolutePath();
        reader = TupleReaderFactory.getReader(finalFilePath.toString(), binary);

        lastPass = false;
        fileNumber = 0;

        while (!lastPass) {
            for (int i = 0; i < numTuples; i++) {
                Tuple t = reader.readNextTuple();
                if (t == null) {
                    lastPass = true;
                    break;
                }
                buffer[i] = t;
            }

            Arrays.sort(buffer, this::compareTuples);

            String tempFilePath = Paths.get(tempDirPath, "tmp" + fileNumber).toAbsolutePath().toString();
            TupleWriter tw = TupleWriterFactory.getWriter(tempFilePath, attributes);
            for (Tuple t : buffer) {
                if (t == null) {
                    break;
                }
                tw.writeTuple(t);
            }
            tw.flush();
            fileNumber++;

            buffer = new Tuple[numTuples];
            LOG.info("Reading a new block of tuples into memory: " + fileNumber);
        }

        arr = new String[fileNumber];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = "tmp" + i;
        }

        finalPath = (mergeRuns(arr))[0];

        finalFilePath = Paths.get(tempDirPath, finalPath).toAbsolutePath();
        reader = TupleReaderFactory.getReader(finalFilePath.toString(), binary);
    }

    /**
     * Compares two tuples for sorting
     *
     * @param o1 First tuple to compare
     * @param o2 Second tuple to compare
     * @return 0 if equal on order by elements, 1 if o1 comes after o2, -1 if o1 comes before o2
     */
    private int compareTuples(Tuple o1, Tuple o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return 1;
        } else if (o2 == null) {
            return -1;
        }

        if (o1.equals(o2)) {
            return 0;
        }

        if (orderByElements == null) {
            return compareTuplesExact(o1, o2);
        }

        OrderByElement e0 = orderByElements.get(0);
        int ind0 = getIndex(e0);
        int val = Integer.parseInt(o1.get(ind0)) - Integer.parseInt(o2.get(ind0));
        if (val == 0) {
            for (int i = 1; i < orderByElements.size(); i++) {
                int index = getIndex(orderByElements.get(i));
                int value = Integer.parseInt(o1.get(index)) - Integer.parseInt(o2.get(index));

                if (value != 0) {
                    return value;
                }
            }
        }
        return val;
    }

    private int compareTuplesExact(Tuple o1, Tuple o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return 1;
        } else if (o2 == null) {
            return -1;
        }

        if (o1.equals(o2)) {
            return 0;
        }

        for (int i = 0; i < o1.length(); i++) {
            int val = Integer.parseInt(o1.get(i)) - Integer.parseInt(o2.get(i));
            if (val != 0) {
                return val;
            }
        }

        return 0;
    }

    /**
     * Returns tuple index of column
     *
     * @param e Element to find index of
     * @return Tuple index of column
     */
    private int getIndex(OrderByElement e) {
        Column col = (Column) e.getExpression();
        String tableName = col.getTable().getName();
        String columnName = col.getColumnName();
        return catalog.getTable(tableName).getIndexForColumn(columnName);
    }

    /**
     * Merges files whose names are stored in runs
     * into one sorted file
     *
     * @param runs Array of filenames to merge
     * @return A single-element array containing the filename
     * with the sorted file
     */
    private String[] mergeRuns(String[] runs) {
        if (runs.length == 1) {
            return runs;
        }

        if (runs.length == 2) {
            mergeFiles(runs[0], runs[1]);
            return new String[]{ runs[1] };
        }

        int mid = runs.length / 2;
        int rightLength = runs.length - mid;
        String[] left = new String[mid];
        String[] right = new String[rightLength];
        System.arraycopy(runs, 0, left, 0, mid);
        System.arraycopy(runs, mid, right, 0, rightLength);

        String[] mergedLeft = mergeRuns(left);
        String[] mergedRight = mergeRuns(right);
        mergeFiles(mergedLeft[0], mergedRight[0]);
        return new String[] { mergedRight[0] };
    }

    /**
     * Merges two files into one sorted file
     *
     * @param file1 First file to merge
     * @param file2 Second file to merge
     */
    private void mergeFiles(String file1, String file2) {
        LOG.info("Merging temp files " + file1 + " and " + file2);
        try {
            Path f1 = Paths.get(tempDirPath, file1 + "-merging").toAbsolutePath();
            Path f2 = Paths.get(tempDirPath, file2 + "-merging").toAbsolutePath();
            Path output = Paths.get(tempDirPath, file2).toAbsolutePath();

            Files.move(Paths.get(tempDirPath, file1).toAbsolutePath(), f1);
            Files.move(output, f2);

            TupleReader t1 = TupleReaderFactory.getReader(f1.toString(), binary);
            TupleReader t2 = TupleReaderFactory.getReader(f2.toString(), binary);
            TupleWriter tw = TupleWriterFactory.getWriter(output.toString(), attributes);
            Tuple left = t1.readNextTuple();
            Tuple right = t2.readNextTuple();

            while (left != null && right != null) {
                int compared = compareTuples(left, right);
                if (compared > 0) {
                    tw.writeTuple(right);
                    right = t2.readNextTuple();

                } else if (compared < 0) {
                    tw.writeTuple(left);
                    left = t1.readNextTuple();

                } else {
                    tw.writeTuple(left);
                    tw.writeTuple(right);
                    left = t1.readNextTuple();
                    right = t2.readNextTuple();
                }
            }

            while (left != null) {
                tw.writeTuple(left);
                left = t1.readNextTuple();
            }

            while (right != null) {
                tw.writeTuple(right);
                right = t2.readNextTuple();
            }

            tw.flush();
            Files.delete(f1);
            Files.delete(f2);
            LOG.info("Successfully merged files " + file1 + " and " + file2);

        } catch (IOException e) {
            LOG.fatal(e);
        }
    }

    /**
     * @return Path to sorted output file
     */
    public Path getFinalFilePath() {
        return finalFilePath;
    }
}
