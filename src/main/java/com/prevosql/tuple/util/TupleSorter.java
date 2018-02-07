package com.prevosql.tuple.util;

import com.prevosql.tuple.Tuple;
import com.prevosql.tuple.io.reader.TupleReader;
import com.prevosql.tuple.io.reader.TupleReaderFactory;
import com.prevosql.tuple.io.writer.TupleWriter;
import com.prevosql.tuple.io.writer.TupleWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Sorts a file of tuples and writes it to file for testing purposes
 */
public class TupleSorter {
    private static final Logger LOG = Logger.getLogger(TupleSorter.class);

    /**
     * Sorts binary file of tuples from filename and writes to file in outfile
     *
     * @param filename Input file to sort
     * @param outfile  Output file to write sorted tuples to
     */
    public static void sortFile(String filename, String outfile) {
        TupleReader tr = TupleReaderFactory.getReader(filename, true);
        List<Tuple> list = new ArrayList<>();
        Tuple t;
        while ((t = tr.readNextTuple()) != null) {
            list.add(t);
        }
        list.sort(Comparator.comparing(Tuple::toString));
        TupleWriter tw = TupleWriterFactory.getWriter(outfile, list.get(0).length());
        for (Tuple tup : list) {
            tw.writeTuple(tup);
        }
        LOG.info("Wrote list of sorted tuples to disk");
    }

    /**
     * Sorts file of tuples from filename and writes to file
     * named ${filename}-sorted
     *
     * @param filename Input file to sort
     */
    private static void sortFile(String filename) {
        sortFile(filename, filename + "-sorted");
    }

    public static boolean sortAndCompare(String file1, String file2) {
        sortFile(file1);
        sortFile(file2);
        File f1 = new File(file1 + "-sorted");
        File f2 = new File(file2 + "-sorted");

        try {
            boolean contentSame = FileUtils.contentEquals(f1, f2);
            f1.delete();
            f2.delete();
            return contentSame;
        } catch (IOException e) {
            LOG.fatal(e);
        }

        return false;
    }
}
