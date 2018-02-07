package com.prevosql.tuple.io.writer;

import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

class PlainTupleWriter implements TupleWriter {
    private FileWriter fileWriter;
    private PrintWriter pw;
    private final String filename;

    private static final Logger LOG = Logger.getLogger(PlainTupleWriter.class);

    PlainTupleWriter(String filename) {
        File f = new File(filename);
        try {
            while (!f.createNewFile()) {
                if (f.delete() && f.createNewFile()) {
                    break;
                }
            }
            fileWriter = new FileWriter(f);
            pw = new PrintWriter(fileWriter);

        } catch (Exception e) {
            LOG.fatal(e);
        }
        this.filename = filename;
    }

    @Override
    public boolean writeTuple(Tuple t) {
        if (t == null) {
            return false;
        }
        pw.println(t);
        return true;
    }

    @Override
    public void reset() {
        try {
            fileWriter = new FileWriter(filename);
            pw = new PrintWriter(fileWriter);

        } catch (IOException e) {
            LOG.fatal(e);
        }
    }

    @Override
    public void flush() {
        try {
            fileWriter.close();
            pw.close();

        } catch (IOException e) {
            LOG.fatal(e);
        }
    }
}
