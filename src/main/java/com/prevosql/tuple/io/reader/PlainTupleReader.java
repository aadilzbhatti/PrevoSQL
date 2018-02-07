package com.prevosql.tuple.io.reader;

import com.prevosql.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

class PlainTupleReader implements TupleReader {
    private BufferedReader fileReader;
    private final String filename;

    private static final Logger LOG = Logger.getLogger(PlainTupleReader.class);

    PlainTupleReader(String filename) {
        this.filename = filename;

        try {
            this.fileReader = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            LOG.fatal(e);
        }
    }

    @Override
    public Tuple readNextTuple() {
        try {
            String line = fileReader.readLine();
            if (line != null) {
                String[] args = line.split(",");
                return new Tuple(args);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOG.fatal(e);
            return null;
        }
    }

    @Override
    public void reset() {
        try {
            fileReader = new BufferedReader(new FileReader(filename));
        } catch (Exception e) {
            LOG.fatal("Failed to reset", e);
        }
    }

    @Override
    public int getNumAttributes() {
        return 0;
    }

    @Override
    public void reset(int index) {
        reset();
        for (int i = 0; i < index; i++) {
            readNextTuple();
        }
    }

    @Override
    public int getCurrPage() {
        throw new UnsupportedOperationException("Cannot get current page from plain tuple reader");
    }

    @Override
    public int getNumTuples() {
        throw new UnsupportedOperationException("Cannot get number of tuples from plain tuple reader");
    }
}
