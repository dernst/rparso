package de.misc.rparso;

import java.io.*;
import java.util.List;
import com.epam.parso.SasFileReader;
import com.epam.parso.Column;
import com.epam.parso.impl.SasFileReaderImpl;


public class BulkRead {
    public enum ColumnType { CT_NUMERIC, CT_STRING };

    private long num_rows = 0;
    private long rows_read = 0;
    private List<Column> columns = null;
    private SasFileReader sasFileReader = null;
    private Object[] ret = null;
    private ColumnType[] col_types = null;

    private int chunk_size = 32768;

    private String[] ret_strings = new String[chunk_size];
    private int[] ret_ints = new int[chunk_size];


    public BulkRead(String filename) throws IOException {
        FileInputStream fis = new FileInputStream(filename);
        sasFileReader = new SasFileReaderImpl(fis);
        num_rows = sasFileReader.getSasFileProperties().getRowCount();
        columns = sasFileReader.getColumns();
    }
    
    public SasFileReader getReader() {
        return sasFileReader;
    }

    public String[] getColnames() {
        if(columns == null)
            return null;

        String n[] = new String[columns.size()];
        for(int i=0; i<columns.size(); i++) {
            n[i] = columns.get(i).getName();
        }

        return n;
    }

    public long getNumRows() {
        return num_rows;
    }

    public String[] getColtypes() {
        if(columns == null)
            return null;

        if(col_types == null)
            col_types = new ColumnType[columns.size()];

        String n[] = new String[columns.size()];
        for(int i=0; i<columns.size(); i++) {
            Class<?> c = columns.get(i).getType();
            n[i] = c.getCanonicalName();

            if(n[i] == "java.lang.Number")
                col_types[i] = ColumnType.CT_NUMERIC;
            else
                col_types[i] = ColumnType.CT_STRING;
        }

        return n;
    }


    public int read_chunk() {
        int read_now = 0;

        try {
        long remaining = num_rows - rows_read;
        int cs = (int) Math.min(remaining, chunk_size);

        if(remaining == 0)
            return 0;

        if(ret == null) {
            String[] ct = getColnames();
            getColtypes();
            ret = new Object[columns.size()];
            for(int i=0; i<ret.length; i++) {
                if(col_types[i] == ColumnType.CT_NUMERIC)
                    ret[i] = new Number[chunk_size];
                else
                    ret[i] = new String[chunk_size];
            }
        }

        for(int i=0; i<cs; i++) {
            Object[] o = sasFileReader.readNext();
            rows_read++;
            read_now++;
            for(int j=0; j<columns.size(); j++) {
                if(col_types[j] == ColumnType.CT_NUMERIC)
                    ((Number[])ret[j])[i] = (Number) o[j];
                else
                    ((String[])ret[j])[i] = (String) o[j];
            }
        }

        //return ret;
        } catch(Exception e) {
            e.printStackTrace();
            return -1;
        }

        return read_now;
    }

    public String[] getStrings(int column) {
        //ret[i]
        //String[] r = new String[ret[0].length];
        /*
        for(int i=0; i<ret_strings.length; i++) {
            ret_strings[i] = (String) ret[column][i];
        }
        */
        System.arraycopy((String[])ret[column], 0, ret_strings, 0, ret_strings.length);
        return ret_strings;
    }
    
    public String getString(int column) {
        StringBuffer sb = new StringBuffer();
        String[] sa = (String[]) ret[column];
        //char c = '\0';
        char c = '\n';
        for(int i=0; i<ret_strings.length; i++) {
            sb.append(sa[i]);
            sb.append(c);
        }
        return sb.toString();
    }

    public int[] getInts(int column) {
        //int[] r = new int[ret[0].length];
        Number[] x = (Number[]) ret[column];
        for(int i=0; i<ret_ints.length; i++) {
            ret_ints[i] = x[i].intValue();
        }
        return ret_ints;
    }

};
