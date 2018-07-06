package de.misc.rparso;

import java.io.*;
import java.util.List;
import java.util.Arrays;
import com.epam.parso.SasFileReader;
import com.epam.parso.Column;
import com.epam.parso.impl.SasFileParser;
import com.epam.parso.impl.SasFileReaderImpl;

import java.lang.reflect.Field;



public class BulkRead {
    //public enum ColumnType { CT_NUMERIC, CT_STRING };
    final static int CT_NUMERIC = 1;
    final static int CT_STRING = 2;

    private long num_rows = 0;
    private long rows_read = 0;
    private List<Column> columns = null;
    private SasFileReader sasFileReader = null;
    private int[] col_types = null;

    private int max_rows = -1;
    private int skip_rows = -1;
    private List<String> select_colnames = null;

    //private int chunk_size = 32768;

    //private String[] ret_strings = new String[chunk_size];
    //private int[] ret_ints = new int[chunk_size];

    public static void init(String sofn) {
        System.load(sofn);
    }

    public BulkRead(String filename, String encoding) throws IOException {
        FileInputStream fis = new FileInputStream(filename);
        sasFileReader = new SasFileReaderImpl(fis);
        if(encoding != null)
            set_encoding(encoding);
        num_rows = sasFileReader.getSasFileProperties().getRowCount();
        columns = sasFileReader.getColumns();

        // only for side-effects.. :/
        // TODO: fix
        getColtypes();
    }

    public BulkRead(String filename) throws IOException {
        this(filename, null);
    }

    // temporary(?) workaround
    private void set_encoding(String enc) {
        try {
            Field f = sasFileReader.getClass().getDeclaredField("sasFileParser");
            f.setAccessible(true);
            SasFileParser sfp = (SasFileParser) f.get(sasFileReader);

            f = sfp.getClass().getDeclaredField("encoding");
            f.setAccessible(true);

            f.set(sfp, (Object) enc);
        } catch(Exception e) {
            //TODO: do something maybe
        }
    }
    
    public SasFileReader getReader() {
        return sasFileReader;
    }

    public native void cb_set_int(int col, int row, int i);
    public native void cb_set_numeric(int col, int row, double i);
    public native void cb_set_string(int col, int row, String str);
    public native void cb_set_bytes(int col, int row, byte[] s);


    public void setSkipRows(int n) {
        this.skip_rows = n;
    }

    public void setMaxRows(int n) {
        this.max_rows = n;
    }

    public void setSelectColnames(String[] names) {
        this.select_colnames = Arrays.asList(names); //new ArrayList<String>(names);
    }

    public long getNumRows() {
        return num_rows;
    }

    public int getActualRows() {
        int rows_to_read = (int)num_rows;
        if(skip_rows > 0) {
            rows_to_read = (int)num_rows - skip_rows;
        }
        if(max_rows > 0) {
            rows_to_read = Math.min(rows_to_read, max_rows);
        }
        return rows_to_read;
    }


    private int[] getColIndexes() {
        int num_cols = columns.size();
        if(select_colnames != null)
            num_cols = select_colnames.size();
        int[] idx = new int[num_cols];

        if(select_colnames != null) {
            for(int i=0; i<num_cols; i++) {
                idx[i] = -1;
                // welches nicht gefunden?
                for(int j=0; j<columns.size(); j++) {
                    //System.out.println("`" + columns.get(j).getName() + "'");
                    if(columns.get(j).getName().equals(select_colnames.get(i))) {
                        //System.out.println("boo");
                        idx[i] = j;
                        break;
                    }
                }
            }
        } else {
            for(int i=0; i<num_cols; i++) {
                idx[i] = i;
            }
        }
        
        return idx;
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

    public String[] getColtypes() {
        if(columns == null)
            return null;
       
        int[] idx = getColIndexes();
        final int num_cols = idx.length;

        if(col_types == null || col_types.length != num_cols)
            col_types = new int[num_cols];

        String n[] = new String[num_cols];
        for(int i=0; i<num_cols; i++) {
            Class<?> c = columns.get(idx[i]).getType();
            n[i] = c.getCanonicalName();

            if(n[i] == "java.lang.Number")
                col_types[i] = CT_NUMERIC;
            else
                col_types[i] = CT_STRING;
        }

        return n;
    }

    /*
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
                if(col_types[i] == CT_NUMERIC)
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
                if(col_types[j] == CT_NUMERIC)
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
    */

    public int read_all() throws IOException {
        int i;
        int rows_to_read = getActualRows();

        if(skip_rows > 0) {
            for(i=0; i<skip_rows; i++)
                sasFileReader.readNext();
        }

        for(i=0; i<rows_to_read; i++) {
            /*
            if(i % 100000 == 0)
                System.out.println(i);
            */
            Object[] o = sasFileReader.readNext(select_colnames);
            for(int j=0; j<o.length; j++) {
                if(col_types[j] == CT_NUMERIC) {
                    //int num = ((Number) o[j]).intValue();
                    //cb_set_int(j, i, num);
                    double num = ((Number) o[j]).doubleValue();
                    cb_set_numeric(j, i, num);
                } 
                else {
                    String s = (String) o[j];
                    cb_set_bytes(j, i, (s != null) ? s.getBytes("UTF-8") : null);
                }
                /*
                else {
                    String s = (String) o[j];
                    cb_set_string(0, s);
                } */
            }
        }
        return i;
    }


    /*
    public String[] getStrings(int column) {
        //ret[i]
        //String[] r = new String[ret[0].length];
        System.arraycopy((String[])ret[column], 0, ret_strings, 0, ret_strings.length);
        return ret_strings;
    }
    */
   
    /*
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
    */

    /*
    public int[] getInts(int column) {
        //int[] r = new int[ret[0].length];
        Number[] x = (Number[]) ret[column];
        for(int i=0; i<ret_ints.length; i++) {
            ret_ints[i] = x[i].intValue();
        }
        return ret_ints;
    }
    */
};

