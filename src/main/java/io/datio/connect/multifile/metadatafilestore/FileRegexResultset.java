package io.datio.connect.multifile.metadatafilestore;

import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by jjgarcia on 2/03/16.
 */
public class FileRegexResultset {
    public static int columnNullable;
    public static int columnNullableUnknown;

    public boolean next() {
        return false;
    }

    public void close() {
    }

    public FilesysEntityMetadata getMetaData() {
        return null;
    }

    public String getString(int getColumnsIsAutoincrement) {
     return null;
    }

    public Boolean getBoolean(int col) {
        return null;
    }

    public Byte getByte(int col) {
        return null;
    }

    public Short getShort(int col) {
        return null;
    }

    public Integer getInt(int col) {
        return null;
    }

    public Long getLong(int col) {
        return null;
    }

    public Float getFloat(int col) {
        return null;
    }

    public Double getDouble(int col) {
        return null;
    }

    public BigDecimal getBigDecimal(int col) {
        return null;
    }

    public String getNString(int col) {
        return null;
    }

    public Byte[] getBytes(int col) {
        return null;
    }

    public Date getDate(int col, Calendar utcCalendar) {
        return null;
    }

    //public  getTime(int col, Calendar utcCalendar) {
     //   return null;
    //}

//    public Object getTimestamp(int col, Calendar utcCalendar) {
  //      return null;
   // }

    public URL getURL(int col) {
        return null;
    }
/*
    public Blob getBlob(int col) {
        return null;
    }

    public Clob getClob(int col) {
        return null;
    }

    public Clob getNClob(int col) {
        return null;
    }

    public SQLXML getSQLXML(int col) {
        return null;
    }
*/
    public boolean wasNull() {
        return false;
    }
}
