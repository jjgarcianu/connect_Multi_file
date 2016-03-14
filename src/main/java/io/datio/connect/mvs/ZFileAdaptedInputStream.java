package io.datio.connect.mvs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


import com.ibm.jzos.ZFile;

public class ZFileAdaptedInputStream extends InputStream {

    public static String DEFAULT_RECORD_SEPARATOR = "\n";
    public static String DEFAULT_RECORD_ACCESS_TYPE = "rb,type=record,noseek";
    ZFile zFileIn = null;
    int readRecordSize = 0;
    byte[] recBuf =null;
    int transmitPending = 0;
    ByteBuffer recordEndBuf = null;

    ZFileAdaptedInputStream(String sFilename) throws Exception
    {
        init(sFilename, DEFAULT_RECORD_SEPARATOR, DEFAULT_RECORD_ACCESS_TYPE);
    }

    ZFileAdaptedInputStream(String sFilename, String sRecordSeparator, String sRecordAccessType) throws Exception
    {
        init(sFilename, sRecordSeparator, sRecordAccessType);
    }

    private void init(String sFilename, String sRecordSeparator, String sRecordAccessType) throws Exception
    {
        zFileIn = new ZFile(sFilename, sRecordAccessType);
        recordEndBuf =
                Charset.forName(Charset.defaultCharset().toString()).encode(sRecordSeparator);

    }
    private void endRecordBuffer(int readLength) throws IOException
    {
        readRecordSize = readLength +  recordEndBuf.limit();
        for(int i = 0; i < recordEndBuf.limit(); i ++) {
            recBuf[readLength + i ] = recordEndBuf.get(i);
        }
        transmitPending = readRecordSize;
    }

    @Override
    public void close() throws IOException {
        try { zFileIn.close(); }
        catch(Exception e) { throw new IOException(e);}
    }

    @Override
    public int read() throws IOException {
        if(transmitPending==0) {
            recBuf = new byte[zFileIn.getLrecl() + recordEndBuf.limit() ];
            int length = zFileIn.read(recBuf);
            if (length<0) {return -1;}
            endRecordBuffer(length);
        }
        int pos = transmitted();
        transmitPending--;
        return recBuf[pos];
    }

    private int transmitted() { return  readRecordSize - transmitPending; }
}