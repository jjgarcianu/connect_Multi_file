package io.datio.connect.mvs;

import java.io.IOException;
import java.io.InputStream;


import com.ibm.jzos.ZFile;

/**
 * Created by jjgarcia on 11/03/16.
 */
public class ZFileInputStreamFactory extends InputStream {

    ZFile zFileIn = null;
    int iRecordSize = 0;
    int posByteTransmited = -1;
    byte[] recBuf =null;
    int transmitPending = 0;

    ZFileInputStreamFactory(String sFilename) throws Exception
    {
        zFileIn = new ZFile(sFilename, "rb,type=record,noseek");
        iRecordSize = zFileIn.getLrecl() +1 ;
        recBuf = new byte[iRecordSize];
    }
    @Override
    public void close() throws IOException {
        try {
            zFileIn.close();
        } catch(Exception e)
        {
            throw new IOException(e);
        }

    }

    @Override
    public int read() throws IOException {
            if(!(transmitPending>0)) {

                if (zFileIn.read(recBuf) < 0) return -1;
                transmitPending = iRecordSize;
                recBuf[iRecordSize - 1] = (byte) 0xA;
            }
            int pos = transmited();
            transmitPending--;
            return recBuf[pos];
    }

    private int transmited()
    {
       return  iRecordSize - transmitPending;
    }
}

