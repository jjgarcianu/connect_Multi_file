package io.datio.connect.mvs;
import java.io.InputStream;
import com.ibm.jzos.ZFile;

public class ZFileInputStreamFactory {

    public static InputStream getInputStream(String sFilename, boolean bAdapted) throws Exception
    {
        return bAdapted ?
                (new ZFileAdaptedInputStream(sFilename)) :
                (new ZFile(sFilename,
                        ZFileAdaptedInputStream.DEFAULT_RECORD_ACCESS_TYPE)).getInputStream();
    }

    public static InputStream getInputStream(String sFilename) throws Exception
    {
        return getInputStream(sFilename, true);
    }
}

