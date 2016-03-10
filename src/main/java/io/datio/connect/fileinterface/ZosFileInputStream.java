package io.datio.connect.fileinterface;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jjgarcia on 10/03/16.
 */
public class ZosFileInputStream  extends InputStream {

    ZosFileInputStream(String filename)
    {

    }
    @Override
    public int read() throws IOException {
        return 0;
    }
}
