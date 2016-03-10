package io.datio.connect.fileinterface;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jjgarcia on 10/03/16.
 */
public class FileInterface extends InputStream {
    public FileInterface(String filename) throws FileDoesNotExistException{
    }

    @Override
    public int read() throws IOException {
        return 0;
    }
}
