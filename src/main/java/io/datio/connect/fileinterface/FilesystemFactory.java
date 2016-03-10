package io.datio.connect.fileinterface;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public  class FilesystemFactory {


    public static InputStream getFile(String filename, FilesystemType type)
            throws IOException {

        switch (type) {
            case NORMAL:
                return new FileInputStream(filename);
            case ZOS:
                return new ZosFileInputStream(filename);
            default:
                throw new FilesystemNotSupportedException();
        }
    }
}
