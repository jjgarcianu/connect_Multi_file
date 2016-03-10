package io.datio.connect.fileinterface;

import java.io.IOException;
import java.io.InputStream;

public  class FileInterface extends InputStream {

    private FilesystemType filesysType  = null;
    private String filenameRegEx  = null;

    public FileInterface(String pFilenameRegEx, FilesystemType pFilesysType ) throws FileDoesNotExistException{

        filenameRegEx = pFilenameRegEx;
        filesysType = pFilesysType;
    }

    @Override
    public int read() throws IOException {
        return 0;
    }
}
