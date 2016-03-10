package io.datio.connect.multifile.metadatafilestore;

/**
 * Created by jjgarcia on 2/03/16.
 */
public class FilesysAdaptor {

    public FilesysSourceMetadata getMetaData() {
        return null;
    }


    public FileRegEx createFileRegEx() {
        return null;
    }

    public FileRegEx prepareRegEx(String regex) {
        return null;
    }

    public static FilesysAdaptor getFilesys(String idFilesys) throws FilesysException{
        return null;
    }

    public void close() throws FilesysException {
    }
}
