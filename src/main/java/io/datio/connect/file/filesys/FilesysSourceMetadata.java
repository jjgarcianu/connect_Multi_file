package io.datio.connect.file.filesys;

/**
 * Created by jjgarcia on 2/03/16.
 */
public interface FilesysSourceMetadata {
    String getFilesysType();

    FileRegexResultset getFilesList(Object o, Object o1, String s, Object o2);


    FileRegexResultset getColumns(Object o, Object o1, String table, String s);

    String getIdentifierQuoteString();
}
