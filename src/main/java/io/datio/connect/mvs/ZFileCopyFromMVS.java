package io.datio.connect.mvs;

import java.io.*;

public class ZFileCopyFromMVS {


    public static void main(String[] args) throws Exception {

        InputStream inputStream = ZFileInputStreamFactory.getInputStream(args[0]);
        PrintWriter printWriter = new PrintWriter (args[1]);

        Reader inputStreamReader = new InputStreamReader(inputStream);
        int data = inputStreamReader.read();
        while (data >= 0) {
            char theChar = (char) data;
            printWriter.print(theChar);
            data = inputStreamReader.read();
        }
        inputStreamReader.close();
        printWriter.close ();

    }
}
