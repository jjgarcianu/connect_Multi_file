package io.datio.connect.mvs;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

public class ZFileCopyFromMVSTest {

    private static String FILE1_NAME = "KAFKA.PRUEBA.CP";
    private static String FILE2_NAME = "UAMCO05.KAFKA.OUT";


    @Test
    public  void testFile1() throws Exception
        {ZFileCopyFromMVSTest.test(FILE1_NAME);}

    @Test
    public  void testFile2() throws Exception
        {ZFileCopyFromMVSTest.test(FILE2_NAME);}

    public static void main(String[] args) throws Exception {
        test(args[0]);
    }

    public static void test(String sFilename)  throws Exception
    {
        String copyCommand="sh get.sh " + sFilename;
        System.out.println(copyCommand);
        Process p1 = Runtime.getRuntime().exec(copyCommand);
        p1.waitFor();
        System.out.println("Retorno: " + p1.exitValue());
        printOutput(p1);



        String zCopyCommand="java -cp . io.datio.connect.mvs.ZFileCopyFromMVS //" + sFilename + " test." + sFilename + ".txt";
        System.out.println(zCopyCommand);
        Process p2 = Runtime.getRuntime().exec(zCopyCommand);
        p2.waitFor();
        System.out.println("Retorno: " + p2.exitValue());
        printOutput(p2);



        String finalCommand="diff "+"test."+sFilename+".txt "+
                "orig."+sFilename+".txt";
        System.out.println(finalCommand);
        Process p3 = Runtime.getRuntime().exec(finalCommand);
        p3.waitFor();
        int finalReturn = p3.exitValue();
        System.out.println("Retorno: " + p3.exitValue());
        printOutput(p3);
        assertTrue(finalReturn == 0);


    }

    static void  printOutput(Process proc) throws IOException
    {
        BufferedReader stdInput = new BufferedReader(new
                InputStreamReader(proc.getInputStream()));

        BufferedReader stdError = new BufferedReader(new
                InputStreamReader(proc.getErrorStream()));
        String s = null;
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }
        while ((s = stdError.readLine()) != null) {
            System.out.println(s);
        }
    }
}