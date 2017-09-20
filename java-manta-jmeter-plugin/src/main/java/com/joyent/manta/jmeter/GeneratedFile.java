package com.joyent.manta.jmeter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

/**
 * This will create a random file of garbage but specific size. With the option
 * for multipart.
 * 
 * @author DouglasAnderson z
 */
public class GeneratedFile extends AbstractJavaSamplerClient {
    private int size;
    private String filename;

    @Override
    public void setupTest(JavaSamplerContext context) {
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("size", "");
        params.addArgument("filename", "5000");
        return params;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        // setting vars from input table.
        try {
            size = Integer.parseInt(context.getParameter("size"));
            filename = context.getParameter("filename");
        } catch (Exception e) {
            // Catching number format exception, if we have one we are going to fail the test.
            SampleResult result = new SampleResult();
            result.setSuccessful(false);
            result.setResponseData(String.format("Parameter incorrect %s", e.getMessage()).getBytes());
            return result;
        }
        SampleResult result = new SampleResult();
        System.out.println("File name " + filename);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            byte[] b = new byte[size];
            new Random().nextBytes(b);
            bw.write(new String(b));
        } catch (Exception e) {
            result.setResponseData(String.format("Cannot put obj: %s", e.getMessage()).getBytes());
            result.setSuccessful(false);
            result.sampleEnd();
        }
        result.setSuccessful(true);
        result.setSamplerData("Successfully wrote a file of size " + size + " to file : " + filename);
        return result;
    }
}
