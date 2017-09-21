package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.client.multipart.EncryptedServerSideMultipartManager;
import com.joyent.manta.client.multipart.MantaMultipartUploadTuple;
import com.joyent.manta.client.multipart.ServerSideMultipartManager;
import com.joyent.manta.client.multipart.ServerSideMultipartUpload;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.stream.Stream;

/**
 * This will create a random file of garbage but specific size. With the option
 * for multipart.
 * 
 * @author DouglasAnderson z
 */
public class UploadGeneratedFile extends MantaTester {

    private static final Logger LOG = LoggerFactory.getLogger(UploadGeneratedFile.class);

    private String dir;
    private int splitSize;
    private int noOfSplits;
    private boolean multipart;
    private boolean verify;
    private boolean encrypted;
    private int timeout;
    private int retries;
    private int maxConnections;
    private String localfile;
    private String encAlgo = "";
    private String objectName;

    @Override
    public void setupTest(final JavaSamplerContext context) {
        LOG.debug("Setup being called");
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("multipart", "true");
        params.addArgument("splitSize", "5000");
        params.addArgument("noOfSplits", "2");
        params.addArgument("directory", "~~/stor/");
        params.addArgument("fileName", "file1.txt");
        params.addArgument("verifyUpload", "true");
        params.addArgument("encrypted", "true");
        params.addArgument("socketTimeout", "10000");
        params.addArgument("retries", "0");
        params.addArgument("maxConnections", "1");
        params.addArgument("encryptionAlgorithm", "AES256/CTR/NoPadding");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        // setting vars from input table.
        try {
            dir = context.getParameter("directory");
            objectName = dir + context.getParameter("fileName");
            multipart = Boolean.parseBoolean(context.getParameter("multipart"));
            if (multipart) {
                splitSize = Integer.parseInt(context.getParameter("splitSize").trim());
            }
            verify = Boolean.parseBoolean(context.getParameter("verifyUpload"));
            encrypted = Boolean.parseBoolean(context.getParameter("encrypted"));
            timeout = Integer.parseInt(context.getParameter("socketTimeout").trim());
            retries = Integer.parseInt(context.getParameter("retries").trim());
            maxConnections = Integer.parseInt(context.getParameter("maxConnections").trim());
            localfile = context.getParameter("localfile");
            encAlgo = context.getParameter("encryptionAlgorithm");
            noOfSplits = Integer.parseInt(context.getParameter("noOfSplits"));
        } catch (Exception e) {
            // Catching number format exception, if we have one we are going to
            // fail the test.
            SampleResult result = new SampleResult();
            result.setSuccessful(false);
            result.setResponseData(String.format("Parameter incorrect %s", e.getMessage()).getBytes());
            return result;
        }
        // Setting up return value.
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");

        // Setting up the config && client
        ChainedConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        if (encrypted) {
            config.setClientEncryptionEnabled(true).setEncryptionAlgorithm(encAlgo).setPermitUnencryptedDownloads(false)
                    .setEncryptionKeyId("simple/example").setEncryptionPrivateKeyBytes(
                            Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));
        }
        config.setTimeout(timeout);
        config.setVerifyUploads(verify);
        config.setMaximumConnections(maxConnections);
        config.setRetries(retries);
        try (MantaClient client = new MantaClient(config)) {
            if (encrypted) {
                EncryptedServerSideMultipartManager multipart = new EncryptedServerSideMultipartManager(client);
                uploadMultipartGeneratedEncrrypted(splitSize, this.noOfSplits, objectName, multipart);
            } else {
                ServerSideMultipartManager multipart = new ServerSideMultipartManager(client);
                uploadMultipartGenerated(splitSize, this.noOfSplits, objectName, multipart);
            }
        } catch (Exception e) {
            result.setResponseData(String.format("Cannot put obj: %s", e.getMessage()).getBytes());
            result.setSuccessful(false);
            result.sampleEnd();
        }
        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }

    /**
     * This will generate a random file to upload. This will just use a random
     * byte array. So if you supply 5242880, 2, this will create a 10MB random
     * file. Once generated and uploaded, we are not keeping the file so we will
     * not be able to
     * 
     * @param size
     *            - The size of each chunk in bytes.
     * @param numParts
     *            - The number of parts that should be generated.
     * @param multipart
     *            - The multipart manager.
     * @throws IOException
     */
    private void uploadMultipartGenerated(final int size,
                                          final int numParts,
                                          final String name,
                                          final ServerSideMultipartManager multipart)
            throws IOException {
        final MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[numParts];
        final ServerSideMultipartUpload upload = multipart.initiateUpload(name);
        for (int i = 0; i < parts.length; i++) {
            byte[] b = new byte[size];
            new Random().nextBytes(b);
            parts[i] = multipart.uploadPart(upload, (i + 1), b);
        }
        final Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
    }

    private static void uploadMultipartGeneratedEncrrypted(final int size,
                                                           final int numParts,
                                                           final String name,
                                                           final EncryptedServerSideMultipartManager multipart)
            throws IOException {
        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(name);
        final MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[numParts];
        for (int i = 0; i < numParts; i++) {
            byte[] b = new byte[size];
            new Random().nextBytes(b);
            parts[i] = multipart.uploadPart(upload, (i + 1), b);
        }
        final Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
    }
}
