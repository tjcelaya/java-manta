package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.client.multipart.EncryptedServerSideMultipartManager;
import com.joyent.manta.client.multipart.MantaMultipartUploadTuple;
import com.joyent.manta.client.multipart.ServerSideMultipartManager;
import com.joyent.manta.client.multipart.ServerSideMultipartUpload;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.commons.io.IOUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;

/**
 * This will uplaod a file, it has the option of being encrypted and/or multipart uploaded.
 * 
 * @author DouglasAnderson
 */
public class UploadMultipartFile extends MantaTester {

    private String dir;
    private String fileName;
    private int splitSize;
    private byte[] data;
    private boolean multipart;
    private boolean verify;
    private boolean encrypted;
    private int timeout;
    private int retries;
    private int maxConnections;
    private String localfile;
    private String encAlgo = "";

    @Override
    public void setupTest(JavaSamplerContext context) {
        getNewLogger().debug("Setup being called");
    }

    /**
     * This will have 5 parameters, multipart - boolean - if true will use
     * multipart upload size - The total size of the file. split - T
     */
    @Override
    public Arguments getDefaultParameters() {
        getNewLogger().debug("Parameters beign called");
        Arguments params = super.getDefaultParameters();
        params.addArgument("multipart", "true");
        params.addArgument("splitSize", "5000");
        params.addArgument("directory", "~~/stor/");
        params.addArgument("fileName", "file1.txt");
        params.addArgument("localfile", "/tmp/upload.zip");
        params.addArgument("verifyUpload", "true");
        params.addArgument("encrypted", "true");
        params.addArgument("socketTimeout", "10000");
        params.addArgument("retries", "0");
        params.addArgument("maxConnections", "1");
        params.addArgument("encryptionAlgorithm", "AES256/CTR/NoPadding");
        return params;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        // setting vars from input table.
        try {
            dir = context.getParameter("directory");
            fileName = context.getParameter("fileName");
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
        } catch (Exception e) {
            // Catching number format exception, if we have one we are going to
            // fail the test.
            SampleResult result = new SampleResult();
            result.setSuccessful(false);
            result.setResponseData(String.format("Parameter incorrect %s", e.getMessage()).getBytes());
            return result;
        }
        String uploadObject = dir + fileName;
        // Setting up return value.
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");

        // Setting up the config && client
        ChainedConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        if (encrypted) {
            getNewLogger().debug("Setting encrypted settings to true");
            config.setClientEncryptionEnabled(true).setEncryptionAlgorithm(encAlgo).setPermitUnencryptedDownloads(false)
                    .setEncryptionKeyId("simple/example").setEncryptionPrivateKeyBytes(
                            Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));
        }
        config.setTimeout(timeout);
        config.setVerifyUploads(verify);
        config.setMaximumConnections(maxConnections);
        config.setRetries(retries);
        getNewLogger().debug("Multipart ? : " + multipart + " multiple files  : " + localfile.contains(","));
        try (MantaClient client = new MantaClient(config)) {
            result.sampleStart();
            getNewLogger().debug("Multipart ? : " + multipart + " multiple files  : " + localfile.contains(","));
            if (multipart) {
                if (localfile.contains(",")) {
                    if (encrypted) {
                        EncryptedServerSideMultipartManager multipart = new EncryptedServerSideMultipartManager(client);
                        multipleFileMPUEncrypted(multipart, uploadObject, localfile);
                    } else {
                        ServerSideMultipartManager multipart = new ServerSideMultipartManager(client);
                        multipleFileMPU(multipart, uploadObject, localfile);
                    }
                    result.sampleEnd();
                    result.setResponseData("Multipart file uploaded successfully.".getBytes());
                    result.setSuccessful(true);
                    return result;
                }
                if (encrypted) {
                    getNewLogger().debug("**************** Encrypted : Yes ****************");
                    EncryptedServerSideMultipartManager multipart = new EncryptedServerSideMultipartManager(client);
                    encryptedMultipartUpload(multipart, uploadObject, localfile, splitSize);
                    result.sampleEnd();
                    // There should be something more here.
                    result.setResponseData("Multipart file uploaded successfully.".getBytes());
                    result.setSuccessful(true);
                } else {
                    ServerSideMultipartManager multipart = new ServerSideMultipartManager(client);
                    multipartUpload(multipart, uploadObject, localfile, splitSize);
                    result.sampleEnd();
                    // There should be something more here.
                    result.setResponseData("Multipart file uploaded successfully.".getBytes());
                    result.setSuccessful(true);
                }
            } else {
                // We are going to upload it all in one shot.
                File spittingFile = new File(localfile);
                FileInputStream fstream = new FileInputStream(spittingFile);
                byte[] fileBytes = IOUtils.toByteArray(fstream);

                MantaObjectResponse mor = client.put(uploadObject, fileBytes);
                result.sampleEnd();
                result.setResponseData(mor.toString().getBytes());
                result.setSuccessful(true);
            }

        } catch (Exception e) {
            result.setResponseData(String.format("Cannot put obj: %s", e.getMessage()).getBytes());
            result.setSuccessful(false);
            result.sampleEnd();
        }
        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }

    private void encryptedMultipartUpload(final EncryptedServerSideMultipartManager multipart, final String uploadObject,
            String uploadFile, int split) throws IOException {
        getNewLogger().debug("*********    Using the encrypted manager *********");
        File spittingFile = new File(uploadFile);
        FileInputStream fstream = new FileInputStream(spittingFile);
        byte[] fileBytes = IOUtils.toByteArray(fstream);
        int size = fileBytes.length;
        int numUploads = fileBytes.length / split;
        // Take care of remainder of bits.
        if (fileBytes.length % split > 0) {
            numUploads++;
        }
        getNewLogger().debug("Splitting object : " + uploadFile + " for split upload");
        getNewLogger().debug("Creating " + numUploads + " for multipart" + size + " : " + split + " : " + (size / split)
                + " with remainder " + (size % split));
        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(uploadObject);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[numUploads];
        int curByte = 0;
        getNewLogger().debug("Starting the process of creating the parts");
        for (int i = 0; i < numUploads - 1; i++) {
            getNewLogger().debug("Current Byte : " + curByte + " of " + split);
            parts[i] = multipart.uploadPart(upload, (i + 1), Arrays.copyOfRange(fileBytes, curByte, curByte + split));
            curByte = curByte + split + 1;
        }
        parts[numUploads - 1] = multipart.uploadPart(upload, numUploads,
                Arrays.copyOfRange(fileBytes, curByte, size - 1));
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        getNewLogger().debug(uploadObject + " is now assembled!");
    }

    private void multipartUpload(final ServerSideMultipartManager multipart, final String uploadObject, final String uploadFile,
            final int split) throws Exception {
        File splittingFile = new File(uploadFile);
        FileInputStream fstream = new FileInputStream(splittingFile);
        long size = splittingFile.length();
        int splits = Math.floorDiv((int) size, this.splitSize);
        int remainder = ((int) size) % splitSize;
        getNewLogger().debug("Splitting file, size :  " + size + " into :" + splits + " file parts, the last part is : "
                + remainder);
        MantaMultipartUploadTuple[] parts;
        if (remainder == 0) {
            getNewLogger().debug(" No remainder here");
            parts = new MantaMultipartUploadTuple[splits];
        } else {
            parts = new MantaMultipartUploadTuple[splits + 1];
        }
        ServerSideMultipartUpload upload = multipart.initiateUpload(uploadObject);
        getNewLogger().debug("HERE 2");
        for (int i = 0; i < splits; i++) {
            getNewLogger().debug("Adding part : " + i);
            parts[i] = multipart.uploadPart(upload, (i + 1), IOUtils.toByteArray(fstream, splitSize));
            getNewLogger().debug("grabbed the next " + splitSize + " bytes");
        }

        // All this does is take care of the remainder.
        if (remainder > 0) {
            getNewLogger().debug("Adding the last part");
            parts[splits] = multipart.uploadPart(upload, (splits + 1), IOUtils.toByteArray(fstream, remainder));
            getNewLogger().debug("grabbed the next " + remainder + " bytes");
        }

        getNewLogger().debug("Adding the array for the stream having " + parts.length + " parts");
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        getNewLogger().debug(uploadObject + " is now assembled!");
    }

    /**
     * This method is going to be a little odd, this one will take a split file
     * and upload each part, split works much better for me than trying to
     * rewrite split.
     * 
     * @param multipart
     * @param uploadObject
     * @param uploadFile
     * @throws Exception
     */
    private void multipleFileMPUEncrypted(final EncryptedServerSideMultipartManager multipart, final String uploadObject,
            final String uploadFile) throws Exception {
        String[] uploadFiles = uploadFile.split(",");
        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(uploadObject);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[uploadFiles.length];
        for (int i = 0; i < uploadFiles.length; i++) {
            FileInputStream curStream = new FileInputStream(new File(uploadFiles[i]));
            byte[] arr = IOUtils.toByteArray(curStream);
            parts[i] = multipart.uploadPart(upload, (i + 1), arr);
            getNewLogger().debug("Part uploaded " + i + " of " + uploadFiles.length);
        }
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        getNewLogger().debug(uploadObject + " is now assembled!");
    }

    /**
     * This method is going to be a little odd, this one will take a split file
     * and upload each part, split works much better for me than trying to
     * rewrite split.
     * 
     * @param multipart
     * @param uploadObject
     * @param uploadFile
     * @throws Exception
     */
    private void multipleFileMPU(final ServerSideMultipartManager multipart, final String uploadObject, final String uploadFile)
            throws Exception {
        String[] uploadFiles = uploadFile.split(",");
        ServerSideMultipartUpload upload = multipart.initiateUpload(uploadObject);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[uploadFiles.length];
        for (int i = 0; i < uploadFiles.length; i++) {
            FileInputStream curStream = new FileInputStream(new File(uploadFiles[i]));
            byte[] arr = IOUtils.toByteArray(curStream);
            parts[i] = multipart.uploadPart(upload, (i + 1), arr);
            getNewLogger().debug("Part uploaded " + i + " of " + uploadFiles.length);
        }
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        getNewLogger().debug(uploadObject + " is now assembled!");
    }

    @Override
    public String toString() {
        return "CreateMultiPartObject [dir=" + dir + ", fileName=" + fileName + ", size=" + size + ", splitSize="
                + splitSize + ", data=" + Arrays.toString(data) + ", multipart=" + multipart + ", verify=" + verify
                + ", encrypted=" + encrypted + ", timeout=" + timeout + ", retries=" + retries + ", maxConnections="
                + maxConnections + "]";
    }
}
