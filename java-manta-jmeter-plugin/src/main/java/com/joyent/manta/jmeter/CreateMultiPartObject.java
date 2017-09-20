/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
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
import org.apache.commons.lang3.RandomUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;

/**
 * This will create a random file of garbage but specific size. With the option
 * for multipart.
 *
 * @author DouglasAnderson
 */
public class CreateMultiPartObject extends MantaTester {
    private String dir;
    private String fileName;
    private int size;
    private int splitSize;
    private byte[] data;
    private boolean multipart;
    private boolean verify;
    private boolean encrypted;
    private int timeout;
    private int retries;
    private int maxConnections;

    @Override
    public void setupTest(final JavaSamplerContext context) {
        System.out.println("Setup being called");

    }

    /**
     * This will have 5 parameters, multipart - boolean - if true will use
     * multipart upload size - The total size of the file. split - T
     */
    @Override
    public Arguments getDefaultParameters() {
        System.out.println("Parameters beign called");
        Arguments params = super.getDefaultParameters();
        params.addArgument("multipart", "true");
        params.addArgument("size", String.valueOf(size));
        params.addArgument("splitSize", "5000");
        params.addArgument("directory", "~~/stor/");
        params.addArgument("fileName", "file1.txt");
        params.addArgument("verifyUpload", "true");
        params.addArgument("encrypted", "true");
        params.addArgument("socketTimeout", "10000");
        params.addArgument("retries", "0");
        params.addArgument("maxConnections", "1");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        // setting vars from input table.
        try {
            dir = context.getParameter("directory");
            fileName = context.getParameter("fileName");
            multipart = Boolean.parseBoolean(context.getParameter("multipart"));

            if (multipart) {
                splitSize = Integer.parseInt(context.getParameter("splitSize").trim());
            }
            size = Integer.parseInt(context.getParameter("size").trim());
            verify = Boolean.parseBoolean(context.getParameter("verifyUpload"));
            encrypted = Boolean.parseBoolean(context.getParameter("encrypted"));
            timeout = Integer.parseInt(context.getParameter("socketTimeout").trim());
            retries = Integer.parseInt(context.getParameter("retries").trim());
            maxConnections = Integer.parseInt(context.getParameter("maxConnections").trim());
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
            config.setClientEncryptionEnabled(true).setEncryptionAlgorithm("AES256/CTR/NoPadding")
                    .setPermitUnencryptedDownloads(false).setEncryptionKeyId("simple/example")
                    .setEncryptionPrivateKeyBytes(
                            Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));
        }
        config.setTimeout(timeout);
        config.setVerifyUploads(verify);
        config.setMaximumConnections(maxConnections);
        config.setRetries(retries);

        try (MantaClient client = new MantaClient(config)) {
            result.sampleStart();
            if (multipart) {
                System.out.println("**************** Multipart : Yes ****************");
                if (encrypted) {
                    System.out.println("**************** Encrypted : Yes ****************");
                    EncryptedServerSideMultipartManager multipart = new EncryptedServerSideMultipartManager(client);
                    encryptedMultipartUpload(multipart, uploadObject, size, splitSize);
                    result.sampleEnd();
                    // There should be something more here.
                    result.setResponseData("Multipart file uploaded successfully.".getBytes());
                    result.setSuccessful(true);
                } else {
                    System.out.println("**************** Encrypted : Noooooooo ****************");
                    ServerSideMultipartManager encryptMultipart = new ServerSideMultipartManager(client);
                    multipartUpload(encryptMultipart, uploadObject, size, splitSize);
                    result.sampleEnd();
                    // There should be something more here.
                    result.setResponseData("Multipart file uploaded successfully.".getBytes());
                    result.setSuccessful(true);
                }
            } else {
                data = RandomUtils.nextBytes(size);
                MantaObjectResponse mor = client.put(uploadObject, data);

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

    private static void encryptedMultipartUpload(final EncryptedServerSideMultipartManager multipart,
                                                 final String uploadObject,
                                                 final int size,
                                                 final int split) throws IOException {
        System.out.println("*********    Using the encrypted manager *********");
        int numUploads = size / split;
        System.out.println("Splitting object : " + uploadObject + " for split upload");
        System.out.println("Creating " + numUploads + " for multipart" + size + " : " + split + " : " + (size / split)
                + (size % split));
        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(uploadObject);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[numUploads];
        for (int i = 0; i < numUploads; i++) {
            parts[i] = multipart.uploadPart(upload, (i + 1), RandomUtils.nextBytes(split));
        }
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        System.out.println(uploadObject + " is now assembled!");
    }

    private static void multipartUpload(final ServerSideMultipartManager multipart,
                                        final String uploadObject,
                                        final int size,
                                        final int split)
            throws IOException {
        int numUploads = size / split;
        System.out.println("Splitting object : " + uploadObject + " for split upload");
        System.out.println("Creating " + numUploads + " for multipart" + size + " : " + split + " : " + (size / split)
                + (size % split));
        ServerSideMultipartUpload upload = multipart.initiateUpload(uploadObject);
        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[numUploads];
        for (int i = 0; i < numUploads; i++) {
            parts[i] = multipart.uploadPart(upload, (i + 1), RandomUtils.nextBytes(split));
        }
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        System.out.println(uploadObject + " is now assembled!");
    }

    @Override
    public String toString() {
        return "CreateMultiPartObject [dir=" + dir + ", fileName=" + fileName + ", size=" + size + ", splitSize="
                + splitSize + ", data=" + Arrays.toString(data) + ", multipart=" + multipart + ", verify=" + verify
                + ", encrypted=" + encrypted + ", timeout=" + timeout + ", retries=" + retries + ", maxConnections="
                + maxConnections + "]";
    }

    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
