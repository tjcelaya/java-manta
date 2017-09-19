package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;

import java.io.File;
import java.io.IOException;
import java.util.Base64;

/**
 * This will have the flag to add the options to encrypt the file using a simple encryption
 *
 * @author DouglasAnderson
 */
public class CreateEncryptedObject extends MantaTester {
    boolean isBase;
    StringBuffer dir;
    String localfile;
    byte[] data;

    @Override
    public void setupTest(final JavaSamplerContext context) {
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("name", "", "name you want to give obj once it is uploaded");
        params.addArgument("directory", "~~/stor/");
        params.addArgument("localfile", "/tmp/text.txt", "This will be an absolute path to the file you want to upload");
        params.addArgument("connectionTimeout", "10000", "This will be an absolute path to the file you want to upload");
        params.addArgument("verifyUpload", "true", "Verify the uplaod MD5");
        params.addArgument("Encrypt", "true", "Encrypt the package");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        dir = new StringBuffer(context.getParameter("directory"));
        if (!dir.toString().trim().endsWith("/")) {
            dir.append('/');
        }
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("Upload Encrypted File");

        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setClientEncryptionEnabled(true)
                .setEncryptionAlgorithm("AES256/CTR/NoPadding")
                .setPermitUnencryptedDownloads(false)
                .setEncryptionKeyId("simple/example")
                .setTimeout(Integer.parseInt(context.getParameter("connectionTimeout")))
                .setVerifyUploads(Boolean.parseBoolean(context.getParameter("verifyUpload")))
                .setEncryptionPrivateKeyBytes(Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));
        dir.append(context.getParameter("name"));

        try (MantaClient client = new MantaClient(config)) {
            File targetFile = new File(context.getParameter("localfile"));
            result.sampleStart();
            MantaObjectResponse mor = client.put(dir.toString(), targetFile);
            result.sampleEnd();
            result.setResponseData(mor.toString().getBytes());
            result.setSuccessful(true);
        } catch (IOException e) {
            result.setResponseData(String.format("Cannot put obj: %s", e.getMessage()).getBytes());
            result.setSuccessful(false);
            result.sampleEnd();
        }


        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }


    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
