package com.joyent.manta.jmeter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

import org.apache.commons.io.IOUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;

/**
 * This class will create a directory in manta in JMeter. It will just read from
 * the UI for the data that it needs.
 * 
 * @author DouglasAnderson
 *
 */
public class DownloadObject extends MantaTester {
	String filename;
	String destFile;
	boolean encrypted;
	String encAlgo=""; 
	//private static transient Logger logger	 = LoggingManager.getLoggerForClass();

	@Override
	public void setupTest(JavaSamplerContext context) {
	}

	@Override
	public Arguments getDefaultParameters() {
		Arguments params = super.getDefaultParameters();
		params.addArgument("file", "/douglas.anderson/stor/temp.txt", "Name of the file to be downloaded");
		params.addArgument("localfile", "/tmp/text.txt","The filepath where you want your file stored");
		params.addArgument("encrypted", "/tmp/text.txt","The filepath where you want your file stored");
		params.addArgument("encryptionAlgorithm", "AES256/CTR/NoPadding");
		return params;
	}

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		filename = context.getParameter("file");
		destFile = context.getParameter("localfile");
		encrypted  = Boolean.parseBoolean(context.getParameter("encrypted"));
		encAlgo = context.getParameter("encryptionAlgorithm");
		
		SampleResult result = new SampleResult();
		result.setDataType(SampleResult.TEXT);
		result.setContentType("text/plain");
		
		ChainedConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
				new SystemSettingsConfigContext());
		if (encrypted) {
			
			config.setClientEncryptionEnabled(true).setEncryptionAlgorithm(encAlgo)
					.setPermitUnencryptedDownloads(false).setEncryptionKeyId("simple/example")
					.setEncryptionPrivateKeyBytes(
							Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));
		}
		File outFile = null;
		try {
			outFile = new File(destFile);
		} catch (Exception e) {
			// TODO: I should add a logger to these classes, and change these to
			// log.debug statements.
			
		}
		result.sampleStart();
		
		try (MantaClient client = new MantaClient(config);
				InputStream inputStream = client.getAsInputStream(filename);
				FileOutputStream fos = new FileOutputStream(new File(destFile))) {
			IOUtils.copy(inputStream, fos);
			result.sampleEnd();
			result.setResponseData("Something needs to be added here".getBytes());
			result.setSuccessful(true);
		} catch (IOException e) {
			result.setResponseData(String.format("Cannot get obj: %s", e.getMessage()).getBytes());
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
