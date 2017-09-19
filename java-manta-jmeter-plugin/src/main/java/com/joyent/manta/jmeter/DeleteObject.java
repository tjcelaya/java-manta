package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;

import java.io.IOException;

/**
 * This class will create a directory in manta in JMeter. It will just read from
 * the UI for the data that it needs.
 *
 * @author DouglasAnderson
 */
public class DeleteObject extends MantaTester {
    private String fileName = "";

    @Override
    public void setupTest(final JavaSamplerContext context) {

    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("fileName", "");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        System.out.println("Run Is called");
        fileName = context.getParameter("fileName");

        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.sampleStart();
        result.setResponseMessageOK();
        result.setResponseCodeOK();

        try (MantaClient client = new MantaClient(config)) {
            client.delete(fileName);
            result.setSuccessful(true);
        } catch (IOException e) {
            e.printStackTrace();
            result.setSuccessful(false);
        }
        result.sampleEnd();
        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }

    @Override
    public String toString() {
        return "DeleteObject [fileName=" + fileName + ", basedir=" + basedir + "]";
    }

    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
