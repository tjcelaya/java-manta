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

/**
 * This class will create a directory in manta in JMeter. It will just read from
 * the UI for the data that it needs.
 *
 * @author DouglasAnderson
 */
public class SimpleCreateDirectory extends MantaTester {
    private String directory;

    @Override
    public void setupTest(final JavaSamplerContext context) {
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("directory", "");
        // These actually have no place in this call, but they are generally useful.
        params.removeArgument("size");
        params.removeArgument("depth");
        params.removeArgument("dir");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        directory = context.getParameter("directory");

        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("Create Directory");
        result.setSamplerData("");
        result.sampleStart();

        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        try (MantaClient client = new MantaClient(config)) {
            boolean cresult = client.putDirectory(new String(directory));
            String json = "{ \"Directory\":\"" + new String(directory) + "\"}";
            result.setResponseData(json.getBytes());
            result.setSuccessful(cresult);
        } catch (Exception e) {
            e.printStackTrace();
            result.setResponseData(String.format("Cannot create directory: %s", e.getMessage()).getBytes());
            result.setSuccessful(false);
        }
        result.sampleEnd();
        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }

    @Override
    public String toString() {
        return "SimpleCreateDirectory [directory=" + directory + "]";
    }
}
