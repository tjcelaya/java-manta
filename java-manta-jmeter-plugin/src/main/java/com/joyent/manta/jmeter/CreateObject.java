package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
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
import java.util.Arrays;
import java.util.Random;

/**
 * This will create an object.
 *
 * @author DouglasAnderson
 */
public class CreateObject extends MantaTester {
    private boolean isBase;
    private StringBuffer dir;
    private int id;
    private int iteration;
    private int size;
    private byte[] data;

    @Override
    public void setupTest(final JavaSamplerContext context) {
        System.out.println("Setup being called");

    }

    @Override
    public Arguments getDefaultParameters() {
        System.out.println("Parameters beign called");
        Arguments params = super.getDefaultParameters();
        params.addArgument("size", String.valueOf(size));
        params.addArgument("id", "${__threadNum}_");
        params.addArgument("directory", "~~/stor/");
        params.addArgument("iteration", "0");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {

        try {
            size = Integer.parseInt(context.getParameter("size"));
            dir = new StringBuffer(context.getParameter("directory"));
            id = Integer.parseInt(context.getParameter("id"));
            iteration = Integer.parseInt(context.getParameter("iteration"));
            data = new byte[size];
            new Random().nextBytes(data);
        } catch (Exception e) {
            // Catching number format exception,
        }
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("Upload File");

        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        System.out.println(dir + " : " + id + iteration);
        dir.append(String.format("/obj-%04d-%02d", id, iteration));
        try (MantaClient client = new MantaClient(config)) {
            result.sampleStart();

            MantaObjectResponse mor = client.put(dir.toString(), data);
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

    @Override
    public String toString() {
        return "CreateObject [isBase=" + isBase + ", dir=" + dir + ", id=" + id + ", iteration=" + iteration + ", size="
                + size + ", data=" + Arrays.toString(data) + ", basedir=" + basedir + "]";
    }

    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
