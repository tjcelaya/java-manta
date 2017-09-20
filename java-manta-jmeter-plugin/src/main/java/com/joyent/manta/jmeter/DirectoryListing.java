package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class will create a directory in manta in JMeter. It will just read from
 * the UI for the data that it needs.
 *
 * @author DouglasAnderson
 */
public class DirectoryListing extends MantaTester {
    private String baseDir = "";
    private String filter = "";

    @Override
    public void setupTest(final JavaSamplerContext context) {
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("directory", "");
        params.addArgument("filter", "");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        baseDir = context.getParameter("directory");
        // Filter will just use the simple contains for matching
        filter = context.getParameter("filter");
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());

        SampleResult result = new SampleResult();

        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("List Directory Contents");
        result.sampleStart();
        result.setResponseMessageOK();
        result.setResponseCodeOK();

        try (MantaClient client = new MantaClient(config)) {
            Stream<MantaObject> listing = client.listObjects(baseDir);
            result.setSamplerData("mls " + baseDir);
            List<MantaObject> objList = listing.filter(item -> {
                System.out.println(item.getPath() + " : " + filter + " : " + item.getPath().contains(filter));
                return item.getPath().contains(filter);
            }).collect(Collectors.toList());
            String resultString = "{\"Results\":[";
            int count = 0;
            for (MantaObject obj : objList) {
                resultString += "{\"result\":" + "\"" + obj.getPath() + "\"},";
                count++;
            }
            resultString += "]}";
            result.setResponseData(resultString.getBytes());
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
        return "DirectoryListing [baseDir=" + baseDir + ", filter=" + filter + ", basedir=" + basedir + "]";
    }
}
