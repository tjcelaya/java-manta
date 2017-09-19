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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * This class will create a directory in manta in JMeter. It will just read from
 * the UI for the data that it needs.
 *
 * @author DouglasAnderson
 */
public class CreateDirectory extends MantaTester {
    private boolean isBase;
    private String baseDir = "";
    private int threadNumber;
    private int iteration;
    private int depth;

    @Override
    public void setupTest(final JavaSamplerContext context) {
        Iterator<String> names = context.getParameterNamesIterator();
        while (names.hasNext()) {
            String currentName = names.next();
        }
        try {
            threadNumber = Integer.parseInt(context.getParameter("ThreadNumber"));
            iteration = Integer.parseInt(context.getParameter("Iteration"));
            depth = Integer.parseInt(context.getParameter("CurrentDepth"));
            isBase = Boolean.parseBoolean(context.getParameter("isBaseDir"));
            baseDir = context.getParameter("BaseDir");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
            // Catching number format exception,
        }
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = super.getDefaultParameters();
        params.addArgument("depth", String.valueOf(depth));
        // I think I can get the thread number from context, but let's just put
        // it here for now.
        params.addArgument("ThreadNumber", "${__threadNum}", "The current thread number",
                "This is used in forming the directory name");
        params.addArgument("Iteration", "");
        params.addArgument("CurrentDepth", "");
        params.addArgument("BaseDir", "");
        params.addArgument("isBaseDir", "");
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        try {
            baseDir = context.getParameter("BaseDir");
            threadNumber = Integer.parseInt(context.getParameter("ThreadNumber"));
            iteration = Integer.parseInt(context.getParameter("Iteration"));
            depth = Integer.parseInt(context.getParameter("CurrentDepth"));
            isBase = Boolean.parseBoolean(context.getParameter("isBaseDir"));

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
            // Catching number format exception,
        }

        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("Create Directory");
        result.setSamplerData("");
        result.sampleStart();
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());

        result.setResponseMessageOK();
        result.setResponseCodeOK();
        if (isBase) {
            String basedir = "/" + System.getenv("MANTA_USER") + "/stor/"
                    + new SimpleDateFormat("yyyyMMdd-HHmmss.S").format(new Date());
            try (MantaClient client = new MantaClient(config)) {
                client.putDirectory(basedir);
                result.setSuccessful(true);
                String json = "{ \"baseDir\":\"" + basedir + "\"}";
                result.setResponseData(json.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
                result.setResponseData(String.format("Cannot create directory: %s", e.getMessage()).getBytes());
                result.setSuccessful(false);
            }
        } else {
            StringBuffer dir = new StringBuffer(baseDir);
            dir.append(String.format("/%04d-%06d-%02d", threadNumber, iteration, depth));
            try (MantaClient client = new MantaClient(config)) {
                boolean cresult = client.putDirectory(new String(dir));
                String json = "{ \"Directory\":\"" + new String(dir) + "\"}";
                result.setResponseData(json.getBytes());
                result.setSuccessful(cresult);
            } catch (Exception e) {
                e.printStackTrace();
                result.setResponseData(String.format("Cannot create directory: %s", e.getMessage()).getBytes());
                result.setSuccessful(false);
            }
        }
        result.sampleEnd();
        result.setSamplerData(config.toString() + "\n" + this.toString());
        return result;
    }


    @Override
    public String toString() {
        return "CreateDirectory [isBase=" + isBase + ", baseDir=" + baseDir + ", threadNumber=" + threadNumber
                + ", iteration=" + iteration + ", depth=" + depth + "]";
    }

    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
