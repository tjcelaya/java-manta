package com.joyent.manta.jmeter;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class MantaTester extends AbstractJavaSamplerClient {
    private Map<String, String> mapParams = new HashMap<String, String>();

    protected static final AtomicReference<MantaClient> mantaClientRef = new AtomicReference<>(null);

    protected int size = 1024;
    protected int depth = 7;
    protected String basedir = "/" + System.getenv("MANTA_USER") + "/stor/"
            + new SimpleDateFormat("yyyyMMdd-HHmmss.S").format(new Date());

    public MantaTester() {
    }

    @Override
    public void setupTest(final JavaSamplerContext context) {
        for (Iterator<String> it = context.getParameterNamesIterator(); it.hasNext(); ) {
            String paramName = it.next();
            mapParams.put(paramName, context.getParameter(paramName));
        }
        if (mapParams.containsKey("size")) {
            size = Integer.parseInt(mapParams.get("size"));
        }
        if (mapParams.containsKey("depth")) {
            depth = Integer.parseInt(mapParams.get("depth"));
        }
        if (mapParams.containsKey("dir")) {
            basedir = mapParams.get("dir");
        }

        ensureMantaClientInitialized();
    }

    private void ensureMantaClientInitialized() {
        if (mantaClientRef.get() != null) {
            return;
        }
        final ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext());
        final MantaClient client = new MantaClient(config);
        mantaClientRef.compareAndSet(null, client);
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("Manta URL", System.getenv("MANTA_URL"));
        params.addArgument("MANTA_USER", System.getenv("MANTA_USER"));
        params.addArgument("MANTA_KEY_ID", System.getenv("MANTA_KEY_ID"));
        return params;
    }

    @Override
    public SampleResult runTest(final JavaSamplerContext context) {
        SampleResult result = new SampleResult();
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        MantaClient client = new MantaClient(config);
        result.setResponseCodeOK();
        try {
            client.putDirectory(basedir, false);
        } catch (IOException e) {

        }
        client.close();

        return result;
    }

    protected SampleResult failWithException(final SampleResult result, final Exception e, final String message) {

        final StringWriter errorWriter = new StringWriter();
        if (message != null) {
            errorWriter.append(message);
            errorWriter.append(System.lineSeparator());
        }

        errorWriter.append(ExceptionUtils.getStackTrace(e));

        result.setResponseData(errorWriter.toString(), StandardCharsets.UTF_8.name());

        result.setSuccessful(false);

        return result;
    }

}
