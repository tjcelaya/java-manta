/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
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
 * This will call the recursive delte for the Manta client.
 *
 * It should wipe out the directory we point to.
 *
 * @author DouglasAnderson
 */
public class DeleteDirectory extends MantaTester {

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
        basedir = context.getParameter("directory");

        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(), new EnvVarConfigContext(),
                new SystemSettingsConfigContext());
        SampleResult result = new SampleResult();
        result.setDataType(SampleResult.TEXT);
        result.setContentType("text/plain");
        result.setSampleLabel("Delete Directory");
        result.sampleStart();
        result.setResponseMessageOK();
        result.setResponseCodeOK();

        try (MantaClient client = new MantaClient(config)) {
            client.deleteRecursive(basedir);
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
        return "DeleteDirectory [baseDir=" + basedir + "]";
    }

    public JMeterContext getThreadContext() {
        return JMeterContextService.getContext();
    }

}
