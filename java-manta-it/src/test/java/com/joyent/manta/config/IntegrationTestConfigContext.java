/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.server.MantaServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.SecretKey;

/**
 * {@link ConfigContext} implementation that loads
 * configuration parameters in an order that makes sense for unit testing
 * and allows for TestNG parameters to be loaded.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class IntegrationTestConfigContext extends SystemSettingsConfigContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegrationTestConfigContext.class);

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in.
     */
    public IntegrationTestConfigContext() {
        super(enableTestEncryption(new StandardConfigContext(), encryptionEnabled(), encryptionCipher()));
        prepareMockMantaIfRequested();
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption) {
        super(enableTestEncryption(new StandardConfigContext(),
                (encryptionEnabled() && usingEncryption == null) ||
                        BooleanUtils.isTrue(usingEncryption), encryptionCipher()));
        prepareMockMantaIfRequested();
    }

    /**
     * Populate configuration from defaults, environment variables, system
     * properties and an addition context passed in. Assigns hard-coded
     * client-side encryption configuration settings.
     */
    public IntegrationTestConfigContext(Boolean usingEncryption, String encryptionCipher) {
        super(enableTestEncryption(new StandardConfigContext(),
                (encryptionEnabled() && usingEncryption == null) ||
                        BooleanUtils.isTrue(usingEncryption), encryptionCipher));
        prepareMockMantaIfRequested();
    }

    private static <T> SettableConfigContext<T> enableTestEncryption(
            final SettableConfigContext<T> context,
            final boolean usingEncryption,
            final String encryptionCipher) {
        if (usingEncryption) {
            context.setClientEncryptionEnabled(true);
            context.setEncryptionKeyId("integration-test-key");
            context.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Optional);

            SupportedCipherDetails cipherDetails = SupportedCiphersLookupMap.INSTANCE.getOrDefault(encryptionCipher,
                    DefaultsConfigContext.DEFAULT_CIPHER);

            context.setEncryptionAlgorithm(cipherDetails.getCipherId());
            SecretKey key = SecretKeyUtils.generate(cipherDetails);
            context.setEncryptionPrivateKeyBytes(key.getEncoded());

            System.out.printf("Unique secret key used for test (base64):\n%s\n",
                    Base64.getEncoder().encodeToString(key.getEncoded()));
        }

        return context;
    }

    public static boolean encryptionEnabled() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_CLIENT_ENCRYPTION_ENABLED_ENV_KEY);

        return BooleanUtils.toBoolean(sysProp) || BooleanUtils.toBoolean(envVar);
    }

    public static String encryptionCipher() {
        String sysProp = System.getProperty(MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY);
        String envVar = System.getenv(EnvVarConfigContext.MANTA_ENCRYPTION_ALGORITHM_ENV_KEY);

        return sysProp != null ? sysProp : envVar;
    }

    private static AtomicReference<MantaServer> server = new AtomicReference<>();

    public void prepareMockMantaIfRequested() {
        final String shouldMockManta = System.getProperty("manta.mock");
        if (shouldMockManta == null
                || shouldMockManta.equalsIgnoreCase("false")
                || shouldMockManta.equalsIgnoreCase("0")) {
            return;
        }

        LOGGER.info("Generating keypair and using embedded Manta");

        final ImmutablePair<File, String> key;
        try {
            key = generatePrivateKey();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (server.get() == null) {
            try {
                server.compareAndSet(null, new MantaServer());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        this.setMantaURL("http://localhost:" + server.get().getPort());
        this.setMantaUser("mock");
        this.setMantaKeyPath(key.left.getAbsolutePath());
        this.setMantaKeyId(key.right);

        this.setRetries(0);
    }

    private ImmutablePair<File, String> generatePrivateKey() throws IOException {
        final KeyPair keyPair;
        try {
            keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            // RSA should always be available
            throw new IOException(e);
        }

        final File keyFile = File.createTempFile("private-key", "");
        FileUtils.forceDeleteOnExit(keyFile);

        try (final FileWriter fileWriter = new FileWriter(keyFile);
             final JcaPEMWriter writer = new JcaPEMWriter(fileWriter)) {

            writer.writeObject(keyPair.getPrivate());
            writer.flush();
        }

        return new ImmutablePair<>(keyFile, KeyFingerprinter.md5Fingerprint(keyPair));
    }

}
