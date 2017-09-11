package com.joyent.manta.server;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class MantaServerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantaServerTest.class);

    private CloseableHttpClient client;

    @BeforeMethod
    public void setUp() {
        // final RegistryBuilder<ConnectionSocketFactory> factoryRegistryBuilder = RegistryBuilder.create();
        // PoolingHttpClientConnectionManager poolingConnectionManager =
        //         new PoolingHttpClientConnectionManager(
        //                 factoryRegistryBuilder.register("http", PlainConnectionSocketFactory.getSocketFactory()).build(),
        //                 new ManagedHttpClientConnectionFactory(
        //                         new DefaultHttpRequestWriterFactory(),
        //                         new DefaultHttpResponseParserFactory()),
        //                 new ShufflingDnsResolver());
        // // int maxConns ;
        // poolingConnectionManager.setDefaultMaxPerRoute(maxConns);
        // poolingConnectionManager.setMaxTotal(maxConns);
        // poolingConnectionManager.setDefaultSocketConfig(buildSocketConfig());
        // poolingConnectionManager.setDefaultConnectionConfig(buildConnectionConfig());
        // ;
        // client = HttpClients.custom()
        //         .disableAuthCaching()
        //         .disableCookieManagement()
        //         .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
        //         .setMaxConnTotal(maxConns)
        //         .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
        //         .setDefaultHeaders(Arrays.asList(
        //                 new BasicHeader(MantaHttpHeaders.ACCEPT_VERSION, "~1.0"),
        //                 new BasicHeader(HttpHeaders.ACCEPT, "application/json, */*")
        //         ))
        //         .setConnectionManager(poolingConnectionManager)
        //         .setConnectionManagerShared(false)
        //         .setDefaultRequestConfig(RequestConfig.custom()
        //                 .setAuthenticationEnabled(false)
        //                 .setSocketTimeout(timeout)
        //                 .setConnectionRequestTimeout((int) requestTimeout)
        //                 .setContentCompressionEnabled(true)
        //                 .build())
        //
        //         .build()
        client = HttpClients.createMinimal()
        ;
    }

    @AfterMethod
    public void tearDown() {
        try {
            client.close();
        } catch (IOException e) {
            LOGGER.error("error closing client", e);
        }
    }

    @Test
    public void canCreateDirectories() throws Exception {
        final MantaServer server = new MantaServer();
        final String dirPath = "http://localhost:" + server.getPort() + "/mock/stor/dir";

        putAndCheckDir(dirPath);
        putAndCheckDir(dirPath + "/sub1");
        putAndCheckDir(dirPath + "/sub2");
    }

    private void putAndCheckDir(String dirPath) throws IOException, MethodNotSupportedException {
        final CloseableHttpResponse response =
                execute(client, "PUT", dirPath, new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json; type=directory"));

        final Path persisted;
        try (CloseableHttpResponse res = response) {
            LOGGER.debug("object created? " + res);
            final Header location = res.getFirstHeader(HttpHeaders.LOCATION);
            if (location == null) {
                Assert.fail("location header missing "
                        + Arrays.toString(res.getAllHeaders()));
            }

            persisted = Paths.get(location.getValue());
        } catch (IOException e) {
            Assert.fail("error receiving response", e);
            throw e;
        }

        if (!Files.exists(persisted)) {
            Assert.fail("mock storage directory not created: " + persisted);
        }
    }

    public CloseableHttpResponse execute(CloseableHttpClient client, String method, String path, final Header... headers) throws IOException, MethodNotSupportedException {
        final HttpUriRequest request;
        final CloseableHttpResponse response;
        try {
            switch (method) {
                case "GET":
                    request = new HttpGet(path);
                    break;
                case "PUT":
                    request = new HttpPut(path);
                    break;
                default:
                    String msg = "bad method: " + method;
                    Assert.fail(msg);
                    throw new IllegalArgumentException(msg);
            }

            for (Header header : headers) {
                request.addHeader(header);
            }

            response = client.execute(request);
        } catch (IOException e) {
            Assert.fail("error executing request", e);
            throw e;
        }

        return response;
    }
}
