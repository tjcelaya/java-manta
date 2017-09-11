package com.joyent.manta.server;

import com.joyent.manta.http.MantaHttpHeaders;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.twmacinta.util.MD5InputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MantaServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantaServer.class);

    private final int port = 10_000; // RandomUtils.nextInt(3_000, 30_000);

    private final Path storage;

    private final Map<Path, Map<String, String>> metadata = new ConcurrentHashMap<>();

    private final HttpServer http;

    public MantaServer() throws IOException {
        try {
            storage = Files.createTempDirectory("manta-storage");
            if (!Files.exists(storage)) {
                Files.createDirectory(storage);
            }

            http = HttpServer.create(new InetSocketAddress(port), 0);
            http.setExecutor(
                    Executors.newFixedThreadPool(
                            Math.floorDiv(Runtime.getRuntime().availableProcessors(), 2)));
            LOGGER.debug("port: " + port);

            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException | IOException e) {
            LOGGER.error("MantaServer failed to start", e);
            throw new IOException(e);
        }

        String location = http.getAddress().toString();
        http.createContext("/", new RequestHandler(location, storage, metadata));
        http.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            http.stop(1);

            final String shouldPreserveData = System.getProperty("manta.mock.preserve");
            if (shouldPreserveData != null) {
                if (shouldPreserveData.equalsIgnoreCase("true") || shouldPreserveData.equalsIgnoreCase("1")) {
                    return;
                }
            }

            try {
                final Stream<Path> files = Files.walk(storage, FileVisitOption.FOLLOW_LINKS);

                files.sorted(Comparator.reverseOrder())
                        .forEach((path) -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                LOGGER.error("Error occurred while cleaning up MantaServer storage", e);
                            }
                        });

                Files.deleteIfExists(storage);
            } catch (IOException e) {
                LOGGER.error("Error occurred while cleaning up MantaServer storage", e);
            }
        }));
    }

    public int getPort() {
        return port;
    }

    private static class RequestHandler implements HttpHandler {

        private final String locationPrefix;

        private final Path storage;

        private final Map<Path, Map<String, String>> metadata;

        private RequestHandler(final String locationPrefix,
                               final Path storage,
                               final Map<Path, Map<String, String>> metadata) {
            this.locationPrefix = locationPrefix;
            this.storage = storage;
            this.metadata = metadata;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String method = exchange.getRequestMethod();
            final String path = exchange.getRequestURI().getPath();

            LOGGER.debug("<< " + method + " " + path);

            final String[] requestPath = path.substring(1).split("/");

            if (requestPath.length < 2) {
                respond(exchange, HttpURLConnection.HTTP_NOT_FOUND);
                return;
            } else if (requestPath[1].equalsIgnoreCase("jobs")) {
                respond(exchange, 500);
                return;
            }

            final Path topLevelDir = storage.resolve(requestPath[0] + File.separator + requestPath[1]);
            if (!Files.exists(topLevelDir)) {
                Files.createDirectories(topLevelDir);
            }

            final Path localPath = resolveObjectPath(path);

            final Headers requestHeaders = exchange.getRequestHeaders();
            final Headers responseHeaders = exchange.getResponseHeaders();
            try {

                switch (method) {
                    case "GET":
                        validateExistence(localPath);
                        discardRequestBody(exchange);
                        final OutputStream responseBody = exchange.getResponseBody();
                        get(exchange, localPath, responseHeaders, responseBody);
                        break;
                    case "HEAD":
                        validateExistence(localPath);
                        discardRequestBody(exchange);
                        head(exchange, localPath, responseHeaders);
                        // discardResponseBody(exchange);
                        break;
                    case "PUT":
                        if (!Files.exists(localPath.getParent())) {
                            throw new ObjectNotFoundException(localPath.getParent(), true);
                        }
                        put(exchange, localPath, requestHeaders);
                        discardResponseBody(exchange);
                        break;
                    case "DELETE":
                        validateExistence(localPath);
                        discardRequestBody(exchange);
                        delete(exchange, localPath);
                        discardResponseBody(exchange);
                        break;
                    default:
                        responseHeaders.add("wat", method);
                        respond(exchange, HttpURLConnection.HTTP_BAD_METHOD);
                }
            } catch (ObjectNotFoundException e) {
                discardRequestBody(exchange);
                final String serverCode, suffix;
                if (e.isDirectory()) {
                    serverCode = "DirectoryDoesNotExist";
                    suffix = "does not exist";
                } else {
                    serverCode = "ResourceNotFound";
                    suffix = "was not found";
                }

                if (method.equalsIgnoreCase("HEAD")) {
                    respond(exchange, HttpURLConnection.HTTP_NOT_FOUND);
                    exchange.close();
                    return;
                }

                final String message = String.format(
                        "{\"code\":\"%s\",\"message\":\"%s %s\"}",
                        serverCode,
                        localPath,
                        suffix);

                respond(exchange, HttpURLConnection.HTTP_NOT_FOUND, message.length());
                final OutputStream responseBody = exchange.getResponseBody();
                IOUtils.copy(new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8)), responseBody);
                responseBody.close();
            } catch (DirectoryNotEmptyException e) {
                final String message = String.format(
                        "{\"code\":\"DirectoryNotEmpty\",\"message\":\"%s is not empty\"}",
                        path);
                respond(exchange, HttpURLConnection.HTTP_BAD_REQUEST, message.length());
                final OutputStream responseBody = exchange.getResponseBody();
                IOUtils.copy(new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8)), responseBody);
                responseBody.close();
            } catch (Exception e) {
                LOGGER.error("exception occurred while handling request", e);
                returnException(exchange);
            }
            exchange.close();
        }

        private void get(final HttpExchange exchange,
                         final Path path,
                         final Headers responseHeaders,
                         final OutputStream responseBody) throws IOException {
            final Map<String, String> meta = metadata.get(path);
            attachPersistedObjectHeaders(responseHeaders, path, meta);

            if (!Files.isDirectory(path)) {
                respond(exchange, HttpURLConnection.HTTP_OK, Files.size(path));
                try (InputStream fileContents = Files.newInputStream(path)) {
                    IOUtils.copy(fileContents, responseBody);
                }
                return;
            }

            try (final Stream<Path> dir = Files.list(path)) {
                final Iterator<Path> it = dir.iterator();
                Path p;
                respond(exchange, HttpURLConnection.HTTP_OK, 0);
                while (it.hasNext()) {
                    p = it.next();
                    String mtime = null;
                    try {
                        mtime = getFormattedMtime(p, false);
                    } catch (IOException e) {
                    }

                    final String json;
                    if (Files.isDirectory(p)) {
                        json = String.format(
                                "{\"name\":\"%s\",\"type\":\"directory\", \"mtime\":\"%s\"}\n",
                                p.getFileName(),
                                mtime
                        );
                    } else {
                        json = String.format(
                                "{\"name\":\"%s\",\"etag\":\"%s\",\"size\": %d,"
                                        + "\"type\":\"object\",\"mtime\": \"%s\",\"durability\": 2}\n",
                                p.getFileName(),
                                DigestUtils.md5Hex(p.toString()),
                                Files.size(path),
                                mtime);
                    }

                    responseBody.write(json.getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        private void head(final HttpExchange exchange,
                          final Path path,
                          final Headers responseHeaders) throws IOException {
            final Map<String, String> meta = metadata.get(path);
            attachPersistedObjectHeaders(responseHeaders, path, meta);

            respond(exchange, HttpURLConnection.HTTP_OK);
        }

        private static final String HEADERS_COMPUTED_MD5 = "Computed-MD5";

        private static final String CONTENT_TYPE_DIRECTORY_REQUEST = "application/json; type=directory";

        private static final String CONTENT_TYPE_DIRECTORY_RESPONSE = "application/x-json-stream; type=directory";

        private void put(final HttpExchange exchange, final Path path, final Headers requestHeaders) throws IOException {
            final HashMap<String, String> meta = new HashMap<>();

            if (Files.exists(path)) {
                meta.put(HttpHeaders.LAST_MODIFIED, getFormattedMtime(path, true));
            }

            if (requestHeaders.getFirst(HttpHeaders.CONTENT_TYPE).equals(CONTENT_TYPE_DIRECTORY_REQUEST)) {
                // noErrorWhenWeOverwriteAnExistingFile

                if (Files.exists(path) && !Files.isDirectory(path)) {
                    Files.delete(path);
                }
                Files.createDirectories(path);
                meta.put(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_DIRECTORY_RESPONSE);
            } else {
                final byte[] computedMD5;

                try (MD5InputStream md5proxy = new MD5InputStream(exchange.getRequestBody())) {
                    Files.copy(md5proxy, path);
                    computedMD5 = md5proxy.getMD5().Final();
                }

                meta.put(HEADERS_COMPUTED_MD5, Base64.getEncoder().encodeToString(computedMD5));
                meta.put(HttpHeaders.CONTENT_TYPE, requestHeaders.getFirst(HttpHeaders.CONTENT_TYPE));
                meta.put(MantaHttpHeaders.HTTP_DURABILITY_LEVEL,
                        ObjectUtils.firstNonNull(requestHeaders.getFirst(MantaHttpHeaders.HTTP_DURABILITY_LEVEL), "2"));
                // meta.put(MantaHttpHeaders.HTTP_DURABILITY_LEVEL, );
            }

            meta.put(HttpHeaders.LOCATION, path.toString());
            metadata.put(path, meta);

            attachMetadataAsHeaders(meta, exchange.getResponseHeaders());

            respond(exchange, HttpURLConnection.HTTP_NO_CONTENT);
        }

        private void delete(final HttpExchange exchange, final Path path) throws IOException {
            Files.delete(path);
            respond(exchange, HttpURLConnection.HTTP_NO_CONTENT);
        }

        private void respond(final HttpExchange exchange,
                             final int responseCode) throws IOException {
            respond(exchange, responseCode, -1);
        }

        private void respond(final HttpExchange exchange,
                             final int responseCode,
                             final long responseLength) throws IOException {
            LOGGER.debug(">> "
                    + exchange.getRequestMethod() + " "
                    + exchange.getRequestURI().getRawPath() + " "
                    + responseCode);
            exchange.sendResponseHeaders(responseCode, responseLength);
        }

        private void returnException(final HttpExchange exchange) throws IOException {
            discardRequestBody(exchange);
            respond(exchange, 500);
            discardResponseBody(exchange);
        }


        void discardRequestBody(HttpExchange exchange) throws IOException {
            final InputStream requestBody = exchange.getRequestBody();
            IOUtils.copy(requestBody, NullOutputStream.NULL_OUTPUT_STREAM);
            requestBody.close();
        }

        void discardResponseBody(HttpExchange exchange) throws IOException {
            final OutputStream responseBody = exchange.getResponseBody();
            responseBody.close();
        }

        private void attachMetadataAsHeaders(Map<String, String> meta, Headers responseHeaders) {
            for (Map.Entry<String, String> metaProp : meta.entrySet()) {
                responseHeaders.add(metaProp.getKey(), metaProp.getValue());
            }
        }

        private void validateExistence(final Path localPath) throws IOException {
            if (!Files.exists(localPath)) {
                throw new ObjectNotFoundException(localPath);
            }

            if (!metadata.containsKey(localPath)) {
                LOGGER.warn("object exists but metadata missing? " + localPath);
                throw new ObjectNotFoundException(localPath);
            }
        }

        private Path resolveObjectPath(final String requestPath) {
            return storage.resolve(requestPath.substring(1));
        }

        private void attachPersistedObjectHeaders(final Headers responseHeaders,
                                                  final Path path,
                                                  final Map<String, String> meta) throws IOException {
            responseHeaders.add(HttpHeaders.LAST_MODIFIED, getFormattedMtime(path, true));

            if (Files.isDirectory(path)) {
                responseHeaders.add(MantaHttpHeaders.RESULT_SET_SIZE, Long.toString(Files.list(path).count()));
            } else {
                responseHeaders.add(HttpHeaders.ETAG, DigestUtils.md5Hex(path.toString()));
                responseHeaders.add(HttpHeaders.CONTENT_LENGTH, Long.toString(Files.size(path)));
            }

            for (Map.Entry<String, String> metaEntry : meta.entrySet()) {
                responseHeaders.add(metaEntry.getKey(), metaEntry.getValue());
            }
        }

        private String getFormattedMtime(Path path, boolean headerFormat) throws IOException {
            final ZonedDateTime mtime =
                    Files.readAttributes(path, BasicFileAttributes.class)
                            .lastModifiedTime()
                            .toInstant()
                            .atZone(ZoneOffset.UTC);
            if (headerFormat) {
                // header uses header date format (Tue, 3 Jun 2008 11:05:30 GMT)
                return DateTimeFormatter.RFC_1123_DATE_TIME.format(mtime);
            }

            // directory listing uses compact UTC (2011-12-03T10:15:30Z)
            return DateTimeFormatter.ISO_INSTANT.format(mtime);
        }

    }

    public static void main(String[] args) throws InterruptedException, IOException {
        new MantaServer();
        while (true) {
            Thread.sleep(100_000L);
        }
    }
}
