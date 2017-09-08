package com.joyent.manta.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.System.err;

public class MantaServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantaServer.class);

    private static final int mantaPort = RandomUtils.nextInt(3_000, 30_000);

    private static final Path storage;

    private static final HttpServer server;

    static {
        try {
            storage = Files.createTempDirectory("manta-storage");
            server = HttpServer.create(new InetSocketAddress(mantaPort), 0);
            LOGGER.debug("Manta Server port: " + mantaPort);
        } catch (IOException e) {
            err.println("failed to start manta");
            throw new UncheckedIOException(e);
        }

        server.createContext("/", new RequestHandler());
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(1);
        }));
    }

    public static HttpServer getServer() {
        return server;
    }

    private static class RequestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            LOGGER.debug("Manta Server Request "
                    + exchange.getRequestMethod() + " "
                    + exchange.getRequestURI().getRawPath());

            final String[] requestPath = exchange.getRequestURI().getPath().substring(1).split("/");

            if (requestPath.length < 2) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, 0);
                exchange.close();
                return;
            }

            final Path topLevelDir = storage.resolve(requestPath[0] + File.separator + requestPath[1]);
            if (!Files.exists(topLevelDir)) {
                Files.createDirectories(topLevelDir);
            }

            switch (exchange.getRequestMethod()) {
                case "GET":
                    get(exchange);
                    break;
                case "PUT":
                    put(exchange);
                    break;
                default:
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            }

            exchange.close();
        }

        private void get(final HttpExchange exchange) throws IOException {
            final Path requestPath = storage.resolve(exchange.getRequestURI().getPath());
            if (Files.exists(requestPath)) {
                try (InputStream fileContents = Files.newInputStream(requestPath);
                     OutputStream response = exchange.getResponseBody()) {
                    IOUtils.copy(fileContents, response);
                }
            }
        }

        private static final String CONTENT_TYPE_DIRECTORY = "application/json; type=directory";

        private void put(final HttpExchange exchange) throws IOException {
            final Path requestPath = storage.resolve(exchange.getRequestURI().getPath().substring(1));

            if (exchange.getRequestHeaders().getFirst(HttpHeaders.CONTENT_TYPE).equals(CONTENT_TYPE_DIRECTORY)) {
                Files.createDirectories(requestPath);
            } else {
                try (InputStream requestBody = exchange.getRequestBody();
                     OutputStream fileOutput = Files.newOutputStream(requestPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    IOUtils.copy(requestBody, fileOutput);
                }
            }

            final Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.add(HttpHeaders.LOCATION, requestPath.toString());

            exchange.sendResponseHeaders(HttpStatus.SC_NO_CONTENT, 0);
        }
    }
}
