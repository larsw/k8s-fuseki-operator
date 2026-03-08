import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateAction;

public final class RDFDeltaServer {
    private static final String DEFAULT_DATASET_NAME = "delta";
    private static final String SNAPSHOT_FILE_NAME = "dataset.trig";

    private RDFDeltaServer() {}

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.parse(args);
        Files.createDirectories(config.storagePath());

        Dataset dataset = DatasetFactory.createTxnMem();
        Path snapshotFile = config.storagePath().resolve(SNAPSHOT_FILE_NAME);
        loadSnapshot(dataset, snapshotFile);

        HttpServer server = HttpServer.create(new InetSocketAddress(config.port()), 0);
        server.createContext("/$/ping", exchange -> {
            if (!methodIs(exchange, "GET")) {
                sendMethodNotAllowed(exchange, "GET");
                return;
            }
            sendResponse(exchange, 200, "text/plain; charset=utf-8", "pong\n".getBytes(StandardCharsets.UTF_8));
        });
        server.createContext("/" + DEFAULT_DATASET_NAME + "/query", new QueryHandler(dataset));
        server.createContext("/" + DEFAULT_DATASET_NAME + "/update", new UpdateHandler(dataset, snapshotFile));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
        Thread.currentThread().join();
    }

    private static void loadSnapshot(Dataset dataset, Path snapshotFile) {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        Txn.executeWrite(dataset, () -> RDFDataMgr.read(dataset, snapshotFile.toUri().toString(), Lang.TRIG));
    }

    private static void persistSnapshot(Dataset dataset, Path snapshotFile) throws IOException {
        Files.createDirectories(snapshotFile.getParent());
        Path tmpFile = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
        try (OutputStream outputStream = Files.newOutputStream(tmpFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            RDFDataMgr.write(outputStream, dataset, RDFFormat.TRIG_PRETTY);
        }
        try {
            Files.move(tmpFile, snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ignored) {
            Files.move(tmpFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static boolean methodIs(HttpExchange exchange, String expected) {
        return expected.equalsIgnoreCase(exchange.getRequestMethod());
    }

    private static void sendMethodNotAllowed(HttpExchange exchange, String allowedMethod) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowedMethod);
        sendResponse(exchange, 405, "text/plain; charset=utf-8", ("method not allowed\n").getBytes(StandardCharsets.UTF_8));
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String contentType, byte[] body) throws IOException {
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", contentType);
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        try (InputStream inputStream = exchange.getRequestBody()) {
            return inputStream.readAllBytes();
        }
    }

    private static String firstQueryParam(URI uri, String name) {
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }

        for (String pair : rawQuery.split("&")) {
            int separatorIndex = pair.indexOf('=');
            String key = separatorIndex >= 0 ? pair.substring(0, separatorIndex) : pair;
            if (!Objects.equals(decode(key), name)) {
                continue;
            }
            String value = separatorIndex >= 0 ? pair.substring(separatorIndex + 1) : "";
            return decode(value);
        }

        return "";
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private record QueryResponse(String contentType, byte[] body) {}

    private record ServerConfig(int port, Path storagePath) {
        static ServerConfig parse(String[] args) {
            int port = 1066;
            Path storagePath = Path.of("/var/lib/rdf-delta");

            List<String> remaining = new ArrayList<>();
            for (String arg : args) {
                remaining.add(arg);
            }

            for (int index = 0; index < remaining.size(); index++) {
                String arg = remaining.get(index);
                switch (arg) {
                    case "--port" -> {
                        index++;
                        if (index >= remaining.size()) {
                            throw new IllegalArgumentException("missing value for --port");
                        }
                        port = Integer.parseInt(remaining.get(index));
                    }
                    case "--storage" -> {
                        index++;
                        if (index >= remaining.size()) {
                            throw new IllegalArgumentException("missing value for --storage");
                        }
                        storagePath = Path.of(remaining.get(index));
                    }
                    default -> {
                        // Ignore unknown flags to preserve compatibility with future wrapper changes.
                    }
                }
            }

            return new ServerConfig(port, storagePath);
        }
    }

    private static final class QueryHandler implements HttpHandler {
        private final Dataset dataset;

        private QueryHandler(Dataset dataset) {
            this.dataset = dataset;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!methodIs(exchange, "GET")) {
                sendMethodNotAllowed(exchange, "GET");
                return;
            }

            String queryString = firstQueryParam(exchange.getRequestURI(), "query");
            if (queryString.isEmpty()) {
                sendResponse(exchange, 400, "text/plain; charset=utf-8", "missing query parameter\n".getBytes(StandardCharsets.UTF_8));
                return;
            }

            try {
                Query query = QueryFactory.create(queryString);
                QueryResponse response = Txn.calculateRead(dataset, () -> executeQuery(query, exchange.getRequestHeaders()));
                sendResponse(exchange, 200, response.contentType(), response.body());
            } catch (RuntimeException runtimeException) {
                sendResponse(exchange, 400, "text/plain; charset=utf-8", (runtimeException.getMessage() + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }

        private QueryResponse executeQuery(Query query, Headers headers) {
            try (QueryExecution queryExecution = QueryExecutionFactory.create(query, dataset)) {
                if (query.isAskType()) {
                    boolean result = queryExecution.execAsk();
                    return new QueryResponse("text/plain; charset=utf-8", (result ? "true\n" : "false\n").getBytes(StandardCharsets.UTF_8));
                }
                if (query.isSelectType()) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    String accept = headers.getFirst("Accept");
                    if (accept != null && accept.contains("text/csv")) {
                        ResultSetFormatter.outputAsCSV(outputStream, queryExecution.execSelect());
                        return new QueryResponse("text/csv; charset=utf-8", outputStream.toByteArray());
                    }
                    ResultSetFormatter.outputAsJSON(outputStream, queryExecution.execSelect());
                    return new QueryResponse("application/sparql-results+json", outputStream.toByteArray());
                }
                if (query.isConstructType()) {
                    Model model = queryExecution.execConstruct();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    RDFDataMgr.write(outputStream, model, RDFFormat.TURTLE_PRETTY);
                    return new QueryResponse("text/turtle; charset=utf-8", outputStream.toByteArray());
                }
                if (query.isDescribeType()) {
                    Model model = queryExecution.execDescribe();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    RDFDataMgr.write(outputStream, model, RDFFormat.TURTLE_PRETTY);
                    return new QueryResponse("text/turtle; charset=utf-8", outputStream.toByteArray());
                }
                throw new IllegalArgumentException("unsupported query type");
            }
        }
    }

    private static final class UpdateHandler implements HttpHandler {
        private final Dataset dataset;
        private final Path snapshotFile;

        private UpdateHandler(Dataset dataset, Path snapshotFile) {
            this.dataset = dataset;
            this.snapshotFile = snapshotFile;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!methodIs(exchange, "POST")) {
                sendMethodNotAllowed(exchange, "POST");
                return;
            }

            byte[] requestBody = readBody(exchange);
            String updateString = new String(requestBody, StandardCharsets.UTF_8).trim();
            if (updateString.isEmpty()) {
                sendResponse(exchange, 400, "text/plain; charset=utf-8", "missing update body\n".getBytes(StandardCharsets.UTF_8));
                return;
            }

            try {
                Txn.executeWrite(dataset, () -> {
                    UpdateAction.parseExecute(updateString, dataset);
                    try {
                        persistSnapshot(dataset, snapshotFile);
                    } catch (IOException ioException) {
                        throw new RuntimeException(ioException);
                    }
                });
                sendResponse(exchange, 204, "text/plain; charset=utf-8", new byte[0]);
            } catch (RuntimeException runtimeException) {
                Throwable cause = runtimeException.getCause();
                if (cause instanceof IOException ioException) {
                    sendResponse(exchange, 500, "text/plain; charset=utf-8", (ioException.getMessage() + "\n").getBytes(StandardCharsets.UTF_8));
                    return;
                }
                sendResponse(exchange, 400, "text/plain; charset=utf-8", (runtimeException.getMessage() + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}