package net.twentyonesolutions.m2pg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

public class PgMigrator {

    public final static String DISCL = getProductName() + " Copyright (C) 2021 Igal Sapir\n"
            + "This program comes with ABSOLUTELY NO WARRANTY;\n"
            + "This is free software, and you are welcome to redistribute it\n"
            + "under certain conditions;\n"
            + "See https://www.gnu.org/licenses/gpl-3.0.txt for details\n"
            + "\n"
            + "Available at https://github.com/isapir/Migrate2Postgres";

    public final static String USAGE = "Usage: java <options> "
            + PgMigrator.class.getCanonicalName()
            + " <command> [<config-file> [<output-file>]]\n"
            + "    where command can be:\n"
            + "        ddl - generate DDL script and execute it if the target DB is empty\n"
            + "        dml - copy the data from the source DB to the target DB\n"
            + "        all - run the ddl command and if executed run the dml command";


    public static void main(String[] args) throws Exception {

        System.out.println(DISCL + "\n");

        if (args.length == 0) {
            System.out.println(USAGE);
            System.exit(-1);
        }

        String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());

        String configFile = "", outputFile = "";

        if (args.length > 2)
            outputFile = args[2];

        if (args.length > 1)
            configFile = args[1];

        String action = args[0].toLowerCase();

        Config config = Config.fromFile(configFile);

        if (outputFile.isEmpty())
            outputFile = config.name + "-" + action + "-" + timestamp; // + (action.equals("ddl") ? ".sql" : ".log");

        Schema schema = new Schema(config);

        TimeZone.setDefault(TimeZone.getTimeZone(schema.config.timezone));

        boolean cmdAll = action.equalsIgnoreCase("all");
        boolean cmdDdl = cmdAll || action.equalsIgnoreCase("ddl");
        boolean cmdDml = cmdAll || action.equalsIgnoreCase("dml");

        if (cmdDdl) {
            // set cmdDml to false if we're not executing the DDL because the DB is not empty
            cmdDml = doDdl(schema, outputFile + ".sql") && cmdAll;
        }

        if (cmdDml) {
            doDml(schema, outputFile + ".log");
        }

        if (!cmdDdl && !cmdDml) {
            System.out.println(USAGE);
            System.exit(-1);
        }
    }


    public static boolean doDdl(Schema schema, String filename) throws IOException {

        String ddl = schema.generateDdl();

        Path path = Files.write(Paths.get(filename), ddl.getBytes(StandardCharsets.UTF_8));

        String sqlCheck = "SELECT count(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
                + " AND table_schema NOT IN ('information_schema', 'pg_catalog');";

        long count = -1;
        try (Connection conn = schema.config.connect(schema.config.target)) {
            count = Util.selectLong(sqlCheck, conn);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        boolean executeDdl = (count == 0);

        if (executeDdl) {

            System.out.println("\n\nDatabase is empty. Executing DDL Script.");

            StringBuilder log = new StringBuilder(1024);
            List<String> queries = Arrays.asList(ddl);
            Util.executeQueries(queries, log, schema.config);

            System.out.println("\n\nExecuted DDL Script:");
            System.out.println(log);
        } else {

            System.out.println("\n\nDatabase is not empty. Not executing DDL Script.");
        }

        System.out.println("\n\nSee generated DDL file at " + path.toAbsolutePath().toString() + "\n");

        return executeDdl;
    }


    public static void doDml(Schema schema, String filename) throws IOException, SQLException {
        IProgress progress = new ProgressOneline(schema);

        long tc = System.currentTimeMillis();

        List<String> queries = null;
        String logentry;
        boolean completed = false;
        Path path = Paths.get(filename);
        Util.log(path, getBanner());

        queries = (List<String>) schema.config.dml.getOrDefault("execute.before_all", Collections.EMPTY_LIST);
        executeQueriesFromFiles(queries, path, schema, "execute.before_all");

        int numThreads = 1;

        Object arg = schema.config.dml.get("threads");

        if (arg != null) {

            if (arg instanceof Number) {
                numThreads = ((Number) arg).intValue();
            } else if (arg instanceof String) {

                if (((String) arg).equalsIgnoreCase("cores")) {
                    numThreads = Runtime.getRuntime().availableProcessors();
                } else {

                    try {
                        numThreads = Integer.parseInt((String) arg);
                    } catch (NumberFormatException ex) {
                        System.err.println("Failed to parse value of [dml.threads]");
                    }
                }
            } else {
                System.err.println("[dml.threads] has an invalid value: " + arg.toString());
            }
        }

        System.out.println("Executing DML with " + numThreads + " concurrent connections");

        List<FutureTask<String>> tasks;
        List<String> tableNames = schema.schema.keySet().stream().sorted().collect(Collectors.toList());
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        // Copy data.
        tasks = tableNames
                .stream()
                .map(tableName -> {

                    Callable<String> callable = () -> {
                        try {
                            return schema.copyTable(tableName, progress);

                        } catch (Exception ex) {
                            return ex.getMessage();
                        }
                    };

                    return new FutureTask<>(callable);
                })
                .collect(Collectors.toList());

        executeTasks(tasks, executorService, path);
        executorService.shutdown();

        queries = (List<String>) schema.config.dml.getOrDefault("execute.after_all", Collections.EMPTY_LIST);
        executeQueriesFromFiles(queries, path, schema, "execute.after_all");

        tc = System.currentTimeMillis() - tc;

        logentry = String.format("-- %tT Completed in %.3f seconds\n", System.currentTimeMillis(), tc / 1000.0);
        Util.log(path, logentry);

        System.out.println("\n" + logentry);
        System.out.println("See log and recommended actions at " + path.toAbsolutePath() + "\n");
    }


    public static String getProductName() {

        String result = PgMigrator.class.getPackage().getImplementationTitle();

        return result != null ? result : "Migrate2Postgres";
    }


    public static String getProductVersion() {

        String result = PgMigrator.class.getPackage().getImplementationVersion();

        return result != null ? result : "";
    }


    public static String getBanner() {

        return "/**\n"
                + "\tScripted by "
                + getProductName()
                + " "
                + getProductVersion()
                + " on "
                + ZonedDateTime.now().format(RFC_1123_DATE_TIME)
                + "\n\n\t"
                + DISCL.replace("\n", "\n\t")
                + "\n**/\n\n";
    }

    private static void executeTasks(List<FutureTask<String>> tasks, ExecutorService executorService, Path logPath) {

        for (FutureTask<String> task: tasks) {
            executorService.execute(task);
        }

        tasks
                .parallelStream()
                .forEach(task -> {
                    try {
                        String entry = task.get() + "\n";
                        Util.log(logPath, entry);

                    } catch (InterruptedException | ExecutionException | IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    private static void executeQueriesFromFiles(List<String> queries, Path logPath, Schema schema, String settingName) throws IOException {
        if (!queries.isEmpty()) {

            String logentry = String.format("-- executing SQL queries from files in %s", settingName);
            System.out.println("\n" + logentry);
            Util.log(logPath, logentry);

            StringBuilder log = new StringBuilder(1024);
            boolean completed = Util.executeQueries(queries, log, schema.config);

            if (!completed) {
                Util.log(logPath, "!!!ABORTING!!!");
                System.exit(1);
            }

            logentry = "-- completed queries execute.before_all";
            System.out.println("\n" + logentry);
            Util.log(logPath, logentry);
        }
    }

    static class ProgressOneline implements IProgress {

        final ConcurrentMap<String, Status> processing = new ConcurrentHashMap();
        final ConcurrentMap<String, Table> remaining = new ConcurrentHashMap<>();
        volatile String lastLine = "";
        AtomicLong lastReport = new AtomicLong(System.currentTimeMillis());

        public ProgressOneline(Schema schema) {
            this.remaining.putAll(schema.schema);
        }

        @Override
        public void progress(Status status) {

            if (status.row >= status.rowCount) {     // if new records were added to the table while copying then row will be greater than rowCount

                String statusLine = status.toString();

                System.out.printf("\r%s", statusLine);

                if (lastLine.length() > statusLine.length()) {
                    System.out.print("                                                           ");
                }

                processing.remove(status.name);
                this.remaining.remove(status.name);
                return;
            }

            processing.put(status.name, status);

            if (System.currentTimeMillis() - lastReport.get() < 100)
                return;
            lastReport.set(System.currentTimeMillis());

            StringBuilder sb = new StringBuilder(512);

            sb.append(remaining.size())
                    .append(" table")
                    .append(remaining.size() > 1 ? "s " : " ")
                    .append("left");

            for (Map.Entry<String, Status> e : processing.entrySet()) {

                sb.append("   ");
                String name = e.getKey();

                name = name.substring(name.indexOf(".") + 1);

                Status s = e.getValue();
                sb.append(name).append(": ");

                if (s.rowCount > 10_000_000)
                    sb.append(String.format("%.2f%%", 100.0 * s.row / s.rowCount));
                else if (s.rowCount > 10_000)
                    sb.append(String.format("%.1f%%", 100.0 * s.row / s.rowCount));
                else
                    sb.append(String.format("%d%%", 100 * s.row / s.rowCount));
            }

            String statusLine = sb.toString();
            if (!statusLine.equals(lastLine)) {

                System.out.printf("\r%s", statusLine);
                if (lastLine.length() > statusLine.length())
                    System.out.print("                                                           ");

                lastLine = statusLine;
            }
        }
    }

    static class ProgressVerbose implements IProgress {

        String lastReport = "";

        @Override
        public void progress(Status status) {

            String report = String.format("%.1f", 100.0 * status.row / status.rowCount);

            if (!lastReport.equals(report) || status.row == status.rowCount) {
                lastReport = report;
                System.out.println(status);
            }
        }
    }

}
