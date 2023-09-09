package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import java.io.*;
import java.util.UUID;
import java.util.function.Supplier;

public interface ExecutorUtils {

    static void documentExecution(Runnable runnable, String... args) {
        long start = System.nanoTime();
        runnable.run();
        long finish = System.nanoTime();

        File executionsDir = new File("/Users/bence/Downloads");
        if (!executionsDir.exists()) {
            executionsDir.mkdir();
        }
        File executionTimeResult = new File(executionsDir, "sql_run_" + finish + "_" + UUID.randomUUID());
        try {
            executionTimeResult.createNewFile();
        } catch (Exception ignored) {}

        try (FileWriter fw = new FileWriter(executionTimeResult)) {
            fw.append("Execution time: %d ms".formatted((finish - start) / 1_000_000));
            fw.append("\n".repeat(5));
            for (String arg : args) {
                fw.append(arg);
            }
        } catch (Exception ignored) {}
    }

    static <T> T documentExecution(Supplier<T> supplier, String... args) {
        long start = System.nanoTime();
        T t = supplier.get();
        long finish = System.nanoTime();

        File executionsDir = new File("/Users/bence/Downloads");
        if (!executionsDir.exists()) {
            executionsDir.mkdir();
        }
        File executionTimeResult = new File(executionsDir, "sql_run_" + finish + "_" + UUID.randomUUID());
        try {
            executionTimeResult.createNewFile();
        } catch (Exception ignored) {}

        try (FileWriter fw = new FileWriter(executionTimeResult)) {
            fw.append("Execution time: %d ms".formatted((finish - start) / 1_000_000));
            fw.append("\n".repeat(5));
            for (String arg : args) {
                fw.append(arg);
            }
        } catch (Exception ignored) {}

        return t;
    }

}
