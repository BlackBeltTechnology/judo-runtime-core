package hu.blackbelt.judo.runtime.core.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class JudoProcessHandler {

    public static final String JUDO_PID_FILE = "judoPidFile";
    public static final String JUDO_PID_NAME = "judoPidName";
    public static final String JUDO_DEFAULT_PID_NAME = "judo.pid";

    final RuntimeVariableResolver runtimeVariableResolver;
    final Runnable shutdownHook;
    final File judoPidFile;

    @Builder
    public JudoProcessHandler(RuntimeVariableResolver runtimeVariableResolver,
                              File judoPidFile,
                              Runnable shutdownHook) {
        if (runtimeVariableResolver != null) {
            this.runtimeVariableResolver = runtimeVariableResolver;
        } else {
            this.runtimeVariableResolver = RuntimeVariableResolver.builder()
                    .prefix("judo")
                    .variableResolvers(Arrays.asList(new SystemPropertiesVariableResolver(),
                            new EnvironmentVariableResolver()))
                    .build();
        }
        if (shutdownHook != null) {
            this.shutdownHook = shutdownHook;
        } else {
            this.shutdownHook = () -> {};
        }
        this.judoPidFile = judoPidFile;
    }

    public long getCurrentProcessPid() {
        int pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        return pid;
    }

    public File getApplicationRoot() {
        try {
            String className = JudoProcessHandler.class.getName().replace('.', '/') + ".class";
            URI rootUri = null;
            try {
                rootUri = Thread.currentThread().getContextClassLoader().getResource("").toURI();
            } catch (Exception e) {
                rootUri = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI();
            }
            if (rootUri.toURL().toString().endsWith(".jar")) {
                rootUri = Path.of(rootUri).getParent().toUri();
            }
            File pidPath = Path.of(rootUri).toFile();
            return pidPath;
        } catch (URISyntaxException | MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public File getPidFile() {
        if (this.judoPidFile != null) {
            return judoPidFile;
        }
        File pidPath = getApplicationRoot();
        File pidFile = new File(runtimeVariableResolver.getVariableAsString(JUDO_PID_FILE,
                new File(pidPath, runtimeVariableResolver.getVariableAsString(JUDO_PID_NAME, JUDO_DEFAULT_PID_NAME)).getAbsolutePath()));
        return pidFile;
    }

    public boolean checkPid() {
        File pidFile = getPidFile();
        if (pidFile.exists()) {
            Optional<Long> pid = readPid();
            // If pid file is not validate, delete it
            if (pid.isEmpty()) {
                pidFile.delete();
            } else {
                // Check pid number
                long currentPid = getCurrentProcessPid();
                // The current process is different from the process registered in file
                if (currentPid != pid.get()) {
                    if (ProcessHandle.of(pid.get()).isPresent()) {
                        log.error("Another instance of application is already running");
                        return false;
                    } else {
                        pidFile.delete();
                    }
                    // The current process is same from the process registered in file
                } else {
                    log.error("Pid file already presented for this application instance");
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isRunning() {
        Optional<Long> pid = readPid();
        if (pid.isPresent()) {
            return ProcessHandle.of(pid.get()).isPresent();
        }
        return false;
    }

    public boolean writePid() {
        if (checkPid()) {
            Iterable<String> pid = ImmutableList.of(Long.toString(getCurrentProcessPid()));
            try {
                Files.write(getPidFile().toPath(), pid, StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("Could not write pid file: " + getPidFile().getAbsolutePath());
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public Optional<Long> readPid() {
        try {
            File pidFile = getPidFile();
            if (pidFile.exists() && pidFile.isFile()) {
                List<String> allLines = Files.readAllLines(pidFile.toPath());
                if (allLines.size() > 0) {
                    try {
                        Long pid = Long.parseLong(allLines.get(0));
                        return Optional.of(pid);
                    } catch (NumberFormatException nf) {
                        throw new RuntimeException("Could not parse pid file: " + pidFile.getAbsolutePath());
                    }
                }
            }
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addShutdownHandler(Runnable shutdownHook) {
        final Thread shutdownThread = new Thread(() -> {
            log.info("\nShutdown hook triggered, quitting...");
            shutdownHook.run();
            log.info("### JUDO finishing!");
            log.info("Goodbye! 👋🏼");
            File pidFile = getPidFile();
            if (pidFile.exists()) {
                pidFile.delete();
            }
        });
        shutdownThread.setName("JUDO Shutdown Hook thread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
}
