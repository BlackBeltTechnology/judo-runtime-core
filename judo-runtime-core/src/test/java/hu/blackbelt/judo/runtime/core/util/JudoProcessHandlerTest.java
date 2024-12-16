package hu.blackbelt.judo.runtime.core.util;

import hu.blackbelt.judo.runtime.core.utils.JudoProcessHandler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JudoProcessHandlerTest {

    @Test
    void testPid() {
        JudoProcessHandler judoProcessHandler = JudoProcessHandler.builder()
                .build();
        judoProcessHandler.addShutdownHandler(() -> {});

        assertTrue(judoProcessHandler.checkPid());
        assertFalse(judoProcessHandler.isRunning());
        judoProcessHandler.writePid();
        assertFalse(judoProcessHandler.checkPid());
        assertTrue(judoProcessHandler.isRunning());
    }
}