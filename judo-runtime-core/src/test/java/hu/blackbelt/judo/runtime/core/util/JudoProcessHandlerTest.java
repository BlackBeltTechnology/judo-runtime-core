package hu.blackbelt.judo.runtime.core.util;

import hu.blackbelt.judo.runtime.core.utils.JudoProcessHandler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JudoProcessHandlerTest {

    @Test
    void testVariableHandling() {
        JudoProcessHandler judoProcessHandler = JudoProcessHandler.builder()
                .build();
        judoProcessHandler.addShutdownHandler(() -> {});

        assertTrue(judoProcessHandler.checkPid());
        judoProcessHandler.writePid();
        assertFalse(judoProcessHandler.checkPid());
    }
}