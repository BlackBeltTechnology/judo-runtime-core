package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JNG-6421: sharing the model runtime across classes is opt-in. The default MUST stay off so
 * the "fresh runtime by default" contract ({@link DefaultIsFreshRuntimePerMethodTest}) holds.
 */
@DisplayName("judo.testkit.shareModel: opt-in, default off")
class ShareModelPropertyTest {

    @AfterEach
    void clearProperty() {
        System.clearProperty(JudoTestExtension.SHARE_MODEL_PROPERTY);
    }

    @Test
    void propertyNameIsStable() {
        assertEquals("judo.testkit.shareModel", JudoTestExtension.SHARE_MODEL_PROPERTY);
    }

    @Test
    void offWhenUnset() {
        System.clearProperty(JudoTestExtension.SHARE_MODEL_PROPERTY);
        assertFalse(JudoTestExtension.shareModel());
    }

    @Test
    void offWhenFalse() {
        System.setProperty(JudoTestExtension.SHARE_MODEL_PROPERTY, "false");
        assertFalse(JudoTestExtension.shareModel());
    }

    @Test
    void onWhenTrue() {
        System.setProperty(JudoTestExtension.SHARE_MODEL_PROPERTY, "true");
        assertTrue(JudoTestExtension.shareModel());
    }
}
