package com.example.interceptors;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.InterceptorCallBusinessException;
import org.eclipse.emf.ecore.EOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Example interceptor that fires webhooks to external systems (e.g., n8n)
 * after successful operations.
 * 
 * Features:
 * - Async execution (non-blocking)
 * - Configurable webhook URL
 * - Operation filtering
 * - JSON payload serialization
 * - Error handling with logging
 */
@Singleton
public class WebhookInterceptor implements OperationCallInterceptor {
    
    private static final Logger log = LoggerFactory.getLogger(WebhookInterceptor.class);
    
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String webhookUrl;
    private final String apiKey;
    
    @Inject
    public WebhookInterceptor(
            HttpClient httpClient,
            ObjectMapper objectMapper,
            @WebhookUrl String webhookUrl,
            @WebhookApiKey String apiKey) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.webhookUrl = webhookUrl;
        this.apiKey = apiKey;
    }
    
    /**
     * Unique name for this interceptor.
     * Used for logging and identification.
     */
    @Override
    public String getName() {
        return "webhook-interceptor";
    }
    
    /**
     * Run asynchronously on a separate thread.
     * This ensures the main operation is not blocked waiting for the webhook.
     * Note: Async interceptors run outside the transaction context.
     */
    @Override
    public boolean async() {
        return true;
    }
    
    /**
     * Don't terminate the main operation if webhook fails.
     * The operation should succeed even if the webhook delivery fails.
     */
    @Override
    public boolean terminateOnException() {
        return false;
    }
    
    /**
     * Filter which operations trigger webhooks.
     * In this example: only create/update operations on Order entities.
     */
    @Override
    public Collection<EOperation> getOperations(AsmModel asmModel) {
        return asmModel.getResourceModel().getContents().stream()
            .filter(org.eclipse.emf.ecore.EClass.class::isInstance)
            .map(org.eclipse.emf.ecore.EClass.class::cast)
            .filter(c -> c.getName().equals("OrderService"))
            .flatMap(c -> c.getEOperations().stream())
            .filter(op -> op.getName().startsWith("create") || 
                         op.getName().startsWith("update"))
            .collect(Collectors.toList());
    }
    
    /**
     * Called after the operation completes successfully.
     * Fires the webhook with operation details and result.
     */
    @Override
    public Object postCall(EOperation operation, Object parameterPayload, Object returnPayload) 
            throws InterceptorCallBusinessException {
        
        try {
            WebhookEvent event = createEvent(operation, parameterPayload, returnPayload);
            sendWebhook(event);
        } catch (Exception e) {
            // Log but don't fail - terminateOnException is false
            log.error("Failed to send webhook for operation {}: {}", 
                operation.getName(), e.getMessage(), e);
        }
        
        // Always return the original result unchanged
        return returnPayload;
    }
    
    private WebhookEvent createEvent(EOperation operation, Object input, Object result) {
        return new WebhookEvent(
            operation.getName(),
            operation.getEContainingClass().getName(),
            Instant.now().toString(),
            extractId(result),
            input,
            result
        );
    }
    
    private String extractId(Object result) {
        if (result instanceof Payload) {
            Object id = ((Payload) result).get("__identifier");
            return id != null ? id.toString() : null;
        }
        return null;
    }
    
    private void sendWebhook(WebhookEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(webhookUrl))
            .header("Content-Type", "application/json")
            .header("X-API-Key", apiKey)
            .header("X-Event-Type", event.operation())
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
        
        HttpResponse<String> response = httpClient.send(
            request, 
            HttpResponse.BodyHandlers.ofString()
        );
        
        if (response.statusCode() >= 400) {
            log.warn("Webhook returned error status {}: {}", 
                response.statusCode(), response.body());
        } else {
            log.debug("Webhook sent successfully for {}", event.operation());
        }
    }
    
    /**
     * Event payload sent to the webhook endpoint.
     */
    public record WebhookEvent(
        String operation,
        String entity,
        String timestamp,
        String entityId,
        Object input,
        Object result
    ) {}
    
    // Qualifier annotations for dependency injection
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @javax.inject.Qualifier
    public @interface WebhookUrl {}
    
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @javax.inject.Qualifier
    public @interface WebhookApiKey {}
}

// =============================================================================
// Guice Module for Registration
// =============================================================================

/*
package com.example.modules;

import com.example.interceptors.WebhookInterceptor;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;

import java.net.http.HttpClient;
import java.time.Duration;

public class WebhookModule extends AbstractModule {
    
    private final String webhookUrl;
    private final String apiKey;
    
    public WebhookModule(String webhookUrl, String apiKey) {
        this.webhookUrl = webhookUrl;
        this.apiKey = apiKey;
    }
    
    @Override
    protected void configure() {
        // Register the interceptor
        Multibinder<OperationCallInterceptor> interceptors = 
            Multibinder.newSetBinder(binder(), OperationCallInterceptor.class);
        interceptors.addBinding().to(WebhookInterceptor.class);
        
        // Bind configuration
        bindConstant()
            .annotatedWith(WebhookInterceptor.WebhookUrl.class)
            .to(webhookUrl);
        bindConstant()
            .annotatedWith(WebhookInterceptor.WebhookApiKey.class)
            .to(apiKey);
    }
    
    @Provides
    @Singleton
    HttpClient provideHttpClient() {
        return HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }
}
*/

// =============================================================================
// Spring Configuration Alternative
// =============================================================================

/*
package com.example.config;

import com.example.interceptors.WebhookInterceptor;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
public class WebhookConfig {
    
    @Value("${webhook.url}")
    private String webhookUrl;
    
    @Value("${webhook.api-key}")
    private String apiKey;
    
    @Bean
    public HttpClient httpClient() {
        return HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }
    
    @Bean
    public OperationCallInterceptor webhookInterceptor(
            HttpClient httpClient, 
            ObjectMapper objectMapper) {
        return new WebhookInterceptor(httpClient, objectMapper, webhookUrl, apiKey);
    }
}
*/

// =============================================================================
// Test Example
// =============================================================================

/*
package com.example.interceptors;

import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@JudoTest
class WebhookInterceptorTest {
    
    @Test
    void shouldSendWebhookOnCreate() throws Exception {
        // Mock HTTP client
        HttpClient mockClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockClient.send(any(HttpRequest.class), any()))
            .thenReturn(mockResponse);
        
        // Create interceptor with mock
        WebhookInterceptor interceptor = new WebhookInterceptor(
            mockClient,
            new ObjectMapper(),
            "https://n8n.example.com/webhook/test",
            "test-api-key"
        );
        
        // Simulate postCall
        EOperation mockOp = createMockOperation("createOrder", "OrderService");
        Payload result = Payload.map("__identifier", "order-123", "total", 99.99);
        
        Object returned = interceptor.postCall(mockOp, Payload.empty(), result);
        
        // Verify webhook was called
        verify(mockClient).send(any(HttpRequest.class), any());
        
        // Verify result unchanged
        assertThat(returned).isSameAs(result);
    }
}
*/
