# JUDO Runtime Core - n8n Integration Plan

This document explores integration patterns between JUDO-generated applications and n8n workflow automation.

## Current JUDO Architecture

```mermaid
flowchart TB
    subgraph JUDO["JUDO Application"]
        ASM["ASM Model<br/>(Entities, Operations)"]
        DISP["Dispatcher<br/>(Routes ops to handlers)"]
        REST["JAX-RS/CXF<br/>(REST API)"]
        
        ASM --> DISP --> REST
        
        subgraph Handlers
            SDK["SDK Ops<br/>(Java)"]
            SCRIPT["Scripts<br/>(Model)"]
            BEHAV["Behaviour<br/>(CRUD)"]
        end
        
        DISP --> SDK
        DISP --> SCRIPT
        DISP --> BEHAV
        
        INT["OperationCallInterceptor<br/>HOOK POINT!<br/>- preCall()<br/>- postCall()<br/>- async mode"]
        
        SDK --> INT
        SCRIPT --> INT
        BEHAV --> INT
        REST -.->|"can trigger"| INT
        
        DAO["DAO<br/>(Database)"]
        INT --> DAO
    end
```

## Key Integration Point: OperationCallInterceptor

The `OperationCallInterceptor` interface (`judo-runtime-core-dispatcher`) provides hooks for integration:

| Method | Purpose |
|--------|---------|
| `getName()` | Unique interceptor identifier |
| `getOperations(AsmModel)` | Filter which operations to intercept (empty = all) |
| `async()` | If true, runs on separate thread (non-blocking) |
| `terminateOnException()` | Whether errors stop execution |
| `ignoreDecoratedCall()` | Skip the original operation entirely |
| `preCall(operation, input)` | Transform input before operation |
| `postCall(operation, input, result)` | React after operation completes |

---

## Integration Patterns

### Pattern 1: JUDO → n8n (Outbound Events)

JUDO operations trigger n8n workflows via webhooks.

```mermaid
sequenceDiagram
    participant User
    participant JUDO as JUDO Application
    participant Interceptor
    participant DAO
    participant n8n
    
    User->>JUDO: Create Order
    JUDO->>Interceptor: preCall()
    Interceptor->>DAO: Execute operation
    DAO-->>Interceptor: Result
    Interceptor->>Interceptor: postCall()
    Interceptor-->>JUDO: Return result
    JUDO-->>User: Response
    
    Note over Interceptor,n8n: Async (separate thread)
    Interceptor--)n8n: HTTP POST webhook
    n8n->>n8n: Execute workflow
    n8n->>n8n: Send Email, Slack, etc.
```

#### Example Implementation

```java
public class N8nWebhookInterceptor implements OperationCallInterceptor {
    
    private final HttpClient httpClient;
    private final String n8nWebhookUrl;
    
    @Override
    public String getName() { return "n8n-webhook"; }
    
    @Override
    public boolean async() { return true; }  // Non-blocking!
    
    @Override
    public Collection<EOperation> getOperations(AsmModel asmModel) {
        // Filter to specific operations, e.g., only "Order" entity creates
        return asmModel.getOperations().stream()
            .filter(op -> op.getName().startsWith("create") && 
                         op.getEContainingClass().getName().equals("OrderService"))
            .collect(toList());
    }
    
    @Override
    public Object postCall(EOperation operation, Object input, Object result) {
        // Fire webhook to n8n after successful operation
        httpClient.post(n8nWebhookUrl, Map.of(
            "event", operation.getName(),
            "entity", operation.getEContainingClass().getName(),
            "payload", result
        ));
        return result;
    }
}
```

### Pattern 2: n8n → JUDO (Inbound Calls)

n8n workflows call JUDO REST APIs.

```mermaid
sequenceDiagram
    participant Trigger as n8n Trigger<br/>(Timer/Webhook)
    participant Workflow as n8n Workflow
    participant HTTP as HTTP Request Node
    participant JUDO as JUDO REST API
    participant Dispatcher
    participant DAO
    
    Trigger->>Workflow: Start workflow
    Workflow->>HTTP: Execute HTTP call
    HTTP->>JUDO: REST API call
    JUDO->>Dispatcher: Route operation
    Dispatcher->>DAO: Execute
    DAO-->>Dispatcher: Result
    Dispatcher-->>JUDO: Response
    JUDO-->>HTTP: JSON response
    HTTP-->>Workflow: Continue flow
```

This pattern works out of the box - JUDO already exposes REST APIs via JAX-RS/CXF. n8n's HTTP Request node can call any operation defined in your model.

---

## Alternative: Message Queue Architecture

For guaranteed delivery and reliability, consider adding a message queue:

```mermaid
flowchart LR
    subgraph JUDO
        INT["Interceptor<br/>postCall()"]
    end
    
    subgraph Queue["Message Queue"]
        MQ["RabbitMQ /<br/>Redis /<br/>Kafka"]
    end
    
    subgraph n8n
        TRIG["Queue Trigger<br/>or Polling"]
    end
    
    INT -->|"publish event"| MQ
    MQ -->|"consume event"| TRIG
```

**Benefits:**
- Guaranteed delivery with retries
- Buffering if n8n is slow or down
- Decoupled scaling
- Event replay capability

---

## Implementation Considerations

### Decision Flow

```mermaid
flowchart TD
    A[Integration Need] --> B{Direction?}
    
    B -->|JUDO → n8n| C{Reliability?}
    B -->|n8n → JUDO| D[Use REST API]
    B -->|Both| E[Implement both patterns]
    
    C -->|Fire & forget OK| F[Async Interceptor + Webhook]
    C -->|Guaranteed delivery| G[Message Queue]
    
    D --> H[Configure n8n HTTP Request node]
    
    F --> I[Implement N8nWebhookInterceptor]
    G --> J[Add RabbitMQ/Kafka]
```

### Questions to Answer

1. **Direction of integration?**
   - JUDO → n8n (events trigger workflows)
   - n8n → JUDO (workflows call JUDO APIs)
   - Bidirectional

2. **Workflow types?**
   - Notifications (email, Slack) on entity changes
   - Data sync with external systems (CRM, ERP)
   - Complex approval workflows
   - Scheduled data processing

3. **Reliability requirements?**
   - Fire-and-forget acceptable?
   - Need guaranteed delivery?
   - Should events queue if n8n is down?

4. **Volume expectations?**
   - Events per second/minute
   - Affects whether async interceptors suffice or need message queue

### Security Considerations

- n8n webhook authentication (API keys, JWT)
- JUDO API authentication for inbound calls (Keycloak integration)
- Network security (VPN, firewall rules)
- Payload encryption for sensitive data

### Observability

- Log all webhook calls (success/failure)
- Metrics for latency and error rates
- Dead letter queue for failed events
- Correlation IDs for tracing across systems

---

## Next Steps

1. **Decide on integration direction** and primary use cases
2. **Prototype** a simple interceptor that posts to n8n webhook
3. **Evaluate reliability needs** - webhook vs message queue
4. **Design event schema** - consistent payload structure
5. **Implement authentication** - secure the integration
6. **Add monitoring** - ensure visibility into integration health

---

## Related Files

- `judo-runtime-core-dispatcher/src/main/java/.../OperationCallInterceptor.java` - Main hook interface
- `judo-runtime-core-dispatcher/src/main/java/.../OperationCallInterceptorProvider.java` - Interceptor registration
- `judo-runtime-core-dispatcher/src/main/java/.../DefaultDispatcher.java` - Dispatcher that invokes interceptors
