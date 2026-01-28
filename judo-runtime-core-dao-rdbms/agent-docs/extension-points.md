# DAO RDBMS Extension Points

## Dialect Interface

**Purpose**: Abstract database-specific behavior

**Location**: `hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect`

```java
public interface Dialect {
    String getName();
    String getDualTable();
}
```

**Registration**:
```java
bind(Dialect.class).to(PostgreSQLDialect.class);
```

---

## RdbmsParameterMapper Interface

**Purpose**: Map Java objects to SQL parameters

**Location**: `hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper`

```java
public interface RdbmsParameterMapper {
    Parameter createParameter(Object value, String targetDbType);
    void mapAttributeParameters(EAttribute attribute, Object value, 
        BiConsumer<String, Parameter> consumer);
    void mapReferenceParameters(EReference reference, Object value, 
        BiConsumer<String, Parameter> consumer);
    int getSqlType(String targetDbType);
}
```

**Default Implementation**: `DefaultRdbmsParameterMapper`

---

## MapperFactory Interface

**Purpose**: Create query element mappers

**Location**: `hu.blackbelt.judo.runtime.core.dao.rdbms.query.MapperFactory`

```java
public interface MapperFactory {
    Map<Class<?>, RdbmsMapper<?>> getMappers(RdbmsBuilder rdbmsBuilder);
}
```

**Default Implementation**: `DefaultMapperFactory`

**Built-in Mappers**:
- `AttributeMapper` - Attribute selectors
- `FunctionMapper` - Function calls
- `VariableMapper` - Variables
- `FilterMapper` - Filters

---

## RdbmsMapper Abstract Class

**Purpose**: Base class for query element translation

```java
public abstract class RdbmsMapper<T> {
    protected final RdbmsBuilder rdbmsBuilder;
    
    public abstract RdbmsField map(T element, RdbmsBuilderContext context);
}
```

---

## RdbmsInit Interface

**Purpose**: Database initialization hook

**Location**: `hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit`

```java
public interface RdbmsInit {
    void execute(DataSource dataSource);
}
```

**Registration**:
```java
bind(RdbmsInit.class).to(CustomRdbmsInit.class);
```

---

## Translator Class

**Purpose**: Expression translation registry

```java
public class Translator {
    public <T extends Expression> void register(
        Class<T> clazz, 
        Function<T, Expression> translator);
    
    public Expression translate(Expression expression);
}
```

**Usage**:
```java
translator.register(CustomExpression.class, expr -> {
    return new StringConstant(expr.evaluate());
});
```
