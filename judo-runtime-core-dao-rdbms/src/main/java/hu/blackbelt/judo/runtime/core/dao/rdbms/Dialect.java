package hu.blackbelt.judo.runtime.core.dao.rdbms;

public interface Dialect {

    /*
    POSTGRESQL("postgresql") {
        @Override
        public String getDualTable() {
            return null;
        }
    },
    HSQLDB("hsqldb") {
        @Override
        public String getDualTable() {
            return "\"INFORMATION_SCHEMA\".\"SYSTEM_USERS\"";
        }
    },
    JOOQ("jooq") {
        @Override
        public String getDualTable() {
            return null;
        }
    };

    @Getter
    private String name;

    public static Dialect parse(final String name, final boolean jooqEnabled) {
        return jooqEnabled ? JOOQ : Arrays.asList(Dialect.values()).stream()
                .filter(d -> Objects.equals(name, d.getName()))
                .findFirst()
                .orElse(null);
    }
    */

    String getName();

    String getDualTable();

}
