package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb;

import lombok.*;

import java.io.File;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class JudoHsqldbModuleConfiguration {
    public static final JudoHsqldbModuleConfiguration DEFAULT = JudoHsqldbModuleConfiguration.builder().build();
    @Builder.Default
    private Boolean runServer = false;
    @Builder.Default
    private String databaseName = "judo";
    @Builder.Default
    private File databasePath = new File(".", "judo.db");
    @Builder.Default
    private Integer port = 31001;
}
