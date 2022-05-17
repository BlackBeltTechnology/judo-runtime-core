package hu.blackbelt.judo.runtime.core.bootstrap;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.asm2script.Asm2Script;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import java.net.URI;
import java.net.URISyntaxException;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.LoadArguments.asmLoadArgumentsBuilder;
import static hu.blackbelt.judo.meta.script.runtime.ScriptModel.buildScriptModel;

@Builder
@Getter
public class JudoModelHolder {

    @NonNull
    AsmModel asmModel;

    @NonNull
    RdbmsModel rdbmsModel;

    @NonNull
    MeasureModel measureModel;

    @NonNull
    ExpressionModel expressionModel;

    @NonNull
    ScriptModel scriptModel;

    @NonNull
    LiquibaseModel liquibaseModel;

    @NonNull
    Asm2RdbmsTransformationTrace asm2rdbms;


    public static JudoModelHolder loadFromURL(String modelName, URI uri, Dialect dialect) throws Exception {
        return loadFromURL(modelName, uri, dialect, true);
    }

    public static JudoModelHolder loadFromURL(String modelName, URI uri, Dialect dialect, boolean validate) throws Exception {

        if (modelName == null) {
            throw new IllegalArgumentException("Model name have to be defined");
        }
        if (uri == null) {
            throw new IllegalArgumentException("URI name have to be defined");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("Dialect name have to be defined");
        }

        AsmModel asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-asm.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelName + "-asm.model"))
                .validateModel(validate)
                .name(modelName));

        String modelNameFromAsm;

        if (asmModel.getResourceSet().getResources().get(0).getAllContents().hasNext()) {
            EObject o = asmModel.getResourceSet().getResources().get(0).getAllContents().next();
            if (o instanceof EPackage) {
                EPackage ePackage = (EPackage) o;
                modelNameFromAsm = ePackage.getName();
            } else {
                throw new IllegalStateException("Illegal ASM model, root package must be EPackage");
            }
        } else {
            throw new IllegalStateException("Illegal ASM model, empty model");
        }

        if (!modelNameFromAsm.equals(modelName)) {
            asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                    .inputStream(calculateRelativeURI(uri, "/" + modelName + "-asm.model").toURL().openStream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm + "-asm.model"))
                    .validateModel(validate)
                    .name(modelNameFromAsm));
        }

        RdbmsModel rdbmsModel = RdbmsModel.loadRdbmsModel(RdbmsModel.LoadArguments.rdbmsLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-rdbms_" + dialect.getName() +".model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-rdbms_" + dialect.getName() + ".model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        MeasureModel measureModel = MeasureModel.loadMeasureModel(MeasureModel.LoadArguments.measureLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-measure.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-measure.model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        ExpressionModel expressionModel = ExpressionModel.loadExpressionModel(ExpressionModel.LoadArguments.expressionLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-expression.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-expression.model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        /*
        ScriptModel scriptModel = ScriptModel.loadScriptModel(ScriptModel.LoadArguments.scriptLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-script.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI("urn:asmjclextractor.judo-meta-expression"))
                .validateModel(validate)
                .name(modelNameFromAsm));
        */
        ScriptModel scriptModel = buildScriptModel()
                .name(modelNameFromAsm)
                .build();

        Asm2Script.executeAsm2Script(
                Asm2Script.Asm2ScriptParameter.asm2ScriptParameter()
                        .asmModel(asmModel)
                        .measureModel(measureModel)
                        .scriptModel(scriptModel)
        );

        LiquibaseModel liquibaseModel = LiquibaseModel.loadLiquibaseModel(LiquibaseModel.LoadArguments.liquibaseLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-liquibase_" + dialect.getName() + ".changelog.xml").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-liquibase_" + dialect.getName() + ".changelog.xml"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        Asm2RdbmsTransformationTrace asm2rdbms  = Asm2RdbmsTransformationTrace.fromModelsAndTrace(modelName,
                asmModel, rdbmsModel,
                calculateRelativeURI(uri, "/" + modelName + "-asm2rdbms_" + dialect.getName() + ".model").toURL().openStream());


        return JudoModelHolder.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .liquibaseModel(liquibaseModel)
                .asm2rdbms(asm2rdbms)
                .scriptModel(scriptModel)
                .build();
    }


    @SneakyThrows(URISyntaxException.class)
    private static URI calculateRelativeURI(URI base, String path) {
        //URI root = JudoModelHolder.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        URI root = base;
        if (root.toString().endsWith(".jar")) {
            root = new URI("jar:" + root.toString() + "!/" + path);
        } else if (root.toString().startsWith("jar:bundle:")) {
            root = new URI(root.toString().substring(4, root.toString().indexOf("!")) + path);
        } else {
            root = new URI(root.toString() + "/" + path);
        }
        return root;
    }
}
