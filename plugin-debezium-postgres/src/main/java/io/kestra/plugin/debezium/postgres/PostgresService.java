package io.kestra.plugin.debezium.postgres;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.util.Locale;
import java.util.Properties;

public abstract class PostgresService {
    public static void handleProperties(Properties properties, RunContext runContext, PostgresInterface postgres) throws IllegalVariableEvaluationException, IOException, OperatorCreationException, PKCSException {
        properties.put("database.dbname", runContext.render(postgres.getDatabase()).as(String.class).orElseThrow());
        properties.put("plugin.name", runContext.render(postgres.getPluginName()).as(PostgresInterface.PluginName.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        properties.put("snapshot.mode", runContext.render(postgres.getSnapshotMode()).as(PostgresInterface.SnapshotMode.class).orElseThrow().name().toLowerCase(Locale.ROOT));
        properties.put("slot.name", runContext.render(postgres.getSlotName()).as(String.class).orElseThrow());

        if (postgres.getPublicationName() != null) {
            properties.put("publication.name", runContext.render(postgres.getPublicationName()).as(String.class).orElseThrow());
        }

        if (postgres.getSslMode() != null) {
            properties.put("database.sslmode", runContext.render(postgres.getSslMode()).as(PostgresInterface.SslMode.class).orElseThrow().name().toUpperCase(Locale.ROOT).replace("_", "-"));
        }

        if (postgres.getSslRootCert() != null) {
            properties.put("database.sslrootcert", runContext.workingDir().createTempFile(runContext.render(postgres.getSslRootCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (postgres.getSslCert() != null) {
            properties.put("database.sslcert", runContext.workingDir().createTempFile(runContext.render(postgres.getSslCert()).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8), ".pem").toAbsolutePath().toString());
        }

        if (postgres.getSslKey() != null) {
            properties.put("database.sslkey", convertPrivateKey(runContext,
                runContext.render(postgres.getSslKey()).as(String.class).orElseThrow(),
                runContext.render(postgres.getSslKeyPassword()).as(String.class).orElseThrow()));
        }

        if (postgres.getSslKeyPassword() != null) {
            properties.put("database.sslpassword", runContext.render(postgres.getSslKeyPassword()).as(String.class).orElseThrow());
        }
    }

    private static Object readPem(RunContext runContext, String vars) throws IllegalVariableEvaluationException, IOException {
        try (
            StringReader reader = new StringReader(runContext.render(vars));
            PEMParser pemParser = new PEMParser(reader)
        ) {
            return pemParser.readObject();
        }
    }

    private static synchronized void addProvider() {
        Provider bc = Security.getProvider("BC");
        if (bc == null) {
            BouncyGPG.registerProvider();
        }
    }

    private static String convertPrivateKey(RunContext runContext, String vars, String password) throws IOException, IllegalVariableEvaluationException, OperatorCreationException, PKCSException {
        PostgresService.addProvider();

        Object pemObject = readPem(runContext, vars);

        PrivateKeyInfo keyInfo;
        if (pemObject instanceof PEMEncryptedKeyPair) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            PEMDecryptorProvider decrypter = new JcePEMDecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            PEMKeyPair decryptedKeyPair = ((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decrypter);
            keyInfo = decryptedKeyPair.getPrivateKeyInfo();
        } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
            if (password == null) {
                throw new IOException("Unable to import private key. Key is encrypted, but no password was provided.");
            }

            InputDecryptorProvider inputDecryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                .setProvider("BC")
                .build(password.toCharArray());

            keyInfo = ((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(inputDecryptorProvider);
        } else {
            keyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
        }

        PrivateKey privateKey = new JcaPEMKeyConverter().getPrivateKey(keyInfo);

        return runContext.workingDir().createTempFile(privateKey.getEncoded(), ".der").toAbsolutePath().toString();
    }
}
