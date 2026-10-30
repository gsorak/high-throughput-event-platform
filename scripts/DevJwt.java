import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;

/**
 * Local development token issuer (ADR-0013). JDK only, run as a single-file program from the repository root:
 *
 * <pre>
 *   java scripts/DevJwt.java keys                       # once: writes .local/jwt/private.pem and public.pem
 *   java scripts/DevJwt.java token &lt;tenant&gt; [minutes]   # prints an RS256 token, valid 60 minutes by default
 *   java scripts/DevJwt.java tokens &lt;tenant&gt;...         # one 60-minute token per tenant, one per line (load tests)
 * </pre>
 *
 * The services verify tokens with {@code .local/jwt/public.pem} and require the issuer and audience below. Not for
 * production: the private key is a plain file and tokens cannot be revoked.
 */
public class DevJwt {

    private static final Path DIRECTORY = Path.of(".local", "jwt");
    private static final Path PRIVATE_KEY = DIRECTORY.resolve("private.pem");
    private static final Path PUBLIC_KEY = DIRECTORY.resolve("public.pem");
    private static final String ISSUER = "event-platform-dev";
    private static final String AUDIENCE = "event-platform";
    /** Same rule as TenantIds in event-contract. */
    private static final String TENANT_PATTERN = "^[a-z0-9][a-z0-9-]{1,62}$";

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("keys")) {
            writeKeys();
        } else if ((args.length == 2 || args.length == 3) && args[0].equals("token")) {
            System.out.println(token(args[1], args.length == 3 ? Long.parseLong(args[2]) : 60));
        } else if (args.length >= 2 && args[0].equals("tokens")) {
            for (int i = 1; i < args.length; i++) {
                System.out.println(token(args[i], 60));
            }
        } else {
            System.err.println("usage: java scripts/DevJwt.java keys | token <tenant> [minutes] | tokens <tenant>...");
            System.exit(2);
        }
    }

    private static void writeKeys() throws Exception {
        if (Files.exists(PRIVATE_KEY)) {
            System.out.println("Keeping the existing keys in " + DIRECTORY);
            return;
        }
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        Files.createDirectories(DIRECTORY);
        Files.writeString(PRIVATE_KEY, pem("PRIVATE KEY", keys.getPrivate().getEncoded()));
        Files.setPosixFilePermissions(PRIVATE_KEY, PosixFilePermissions.fromString("rw-------"));
        Files.writeString(PUBLIC_KEY, pem("PUBLIC KEY", keys.getPublic().getEncoded()));
        System.out.println("Wrote " + PRIVATE_KEY + " and " + PUBLIC_KEY);
    }

    private static String token(String tenant, long minutes) throws Exception {
        // The pattern also guarantees that the tenant needs no escaping in the JSON below.
        if (!tenant.matches(TENANT_PATTERN)) {
            throw new IllegalArgumentException("tenant must match " + TENANT_PATTERN);
        }
        long now = Instant.now().getEpochSecond();
        String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String claims = "{\"iss\":\"%s\",\"aud\":\"%s\",\"tenant_id\":\"%s\",\"iat\":%d,\"exp\":%d}"
                .formatted(ISSUER, AUDIENCE, tenant, now, now + minutes * 60);
        String signingInput = base64Url(header.getBytes(StandardCharsets.UTF_8)) + "."
                + base64Url(claims.getBytes(StandardCharsets.UTF_8));

        Signature rs256 = Signature.getInstance("SHA256withRSA");
        rs256.initSign(readPrivateKey());
        rs256.update(signingInput.getBytes(StandardCharsets.US_ASCII));
        return signingInput + "." + base64Url(rs256.sign());
    }

    private static PrivateKey readPrivateKey() throws Exception {
        if (!Files.exists(PRIVATE_KEY)) {
            throw new IllegalStateException(PRIVATE_KEY + " not found; run: java scripts/DevJwt.java keys");
        }
        String base64 = Files.readString(PRIVATE_KEY).replaceAll("-----[A-Z ]+-----|\\s", "");
        return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }

    private static String base64Url(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
