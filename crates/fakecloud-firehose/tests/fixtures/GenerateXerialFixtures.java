import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/** Fixture generator and independent verifier for snappy-java 1.1.10.8. */
public final class GenerateXerialFixtures {
    private static byte[] binaryInput() {
        byte[] input = new byte[1024];
        for (int i = 0; i < input.length; i++) {
            input[i] = (byte) i;
        }
        return input;
    }

    private static byte[] multiblockInput() {
        byte[] input = new byte[70000];
        for (int i = 0; i < input.length; i++) {
            input[i] = (byte) ((i * 31 + 7) % 256);
        }
        return input;
    }

    private static void write(Path path, byte[] input) throws Exception {
        try (SnappyOutputStream output = new SnappyOutputStream(Files.newOutputStream(path))) {
            output.write(input);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("generate")) {
            Path directory = Path.of(args[1]);
            Files.createDirectories(directory);
            write(directory.resolve("xerial_binary.snappy"), binaryInput());
            write(directory.resolve("xerial_multiblock.snappy"), multiblockInput());
        } else if (args.length == 3 && args[0].equals("verify")) {
            byte[] expected = Files.readAllBytes(Path.of(args[2]));
            try (InputStream input = new SnappyInputStream(Files.newInputStream(Path.of(args[1])))) {
                if (!Arrays.equals(input.readAllBytes(), expected)) {
                    throw new AssertionError("Decoded Snappy bytes differ from the supplied input");
                }
            }
            System.out.println("Verified Xerial decoding: " + args[1]);
        } else {
            throw new IllegalArgumentException("generate <directory> | verify <compressed> <original>");
        }
    }
}
