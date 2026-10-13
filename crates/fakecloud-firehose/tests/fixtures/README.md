# Xerial Snappy interoperability fixtures

These streams were generated independently with
`org.xerial.snappy:snappy-java:1.1.10.8`. Inline Rust tests and SDK E2E tests
validate a separate test decoder against them before decoding emulator output.
Normal Cargo tests consume the checked-in files and need no Java installation.
The emulator uses the existing pure-Rust `snap` crate for compression.

## Inputs and checksums

| File | Uncompressed input | SHA-256 |
| --- | --- | --- |
| `xerial_binary.snappy` | Bytes 0–255 repeated four times (1,024 bytes) | `5a41a7bad5abe7d4eae5d76cf5d4bbb245b21196cf70a0d320edb29574028ed4` |
| `xerial_multiblock.snappy` | 70,000 bytes; byte `i` is `(i * 31 + 7) % 256` | `0b9ca3284023b3eec6b4da8d827781872420f12e108b1c47437973e6000ae593` |

The pinned Maven Central JAR has SHA-256
`50485d06037fea3d6e40c968386feeca6338cc9872e25549593ff3eb352cefcc`.

## Reproduce

Run from the repository root with a JDK, `curl`, and `sha256sum` available:

```sh
work=$(mktemp -d)
fixtures=crates/fakecloud-firehose/tests/fixtures
curl --fail --location --output "$work/snappy.jar" \
  https://repo.maven.apache.org/maven2/org/xerial/snappy/snappy-java/1.1.10.8/snappy-java-1.1.10.8.jar
sha256sum "$work/snappy.jar"
# Compare the JAR checksum with the pinned value above before running Java.
javac -cp "$work/snappy.jar" -d "$work" "$fixtures/GenerateXerialFixtures.java"
java -cp "$work:$work/snappy.jar" GenerateXerialFixtures generate "$fixtures"
sha256sum "$fixtures/"*.snappy
```

On a mixed glibc/musl Linux x86-64 host, native-library autodetection may select
the musl variant even when the JVM uses glibc. If that happens, extract the
JAR's bundled glibc library and explicitly select it for fixture generation:

```sh
unzip -p "$work/snappy.jar" \
  org/xerial/snappy/native/Linux/x86_64/libsnappyjava.so > "$work/libsnappyjava.so"
java -Dorg.xerial.snappy.lib.path="$work" -cp "$work:$work/snappy.jar" \
  GenerateXerialFixtures generate "$fixtures"
```

This selection was used to generate the checked-in files. It affects only the
one-time Java fixture tool, not Rust builds or emulator execution.

The generator also supports `verify`: pass a compressed file and the original
binary input file to check decoding with Java's `SnappyInputStream`. Rust
production-encoder output for binary, multi-block, and deterministic
incompressible input was checked this way during implementation.

## Framing contract

The stream begins with the 16-byte Xerial header:

```text
82 53 4e 41 50 50 59 00 00 00 00 01 00 00 00 01
```

Each block is a big-endian unsigned 32-bit compressed byte length followed by
one raw Snappy block. The default uncompressed block size is 32,768 bytes;
record boundaries do not reset that window. There is no separate framed-stream
identifier, CRC field, or end marker. The test decoder bounds expanded blocks
to this default size before allocating their buffers.

References for the pinned implementation:

- [SnappyOutputStream framing and default block size](https://github.com/xerial/snappy-java/blob/v1.1.10.8/src/main/java/org/xerial/snappy/SnappyOutputStream.java)
- [SnappyCodec header](https://github.com/xerial/snappy-java/blob/v1.1.10.8/src/main/java/org/xerial/snappy/SnappyCodec.java)
- [Rust raw encoder API](https://docs.rs/snap/1.1.2/snap/raw/struct.Encoder.html)
