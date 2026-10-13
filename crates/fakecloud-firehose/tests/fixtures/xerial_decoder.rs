// Test-only Xerial decoder, shared by inline unit tests and SDK E2E tests.
// Derived from snappy-java's stream specification, not the production encoder.

pub fn decode(input: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if !input.starts_with(b"\x82SNAPPY\0\0\0\0\x01\0\0\0\x01") {
        return Err("invalid Xerial version-1 header".to_owned());
    }
    let mut remaining = &input[16..];
    let mut blocks = Vec::new();
    while !remaining.is_empty() {
        let length = remaining.get(..4).ok_or("truncated block length")?;
        let length = u32::from_be_bytes(length.try_into().unwrap()) as usize;
        if length == 0 {
            return Err("empty compressed block".to_owned());
        }
        let body = remaining
            .get(4..)
            .and_then(|body| body.get(..length))
            .ok_or("truncated compressed block")?;
        let decoded_length = snap::raw::decompress_len(body).map_err(|error| error.to_string())?;
        if decoded_length > 32768 {
            return Err("block exceeds the default 32 KiB input size".to_owned());
        }
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(body)
            .map_err(|error| error.to_string())?;
        blocks.push(decoded);
        remaining = &remaining[4 + length..];
    }
    Ok(blocks)
}

pub fn binary_input() -> Vec<u8> {
    (0..1024).map(|i| i as u8).collect()
}

pub fn multiblock_input() -> Vec<u8> {
    (0..70000).map(|i| ((i * 31 + 7) % 256) as u8).collect()
}

// Deterministic, poorly compressible fixture data; not cryptographic randomness.
pub fn incompressible_input(length: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_u32;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}
