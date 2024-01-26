fn main() {
    prost_build::compile_protos(
        &[
            "proto/client_messages.proto",
            "proto/server_messages.proto",
            "proto/peer_messages.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
