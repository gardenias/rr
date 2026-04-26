#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use rr_network::proto::client::ClientRequest;

fuzz_target!(|data: &[u8]| {
    let _ = ClientRequest::decode(data);
});
