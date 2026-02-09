use crate::{deparse_raw, protobuf};
use crate::{Error, Result};

pub fn deparse_raw_iter(protobuf: &protobuf::ParseResult) -> Result<String> {
    // TODO: Implement iterative deparsing
    deparse_raw(protobuf)
}
