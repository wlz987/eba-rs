pub mod body;
pub mod look;

pub use body::{
    err_payload, is_result_err, is_result_ok, ok_payload, result_body, result_error,
    result_request_id, result_value,
};
pub use look::looks_like_result_envelope;
