use crate::params::Params as PyroParams;
use crate::value::Value;
use zero_mysql::error::Result;
use zero_mysql::protocol::r#trait::param::Param;
use zero_mysql::protocol::r#trait::params::Params;

/// Adapter to convert pyro-mysql Params to zero-mysql Params
pub struct ParamsAdapter<'a> {
    params: &'a PyroParams,
}

impl<'a> ParamsAdapter<'a> {
    pub fn new(params: &'a PyroParams) -> Self {
        Self { params }
    }
}

impl<'a> Params for ParamsAdapter<'a> {
    fn len(&self) -> usize {
        self.params.len()
    }

    fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    fn encode_null_bitmap(&self, out: &mut Vec<u8>) {
        match self.params {
            PyroParams::Empty => {}
            PyroParams::Positional(values) => {
                // Calculate number of bytes needed for NULL bitmap
                let num_bytes = (values.len() + 7) / 8;
                let start_len = out.len();
                out.resize(start_len + num_bytes, 0);

                // Set bits for NULL parameters
                for (i, value) in values.iter().enumerate() {
                    if value.is_null() {
                        let byte_pos = start_len + (i / 8);
                        let bit_offset = i % 8;
                        out[byte_pos] |= 1 << bit_offset;
                    }
                }
            }
        }
    }

    fn encode_types(&self, out: &mut Vec<u8>) {
        match self.params {
            PyroParams::Empty => {}
            PyroParams::Positional(values) => {
                for value in values.iter() {
                    let value_ref: &Value = value;
                    value_ref.encode_type(out);
                }
            }
        }
    }

    fn encode_values(&self, out: &mut Vec<u8>) -> Result<()> {
        match self.params {
            PyroParams::Empty => Ok(()),
            PyroParams::Positional(values) => {
                for value in values.iter() {
                    if !value.is_null() {
                        let value_ref: &Value = value;
                        value_ref.encode_value(out)?;
                    }
                }
                Ok(())
            }
        }
    }
}
