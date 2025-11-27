use crate::params::Params as PyroParams;
use zero_mysql::error::Result;
use zero_mysql::protocol::r#trait::param::Param;
use zero_mysql::protocol::r#trait::param::Params;
use zero_mysql::protocol::command::bulk_exec::BulkParamsSet;

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
                    (&value).encode_type(out);
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
                        value.encode_value(out)?;
                    }
                }
                Ok(())
            }
        }
    }

    fn encode_values_for_bulk(&self, out: &mut Vec<u8>) -> Result<()> {
        use zero_mysql::protocol::r#trait::param::ParamIndicator;

        match self.params {
            PyroParams::Empty => Ok(()),
            PyroParams::Positional(values) => {
                for value in values.iter() {
                    if value.is_null() {
                        out.push(ParamIndicator::Null as u8);
                    } else {
                        out.push(ParamIndicator::None as u8);
                        value.encode_value(out)?;
                    }
                }
                Ok(())
            }
        }
    }
}

/// Wrapper for multiple parameter sets that implements BulkParamsSet
pub struct BulkParamsSetAdapter<'a> {
    params_list: Vec<ParamsAdapter<'a>>,
}

impl<'a> BulkParamsSetAdapter<'a> {
    pub fn new(params_list: Vec<ParamsAdapter<'a>>) -> Self {
        Self { params_list }
    }
}

impl<'a> BulkParamsSet for BulkParamsSetAdapter<'a> {
    fn encode_types(&self, out: &mut Vec<u8>) {
        if self.params_list.is_empty() {
            return;
        }

        // Determine number of columns from first row
        let num_columns = self.params_list[0].len();

        // For each column, find the first non-null value to infer type
        for col_idx in 0..num_columns {
            let mut found_type = false;

            for params_adapter in &self.params_list {
                match params_adapter.params {
                    PyroParams::Empty => continue,
                    PyroParams::Positional(values) => {
                        if col_idx < values.len() && !values[col_idx].is_null() {
                            // Found first non-null value, encode its type
                            (&values[col_idx]).encode_type(out);
                            found_type = true;
                            break;
                        }
                    }
                }
            }

            // If all values are null for this column, encode a default type
            if !found_type {
                // Use NULL type (MYSQL_TYPE_NULL) as default
                out.push(0x06); // MYSQL_TYPE_NULL
                out.push(0x00);
            }
        }
    }

    fn encode_rows(self, out: &mut Vec<u8>) -> Result<()> {
        use zero_mysql::protocol::r#trait::param::ParamIndicator;

        for params_adapter in self.params_list {
            match params_adapter.params {
                PyroParams::Empty => {}
                PyroParams::Positional(values) => {
                    for value in values.iter() {
                        if value.is_null() {
                            out.push(ParamIndicator::Null as u8);
                        } else {
                            out.push(ParamIndicator::None as u8);
                            value.encode_value(out)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<'a> IntoIterator for BulkParamsSetAdapter<'a> {
    type Item = ParamsAdapter<'a>;
    type IntoIter = std::vec::IntoIter<ParamsAdapter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.params_list.into_iter()
    }
}
