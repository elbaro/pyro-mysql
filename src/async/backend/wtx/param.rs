use pyo3::{
    prelude::*,
    pybacked::{PyBackedBytes, PyBackedStr},
};

// ─── WTX Parameter Types ──────────────────────────────────────────────────────

/// Enum representing various parameter types for wtx
/// Uses PyBackedStr and PyBackedBytes for zero-copy string and bytes handling
pub enum WtxParam {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(PyBackedStr),
    Bytes(PyBackedBytes),
}

impl WtxParam {
    /// Extract WtxParam from a Python object
    pub fn from_py(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        // Get the type object and its name
        let type_obj = value.get_type();
        let type_name = type_obj.name()?;
        let type_str = type_name.to_str()?;

        match type_str {
            "NoneType" => Ok(WtxParam::Null),
            "bool" => {
                let v = value.extract::<bool>()?;
                Ok(WtxParam::Bool(v))
            }
            "int" => {
                // Try i64 first, then convert to string if too large
                if let Ok(v) = value.extract::<i64>() {
                    Ok(WtxParam::I64(v))
                } else {
                    // Integer too large for i64, store as string
                    let s = value.str()?;
                    let backed_str = s.extract::<PyBackedStr>()?;
                    Ok(WtxParam::Str(backed_str))
                }
            }
            "float" => {
                let v = value.extract::<f64>()?;
                Ok(WtxParam::F64(v))
            }
            "str" => {
                let backed_str = value.extract::<PyBackedStr>()?;
                Ok(WtxParam::Str(backed_str))
            }
            "bytes" => {
                let backed_bytes = value.extract::<PyBackedBytes>()?;
                Ok(WtxParam::Bytes(backed_bytes))
            }
            _ => {
                // Default: convert to string
                let s = value.str()?;
                let backed_str = s.extract::<PyBackedStr>()?;
                Ok(WtxParam::Str(backed_str))
            }
        }
    }

    /// Encode this parameter to wtx buffer
    /// Returns true if the value is NULL
    pub fn encode_to_wtx(
        &self,
        ew: &mut wtx::database::client::mysql::MysqlEncodeWrapper<'_>,
    ) -> Result<bool, wtx::Error> {
        use wtx::de::Encode;

        let is_null = matches!(self, WtxParam::Null);
        self.encode(&mut (), ew)?;
        Ok(is_null)
    }

    /// Get type information for this parameter
    /// Returns (is_null, type_params)
    pub fn get_type_info(&self) -> (bool, Option<wtx::database::client::mysql::TyParams>) {
        use wtx::database::Typed;
        let is_null = matches!(self, WtxParam::Null);
        let ty = self.runtime_ty();
        (is_null, ty)
    }
}

// Implement Encode trait for WtxParam
impl wtx::de::Encode<wtx::database::client::mysql::Mysql<wtx::Error>> for WtxParam {
    fn encode(
        &self,
        aux: &mut (),
        ew: &mut wtx::database::client::mysql::MysqlEncodeWrapper<'_>,
    ) -> Result<(), wtx::Error> {
        use wtx::de::Encode;
        type Mysql = wtx::database::client::mysql::Mysql<wtx::Error>;

        match self {
            WtxParam::Null => {
                // NULL doesn't encode anything to the buffer
                Ok(())
            }
            WtxParam::Bool(v) => <bool as Encode<Mysql>>::encode(v, aux, ew),
            WtxParam::I64(v) => <i64 as Encode<Mysql>>::encode(v, aux, ew),
            WtxParam::F64(v) => <f64 as Encode<Mysql>>::encode(v, aux, ew),
            WtxParam::Str(s) => {
                let str_ref: &str = s.as_ref();
                <&str as Encode<Mysql>>::encode(&str_ref, aux, ew)
            }
            WtxParam::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                <&[u8] as Encode<Mysql>>::encode(&bytes_ref, aux, ew)
            }
        }
    }
}

// Implement Typed trait for WtxParam
impl wtx::database::Typed<wtx::database::client::mysql::Mysql<wtx::Error>> for WtxParam {
    fn runtime_ty(&self) -> Option<wtx::database::client::mysql::TyParams> {
        use wtx::database::client::mysql::{Ty, TyParams};
        const BINARY: u16 = 128; // Flag::Binary

        match self {
            WtxParam::Null => None,
            WtxParam::Bool(_) => Some(TyParams::new(BINARY, Ty::Tiny)),
            WtxParam::I64(_) => Some(TyParams::new(BINARY, Ty::LongLong)),
            WtxParam::F64(_) => Some(TyParams::new(BINARY, Ty::Double)),
            WtxParam::Str(_) => Some(TyParams::new(0, Ty::VarString)),
            WtxParam::Bytes(_) => Some(TyParams::new(BINARY, Ty::Blob)),
        }
    }

    fn static_ty() -> Option<wtx::database::client::mysql::TyParams>
    where
        Self: Sized,
    {
        // WtxParam is an enum with runtime-determined type
        None
    }
}

/// Lightweight parameter wrapper for wtx - holds WtxParam values
pub struct WtxParams {
    pub values: Vec<WtxParam>,
}

impl WtxParams {
    /// Create from Py<PyAny> (Python tuple/list/None)
    pub fn from_py(py: Python, params: &Py<PyAny>) -> PyResult<Self> {
        let params_bound = params.bind(py);

        // Handle None case
        if params_bound.is_none() {
            return Ok(Self { values: Vec::new() });
        }

        // Handle tuple case
        if let Ok(tuple) = params_bound.cast::<pyo3::types::PyTuple>() {
            let mut values = Vec::with_capacity(tuple.len());
            for item in tuple.iter() {
                values.push(WtxParam::from_py(&item)?);
            }
            return Ok(Self { values });
        }

        // Handle list case
        if let Ok(list) = params_bound.cast::<pyo3::types::PyList>() {
            let mut values = Vec::with_capacity(list.len());
            for item in list.iter() {
                values.push(WtxParam::from_py(&item)?);
            }
            return Ok(Self { values });
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected None, tuple, or list for params",
        ))
    }
}

// Implement RecordValues for WtxParams - encode WtxParam values directly
impl wtx::database::RecordValues<wtx::database::client::mysql::Mysql<wtx::Error>> for WtxParams {
    fn encode_values<'inner, 'outer, 'rem, A>(
        &self,
        aux: &mut A,
        ew: &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        prefix_cb: impl FnMut(
            &mut A,
            &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
        ) -> usize,
        suffix_cb: impl FnMut(
            &mut A,
            &mut <wtx::database::client::mysql::Mysql<wtx::Error> as wtx::de::DEController>::EncodeWrapper<'inner, 'outer, 'rem>,
            bool,
            usize,
        ) -> usize,
    ) -> Result<usize, wtx::Error>
    where
        'inner: 'outer,
    {
        self.values
            .as_slice()
            .encode_values(aux, ew, prefix_cb, suffix_cb)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn walk(
        &self,
        mut cb: impl FnMut(
            bool,
            Option<wtx::database::client::mysql::TyParams>,
        ) -> Result<(), wtx::Error>,
    ) -> Result<(), wtx::Error> {
        for param in &self.values {
            let (is_null, ty_params) = param.get_type_info();
            cb(is_null, ty_params)?;
        }
        Ok(())
    }
}
