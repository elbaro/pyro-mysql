use crate::value::Value;
use zero_mysql::constant::ColumnType;
use zero_mysql::protocol::primitive::*;
use zero_mysql::protocol::r#trait::param::Param;

/// Wrapper to implement zero_mysql::Param for our Value type
impl Param for &Value {
    fn is_null(&self) -> bool {
        matches!(self, Value::NULL)
    }

    fn write_type(&self, out: &mut Vec<u8>) {
        match self {
            Value::NULL => {
                out.push(ColumnType::MYSQL_TYPE_NULL as u8);
                out.push(0x00);
            }
            Value::Bytes(_) => {
                out.push(ColumnType::MYSQL_TYPE_BLOB as u8);
                out.push(0x00);
            }
            Value::Str(_) => {
                out.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
                out.push(0x00);
            }
            Value::Int(_) => {
                out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
                out.push(0x00);
            }
            Value::UInt(_) => {
                out.push(ColumnType::MYSQL_TYPE_LONGLONG as u8);
                out.push(0x80); // unsigned flag
            }
            Value::Float(_) => {
                out.push(ColumnType::MYSQL_TYPE_FLOAT as u8);
                out.push(0x00);
            }
            Value::Double(_) => {
                out.push(ColumnType::MYSQL_TYPE_DOUBLE as u8);
                out.push(0x00);
            }
            Value::Date(_, _, _, hour, minute, second, micro) => {
                // Determine if this is a DATE or DATETIME based on time components
                if *hour == 0 && *minute == 0 && *second == 0 && *micro == 0 {
                    out.push(ColumnType::MYSQL_TYPE_DATE as u8);
                } else {
                    out.push(ColumnType::MYSQL_TYPE_DATETIME as u8);
                }
                out.push(0x00);
            }
            Value::Time(_, _, _, _, _, _) => {
                out.push(ColumnType::MYSQL_TYPE_TIME as u8);
                out.push(0x00);
            }
        }
    }

    fn write_value(&self, out: &mut Vec<u8>) -> zero_mysql::error::Result<()> {
        match self {
            Value::NULL => {
                // NULL values don't write anything
                Ok(())
            }
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                write_bytes_lenenc(out, bytes_ref);
                Ok(())
            }
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                write_string_lenenc(out, str_ref);
                Ok(())
            }
            Value::Int(v) => {
                write_int_8(out, *v as u64);
                Ok(())
            }
            Value::UInt(v) => {
                write_int_8(out, *v);
                Ok(())
            }
            Value::Float(v) => {
                write_int_4(out, v.to_bits());
                Ok(())
            }
            Value::Double(v) => {
                write_int_8(out, v.to_bits());
                Ok(())
            }
            Value::Date(year, month, day, hour, minute, second, micro) => {
                // Write DATE/DATETIME value
                // Format depends on whether time components are zero
                if *hour == 0 && *minute == 0 && *second == 0 && *micro == 0 {
                    // DATE: 4 bytes (year:2, month:1, day:1)
                    write_int_1(out, 4); // length
                    write_int_2(out, *year);
                    write_int_1(out, *month);
                    write_int_1(out, *day);
                } else if *micro == 0 {
                    // DATETIME without microseconds: 7 bytes
                    write_int_1(out, 7); // length
                    write_int_2(out, *year);
                    write_int_1(out, *month);
                    write_int_1(out, *day);
                    write_int_1(out, *hour);
                    write_int_1(out, *minute);
                    write_int_1(out, *second);
                } else {
                    // DATETIME with microseconds: 11 bytes
                    write_int_1(out, 11); // length
                    write_int_2(out, *year);
                    write_int_1(out, *month);
                    write_int_1(out, *day);
                    write_int_1(out, *hour);
                    write_int_1(out, *minute);
                    write_int_1(out, *second);
                    write_int_4(out, *micro);
                }
                Ok(())
            }
            Value::Time(is_negative, days, hours, minutes, seconds, micro) => {
                // Write TIME value
                if *micro == 0 {
                    // TIME without microseconds: 8 bytes
                    write_int_1(out, 8); // length
                    write_int_1(out, if *is_negative { 1 } else { 0 });
                    write_int_4(out, *days);
                    write_int_1(out, *hours);
                    write_int_1(out, *minutes);
                    write_int_1(out, *seconds);
                } else {
                    // TIME with microseconds: 12 bytes
                    write_int_1(out, 12); // length
                    write_int_1(out, if *is_negative { 1 } else { 0 });
                    write_int_4(out, *days);
                    write_int_1(out, *hours);
                    write_int_1(out, *minutes);
                    write_int_1(out, *seconds);
                    write_int_4(out, *micro);
                }
                Ok(())
            }
        }
    }
}
