use serde::Serialize;

use super::dbtype::Value;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct RowLocation {
    pub pagenum: u32,
    pub rownum: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EmptyRowLocation {
    pub location: RowLocation,
    pub num_rows_empty: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowInfo {
    pub row: Row,
    pub pagenum: u32,
    pub rownum: u16,
}

impl RowInfo {
    pub fn get_row_location(&self) -> RowLocation {
        RowLocation {
            pagenum: self.pagenum,
            rownum: self.rownum,
        }
    }
}

impl Ord for RowInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pagenum
            .cmp(&other.pagenum)
            .then(self.rownum.cmp(&other.rownum))
    }
}

impl PartialOrd for RowInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for RowInfo {}
