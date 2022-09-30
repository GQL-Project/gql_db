use super::dbtype::Value;

pub type Row = Vec<Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct RowLocation {
    pub pagenum: u32,
    pub rownum: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowInfo {
    pub row: Row,
    pub pagenum: u32,
    pub rownum: u16,
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
