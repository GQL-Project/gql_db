use super::dbtype::Value;

pub type Row = Vec<Value>;

#[derive(Clone)]
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

// Note that comparisons between two rows are not meaningful, and are not implemented.
impl PartialEq for RowInfo {
    fn eq(&self, other: &Self) -> bool {
        self.pagenum == other.pagenum && self.rownum == other.rownum
    }
}

impl Eq for RowInfo {}
