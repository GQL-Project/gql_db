use crate::{util::row::RowInfo, fileio::tableio::Table};

#[derive(Clone)]
pub enum TableIterator {
    TableIter(Table),
    RowIter(RowIterator),
}

#[derive(Clone)]
pub struct RowIterator {
    pub rows: Vec<RowInfo>,
    index: usize
}

impl RowIterator {
    pub fn new(rows: Vec<RowInfo>) -> Self {
        Self {
            rows,
            index: 0
        }
    }
}

impl Iterator for RowIterator {
    // We can refer to this type using Self::Item
    type Item = RowInfo;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.rows.len() {
            let row: RowInfo = self.rows[self.index].clone();
            self.index += 1;
            Some(row)
        } else {
            None
        }
    }
}

impl Iterator for TableIterator {
    // We can refer to this type using Self::Item
    type Item = RowInfo;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TableIterator::TableIter(table) => {
                table.next()
            },
            TableIterator::RowIter(row_iter) => {
                row_iter.next()
            }
        }
    }
}