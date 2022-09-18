use positioned_io::{RandomAccessFile, ReadAt, Size, WriteAt};
use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io::Error;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE]; // Array of Size 4KB

// Creates file with given name and size of Page Size
pub fn create_file(path: &String) -> Result<(), Error> {
    let file = File::create(path)?;
    file.set_len(PAGE_SIZE as u64)?;
    Ok(())
}

/* File IO on the Pages */

// We read and write only in multiples of Page Size
// For more details, refer to fileio/README.md
pub fn read_page(page_num: u64, path: &String) -> Result<Page, Error> {
    let mut buf = [0; PAGE_SIZE];
    let f = RandomAccessFile::open(path)?;
    f.read_at(page_num * PAGE_SIZE as u64, &mut buf)?;
    Ok(buf)
}

// It's memory efficient to just reuse our old buffer (when possible)
pub fn load_page(page_num: u64, path: &String, page: &mut Page) -> Result<(), Error> {
    let f = RandomAccessFile::open(path)?;
    f.read_at(page_num * PAGE_SIZE as u64, page)?;
    Ok(())
}

pub fn write_page(page_num: u64, path: &String, page: &Page) -> Result<(), Error> {
    let file = OpenOptions::new().write(true).open(path)?;
    let file_size = file.size()?.expect("File size is not available");
    if page_num * PAGE_SIZE as u64 > file_size {
        file.set_len((page_num + 1) * PAGE_SIZE as u64)?;
    }
    let mut f = RandomAccessFile::try_new(file)?;
    f.write_at(page_num * PAGE_SIZE as u64, page)?;
    Ok(())
}

/* Making reads and writes on the Pages */
pub fn read_type<T: Sized>(page: &Page, offset: usize) -> T {
    let size = std::mem::size_of::<T>();
    let mut buf = vec![0u8; size];
    buf.copy_from_slice(&page[offset..offset + size]);
    unsafe { std::ptr::read(buf.as_ptr() as *const T) }
}

pub fn read_string(page: &Page, offset: usize, size: usize) -> String {
    let mut buf = vec![0u8; size];
    buf.copy_from_slice(&page[offset..offset + size]);
    buf.retain(|&x| x != 0);
    String::from_utf8(buf).unwrap()
}

pub fn write_type<T: Sized>(page: &mut Page, offset: usize, value: T) {
    let size = std::mem::size_of::<T>();
    let mut buf = vec![0u8; size];
    unsafe { std::ptr::write(buf.as_mut_ptr() as *mut T, value) };
    page[offset..offset + size].copy_from_slice(&buf);
}

pub fn write_string(page: &mut Page, offset: usize, value: &str, len: usize) {
    let mut buf = vec![0u8; len];
    let size = min(len, value.len());
    buf[..size].copy_from_slice(&value.as_bytes()[..size]);
    page[offset..offset + len].copy_from_slice(&buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_io() {
        let path = "test_file_io".to_string();
        create_file(&path).unwrap();
        let mut page = [0u8; PAGE_SIZE];
        write_type::<u32>(&mut page, 0, 1);
        write_type::<u32>(&mut page, 4, 2);
        write_page(0, &path, &page).unwrap();
        write_type::<u32>(&mut page, 0, 12);
        write_type::<u32>(&mut page, 4, 9564);
        load_page(0, &path, &mut page).unwrap();
        assert_eq!(read_type::<u32>(&page, 0), 1);
        assert_eq!(read_type::<u32>(&page, 4), 2);
        // Clean up by removing file
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_pages_u8() {
        let mut page = [0; PAGE_SIZE];
        write_type::<u8>(&mut page, 0, 241);
        assert_eq!(read_type::<u8>(&page, 0), 241);
        assert_eq!(read_type::<u8>(&page, 1), 0);
        write_type::<u8>(&mut page, 436, 241);
        assert_eq!(read_type::<u8>(&page, 436), 241);
    }

    #[test]
    fn test_pages_u16() {
        let mut page = [0; PAGE_SIZE];
        write_type::<u16>(&mut page, 0, 1241);
        assert_eq!(read_type::<u16>(&page, 0), 1241);
        assert_eq!(read_type::<u16>(&page, 2), 0);
        write_type::<u16>(&mut page, 2456, 30321);
        assert_eq!(read_type::<u16>(&page, 2456), 30321);
    }

    #[test]
    fn test_pages_array() {
        let mut page = [0; PAGE_SIZE];
        write_type::<[char; 5]>(&mut page, 0, ['A', 'B', 'C', 'D', 'E']);
        assert_eq!(read_type::<[char; 5]>(&page, 0), ['A', 'B', 'C', 'D', 'E']);
    }

    #[test]
    fn test_pages_strings() {
        let mut page = [0; PAGE_SIZE];
        // Works with exact length strings
        write_string(&mut page, 0, "Test", 5);
        assert_eq!(read_string(&page, 0, 5), "Test");
        // Works with strings that are shorter than the length
        write_string(&mut page, 0, "Hello", 10);
        assert_eq!(read_string(&page, 0, 10), "Hello");
        // Truncate large strings to fit in the buffer
        write_string(&mut page, 0, "Hello, World!", 10);
        assert_eq!(read_string(&page, 0, 10), "Hello, Wor");
    }
}
