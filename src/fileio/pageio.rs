use positioned_io::{RandomAccessFile, ReadAt, Size, WriteAt};
use std::fs::File;
use std::io::Error;

pub const PAGE_SIZE: usize = 4096;
pub type Page = [u8; PAGE_SIZE]; // Array of Size 4KB

/// Reads page at given page number from given file
pub fn read_page(page_num: u64, path: &String) -> Result<Page, Error> {
    let mut buf = [0; PAGE_SIZE];
    let f = RandomAccessFile::open(path)?;
    f.read_at(page_num * PAGE_SIZE as u64, &mut buf)?;
    Ok(buf)
}

// It's memory efficient to just reuse our old buffer
pub fn load_page(page_num: u64, path: &String, page: &mut Page) -> Result<(), Error> {
    let f = RandomAccessFile::open(path)?;
    f.read_at(page_num * PAGE_SIZE as u64, page)?;
    Ok(())
}

pub fn write_page(page_num: u64, path: &String, page: &Page) -> Result<(), Error> {
    let file = File::open(path)?;
    let file_size = file.size()?.expect("File size is not available");
    if page_num * PAGE_SIZE as u64 > file_size {
        file.set_len((page_num + 1) * PAGE_SIZE as u64)?;
    }
    let mut f = RandomAccessFile::try_new(file)?;
    f.write_at(page_num * PAGE_SIZE as u64, page)?;
    Ok(())
}

pub fn create_file(path: &String) -> Result<(), Error> {
    let file = File::create(path)?;
    file.set_len(PAGE_SIZE as u64)?;
    Ok(())
}

/// Read and Write operations for a page
/// These operations are implemented to be independent of the underlying
/// endianness of the system.
pub fn read_type<T: Sized>(page: &Page, offset: usize) -> T {
    let size = std::mem::size_of::<T>();
    let mut buf = vec![0u8; size];
    buf.copy_from_slice(&page[offset..offset + size]);
    // Get the value from the buffer, and return it
    unsafe { std::ptr::read(buf.as_ptr() as *const T) }
}

pub fn read_string(page: &Page, offset: usize, size: usize) -> String {
    let mut buf = vec![0u8; size];
    buf.copy_from_slice(&page[offset..offset + size]);
    String::from_utf8(buf).unwrap()
}

pub fn write_type<T: Sized>(page: &mut Page, offset: usize, value: T) {
    let size = std::mem::size_of::<T>();
    let mut buf = vec![0u8; size];
    unsafe { std::ptr::write(buf.as_mut_ptr() as *mut T, value) };
    // Write the value to the buffer
    page[offset..offset + size].copy_from_slice(&buf);
}

pub fn write_string(page: &mut Page, offset: usize, value: &str) {
    let size = value.len();
    page[offset..offset + size].copy_from_slice(value.as_bytes());
}