use std::io;
use std::process::ExitCode;

use rs_pq_inmem_cols2ints::SimpleConfig;

use rs_pq_inmem_cols2ints::Endian;

use rs_pq_inmem_cols2ints::BATCH_SIZE_DEFAULT;
use rs_pq_inmem_cols2ints::COLIX_DEFAULT;
use rs_pq_inmem_cols2ints::FILESIZE_MAX_DEFAULT;
use rs_pq_inmem_cols2ints::OFFSET_DEFAULT;
use rs_pq_inmem_cols2ints::SCALE_DEFAULT;

fn io_envkey2str(key: &'static str) -> impl Fn() -> String {
    move || std::env::var(key).unwrap_or_default()
}

fn io_envkey2usize(key: &'static str) -> impl Fn() -> Option<usize> {
    move || {
        let val: String = io_envkey2str(key)();
        str::parse(&val).ok()
    }
}

fn io_envkey2usize2noopt(key: &'static str, alt: usize) -> impl Fn() -> usize {
    move || {
        let oval: Option<_> = io_envkey2usize(key)();
        oval.unwrap_or(alt)
    }
}

fn io_batch_size() -> impl Fn() -> usize {
    io_envkey2usize2noopt("ENV_BATCH_SIZE", BATCH_SIZE_DEFAULT)
}

fn io_offset() -> impl Fn() -> usize {
    io_envkey2usize2noopt("ENV_OFFSET", OFFSET_DEFAULT)
}

fn io_colix4proj() -> impl Fn() -> usize {
    io_envkey2usize2noopt("ENV_COLUMN_INDEX", COLIX_DEFAULT)
}

fn io_limit() -> impl Fn() -> Option<usize> {
    io_envkey2usize("ENV_LIMIT")
}

fn io_endian() -> impl Fn() -> Endian {
    move || {
        let s: String = io_envkey2str("ENV_ENDIAN")();
        str::parse(&s).unwrap_or_default()
    }
}

fn io_filename() -> impl Fn() -> String {
    io_envkey2str("ENV_PARQUET_FILENAME")
}

fn io_scale() -> impl Fn() -> f64 {
    move || {
        let s: String = io_envkey2str("ENV_SCALE")();
        str::parse(&s).unwrap_or(SCALE_DEFAULT)
    }
}

fn io_filesize_max() -> impl Fn() -> u64 {
    move || {
        let s: String = io_envkey2str("ENV_FILE_SIZE_MAX")();
        str::parse(&s).unwrap_or(FILESIZE_MAX_DEFAULT)
    }
}

fn io_config() -> impl Fn() -> SimpleConfig {
    move || {
        let batch_size: usize = io_batch_size()();
        let limit: Option<usize> = io_limit()();
        let offset: usize = io_offset()();
        let colix4proj: usize = io_colix4proj()();
        let endian4sink: Endian = io_endian()();
        let pqfilename: String = io_filename()();
        let filesize_max: u64 = io_filesize_max()();
        let scale: f64 = io_scale()();

        SimpleConfig {
            batch_size,
            limit,
            offset,
            colix4proj,
            endian4sink,
            pqfilename,
            filesize_max,
            scale,
        }
    }
}

fn sub() -> Result<(), io::Error> {
    let cfg: SimpleConfig = io_config()();
    cfg.pqfile2mem2reader2stdout()?;
    Ok(())
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");

        let cfg: SimpleConfig = io_config()();

        eprintln!("ENV_BATCH_SIZE: {}", cfg.batch_size);
        eprintln!("ENV_OFFSET: {}", cfg.offset);
        eprintln!("ENV_COLUMN_INDEX: {}", cfg.colix4proj);
        eprintln!("ENV_LIMIT: {:#?}", cfg.limit);
        eprintln!("ENV_ENDIAN: {:#?}", cfg.endian4sink);
        eprintln!("ENV_PARQUET_FILENAME: {}", cfg.pqfilename);
        eprintln!("ENV_SCALE: {}", cfg.scale);
        eprintln!("ENV_FILE_SIZE_MAX: {}", cfg.filesize_max);

        ExitCode::FAILURE
    })
}
