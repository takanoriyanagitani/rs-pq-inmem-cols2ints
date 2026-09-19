use std::fs::File;
use std::io;

use io::Read;

use io::BufWriter;
use io::Write;

use bytes::Bytes;

use arrow_schema::DataType;

use arrow_array::RecordBatch;

use arrow_array::array::ArrayRef;
use arrow_array::cast::AsArray;

use arrow_array::array::Int32Array;
use arrow_array::array::Int64Array;

use arrow_array::array::Float32Array;
use arrow_array::array::Float64Array;

use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use parquet::file::reader::ChunkReader;

pub struct PqReaderBuilder<C>(pub C);

impl<C> PqReaderBuilder<C>
where
    C: ChunkReader + 'static,
{
    pub fn into_reader(
        self,
        batch_size: usize,
        limit: Option<usize>,
        offset: usize,
        colix: usize,
    ) -> Result<ParquetRecordBatchReader, io::Error> {
        let mut bldr: ParquetRecordBatchReaderBuilder<_> =
            ParquetRecordBatchReaderBuilder::try_new(self.0).map_err(io::Error::other)?;
        let sd: &SchemaDescriptor = bldr.parquet_schema();
        let colcnt: usize = sd.num_columns();
        if colcnt <= colix {
            return Err(io::Error::other("colum index out of range"));
        }

        let mask: ProjectionMask = ProjectionMask::leaves(sd, [colix]);
        if let Some(lmt) = limit {
            bldr = bldr.with_limit(lmt);
        }

        bldr.with_batch_size(batch_size)
            .with_offset(offset)
            .with_projection(mask)
            .build()
            .map_err(io::Error::other)
    }
}

pub fn ints2sink<S>(ints: &Int32Array, sink: &mut S) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
{
    let values: &[i32] = ints.values();
    for val in values {
        sink(*val)?;
    }
    Ok(())
}

pub fn longs2sink<S, C>(longs: &Int64Array, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(i64) -> i32,
{
    let values: &[i64] = longs.values();
    for val in values {
        let converted: i32 = conv(*val);
        sink(converted)?;
    }
    Ok(())
}

pub fn floats2sink<S, C>(floats: &Float32Array, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(f32) -> i32,
{
    let values: &[f32] = floats.values();
    for val in values {
        let converted: i32 = conv(*val);
        sink(converted)?;
    }
    Ok(())
}

pub fn doubles2sink<S, C>(doubles: &Float64Array, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(f64) -> i32,
{
    let values: &[f64] = doubles.values();
    for val in values {
        let converted: i32 = conv(*val);
        sink(converted)?;
    }
    Ok(())
}

pub fn i32arr2sink<S>(i32arr: &ArrayRef, sink: &mut S) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
{
    let oarr: Option<&Int32Array> = i32arr.as_primitive_opt();
    let arr: &_ = oarr.ok_or(io::Error::other("not an int32 array"))?;
    ints2sink(arr, sink)
}

pub fn i64arr2sink<S, C>(i64arr: &ArrayRef, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(i64) -> i32,
{
    let oarr: Option<&Int64Array> = i64arr.as_primitive_opt();
    let arr: &_ = oarr.ok_or(io::Error::other("not an int64 array"))?;
    longs2sink(arr, sink, conv)
}

pub fn f32arr2sink<S, C>(f32arr: &ArrayRef, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(f32) -> i32,
{
    let oarr: Option<&Float32Array> = f32arr.as_primitive_opt();
    let arr: &_ = oarr.ok_or(io::Error::other("not an float32 array"))?;
    floats2sink(arr, sink, conv)
}

pub fn f64arr2sink<S, C>(f64arr: &ArrayRef, sink: &mut S, conv: &C) -> Result<(), io::Error>
where
    S: FnMut(i32) -> Result<(), io::Error>,
    C: Fn(f64) -> i32,
{
    let oarr: Option<&Float64Array> = f64arr.as_primitive_opt();
    let arr: &_ = oarr.ok_or(io::Error::other("not an float64 array"))?;
    doubles2sink(arr, sink, conv)
}

pub fn l2i_simple(l: i64) -> i32 {
    l as i32
}

pub fn f2i_scale_round(f: f32, scale: f32) -> i32 {
    (f * scale).round() as i32
}
pub fn d2i_scale_round(d: f64, scale: f64) -> i32 {
    (d * scale).round() as i32
}

pub fn scale2conv32round(scale: f32) -> impl Fn(f32) -> i32 {
    move |f: f32| f2i_scale_round(f, scale)
}

pub fn scale2conv64round(scale: f64) -> impl Fn(f64) -> i32 {
    move |f: f64| d2i_scale_round(f, scale)
}

pub struct SimpleConv<L, F, D> {
    pub long2i: L,
    pub float2i: F,
    pub double2i: D,
}

impl<L, F, D> SimpleConv<L, F, D>
where
    L: Fn(i64) -> i32,
    F: Fn(f32) -> i32,
    D: Fn(f64) -> i32,
{
    pub fn arr2sink<S>(&self, aref: &ArrayRef, sink: &mut S) -> Result<(), io::Error>
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        let dtyp: &DataType = aref.data_type();
        match dtyp {
            DataType::Float32 => f32arr2sink(aref, sink, &self.float2i),
            DataType::Float64 => f64arr2sink(aref, sink, &self.double2i),
            DataType::Int64 => i64arr2sink(aref, sink, &self.long2i),
            _ => Err(io::Error::other(format!("unsupported type: {dtyp}"))),
        }
    }
}

impl<L, F, D> SimpleConv<L, F, D>
where
    L: Fn(i64) -> i32,
    F: Fn(f32) -> i32,
    D: Fn(f64) -> i32,
{
    pub fn rbat2sink<S>(
        &self,
        rbat: &RecordBatch,
        colix: usize,
        sink: &mut S,
    ) -> Result<(), io::Error>
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        let col: &ArrayRef = rbat.column(colix);
        self.arr2sink(col, sink)
    }
}

pub trait BatchSink {
    fn rbat2sink(&mut self, rbat: &RecordBatch) -> Result<(), io::Error>;
}

impl<F> BatchSink for F
where
    F: FnMut(&RecordBatch) -> Result<(), io::Error>,
{
    fn rbat2sink(&mut self, rbat: &RecordBatch) -> Result<(), io::Error> {
        self(rbat)
    }
}

impl<L, F, D> SimpleConv<L, F, D>
where
    L: Fn(i64) -> i32,
    F: Fn(f32) -> i32,
    D: Fn(f64) -> i32,
{
    pub fn into_bat_sink<S>(self, colix: usize, mut sink: S) -> impl BatchSink
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        move |b: &RecordBatch| self.rbat2sink(b, colix, &mut sink)
    }
}

pub struct PqReader(pub ParquetRecordBatchReader);

impl PqReader {
    pub fn into_sink<S>(self, mut sink: S) -> Result<(), io::Error>
    where
        S: BatchSink,
    {
        for rslt in self.0 {
            let rbat: RecordBatch = rslt.map_err(io::Error::other)?;
            sink.rbat2sink(&rbat)?;
        }
        Ok(())
    }
}

pub const BATCH_SIZE_DEFAULT: usize = 8192;
pub const OFFSET_DEFAULT: usize = 0;
pub const COLIX_DEFAULT: usize = 0;
pub const FILESIZE_MAX_DEFAULT: u64 = 1048576; // low value by default
pub const SCALE_DEFAULT: f64 = 1.0;

pub struct SimpleConfig {
    pub batch_size: usize,
    pub limit: Option<usize>,
    pub offset: usize,
    pub colix4proj: usize,

    pub endian4sink: Endian,

    pub pqfilename: String,

    pub filesize_max: u64,

    pub scale: f64,
}

impl SimpleConfig {
    pub fn to_batch_sink<S>(&self, sink: S) -> impl BatchSink
    where
        S: FnMut(i32) -> Result<(), io::Error>,
    {
        SimpleConv {
            long2i: l2i_simple,
            float2i: scale2conv32round(self.scale as f32),
            double2i: scale2conv64round(self.scale),
        }
        .into_bat_sink(0, sink) // will use already projected reader
    }
}

impl SimpleConfig {
    pub fn to_reader(&self) -> Result<ParquetRecordBatchReader, io::Error> {
        let f: File = File::open(&self.pqfilename)?;
        let mut taken = f.take(self.filesize_max);
        let mut v: Vec<u8> = vec![];
        taken.read_to_end(&mut v)?;
        let buf: Bytes = Bytes::from_owner(v); // implements ChunkReader

        PqReaderBuilder(buf).into_reader(self.batch_size, self.limit, self.offset, self.colix4proj)
    }
}

impl SimpleConfig {
    pub fn pqfile2mem2reader2sink<W>(&self, mut wtr: W) -> Result<(), io::Error>
    where
        W: Write,
    {
        let rdr: ParquetRecordBatchReader = self.to_reader()?;
        let sink = self.to_batch_sink(self.endian4sink.wtr2sink(&mut wtr)); // BatchSink
        PqReader(rdr).into_sink(sink)?;
        wtr.flush()
    }

    pub fn pqfile2mem2reader2stdout(&self) -> Result<(), io::Error> {
        let o = io::stdout();
        let mut ol = o.lock();
        self.pqfile2mem2reader2sink(BufWriter::new(&mut ol))?;
        ol.flush()
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub enum Endian {
    #[default]
    Lit,
    Big,
}

impl Endian {
    pub fn wtr2sink<W>(self, mut wtr: W) -> impl FnMut(i32) -> Result<(), io::Error>
    where
        W: Write,
    {
        let i2a = match self {
            Self::Lit => |i: i32| i.to_le_bytes(),
            Self::Big => |i: i32| i.to_be_bytes(),
        };

        move |i: i32| {
            let a: [u8; 4] = i2a(i);
            wtr.write_all(&a)
        }
    }
}

impl std::str::FromStr for Endian {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "l" => Ok(Self::Lit),
            "lit" => Ok(Self::Lit),
            "little" => Ok(Self::Lit),
            "Little" => Ok(Self::Lit),
            "b" => Ok(Self::Big),
            "big" => Ok(Self::Big),
            "Big" => Ok(Self::Big),
            _ => Err(io::Error::other("invalid endian string got")),
        }
    }
}
