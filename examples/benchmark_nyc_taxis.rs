extern crate crossbeam_channel;
extern crate rucene;
extern crate serde_json;

use rucene::core::analysis::WhitespaceTokenizer;
use rucene::core::codec::CodecEnum;
use rucene::core::doc::{DocValuesType, Field, FieldType, Fieldable, IndexOptions, Term};
use rucene::core::index::merge::{SerialMergeScheduler, TieredMergePolicy};
use rucene::core::index::reader::StandardDirectoryReader;
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{
    self, DoublePoint, FloatPoint, LongPoint, MatchAllDocsQuery, PointRangeQuery, Query, TermQuery,
};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::{self, FSDirectory};
use serde_json::{Error, Value};

use crossbeam_channel::{unbounded, Receiver, Sender};

use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::convert::TryInto;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{cmp, env, ptr, thread};

fn indexed_numeric_field_type() -> FieldType {
    let mut field_type = FieldType::default();
    field_type.tokenized = false;
    field_type.doc_values_type = DocValuesType::Null;
    field_type.dimension_count = 1;
    field_type.dimension_num_bytes = 8;
    field_type
}

fn new_index_numeric_field(field_name: String, data: f64) -> Field {
    Field::new_bytes(
        field_name,
        DoublePoint::pack(&[data]),
        indexed_numeric_field_type(),
    )
}

fn indexed_text_field_type() -> FieldType {
    let mut field_type = FieldType::default();
    field_type.index_options = IndexOptions::DocsAndFreqsAndPositionsAndOffsets;
    field_type.store_term_vectors = true;
    field_type.store_term_vector_offsets = true;
    field_type.store_term_vector_positions = true;
    field_type
}

fn new_index_text_field(field_name: String, text: String) -> Field {
    let token_stream = WhitespaceTokenizer::new(Box::new(StringReader::new(text)));
    Field::new(
        field_name,
        indexed_text_field_type(),
        None,
        Some(Box::new(token_stream)),
    )
}

struct StringReader {
    text: String,
    index: usize,
}

impl StringReader {
    fn new(text: String) -> Self {
        StringReader { text, index: 0 }
    }
}

impl io::Read for StringReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remain = buf.len().min(self.text.len() - self.index);
        if remain > 0 {
            buf[..remain].copy_from_slice(&self.text.as_bytes()[self.index..self.index + remain]);
            self.index += remain;
        }
        Ok(remain)
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[derive(Debug)]
struct FieldableDocument {
    fields: Vec<(String, f64)>,
    str_fields: Vec<(String, String)>,
}

impl FieldableDocument {
    fn new() -> Self {
        FieldableDocument {
            fields: Vec::new(),
            str_fields: Vec::new(),
        }
    }

    fn add_field(&mut self, name: String, value: f64) {
        self.fields.push((name, value));
    }

    fn add_str_field(&mut self, name: String, value: String) {
        self.str_fields.push((name, value))
    }
}

// Implement Send and Sync for FieldableDocument

unsafe impl Send for FieldableDocument {}

unsafe impl Sync for FieldableDocument {}

const COMMIT_BATCH_SIZE: usize = 10_000_000;

fn process_chunk(
    lines: &str,
    documents: &Arc<Mutex<Vec<FieldableDocument>>>,
    writers: &Arc<
        Mutex<
            Vec<
                IndexWriter<
                    FSDirectory,
                    rucene::core::codec::CodecEnum,
                    rucene::core::index::merge::SerialMergeScheduler,
                    rucene::core::index::merge::TieredMergePolicy,
                >,
            >,
        >,
    >,
) -> Result<(), Box<dyn std::error::Error>> {
    // for line in lines {
    let value: Value = serde_json::from_str(&lines)?;

    let mut doc = FieldableDocument::new();

    doc.add_field(
        "passenger_count".to_string(),
        value["passenger_count"].as_f64().unwrap(),
    );

    if value.get("surcharge").is_some() {
        doc.add_field(
            "surcharge".to_string(),
            value["surcharge"].as_f64().unwrap(),
        );
    }

    if value.get("dropoff_datetime").is_some() {
        doc.add_str_field(
            "dropoff_datetime".into(),
            value["dropoff_datetime"].as_str().unwrap().to_string(),
        );
    }

    if value.get("trip_type").is_some() {
        doc.add_str_field(
            "trip_type".into(),
            value["trip_type"].as_str().unwrap().to_string(),
        );
    }

    if value.get("mta_tax").is_some() {
        doc.add_field("mta_tax".to_string(), value["mta_tax"].as_f64().unwrap());
    }

    if value.get("rate_code_id").is_some() {
        doc.add_str_field(
            "rate_code_id".into(),
            value["rate_code_id"].as_str().unwrap().to_string(),
        );
    }

    if value.get("pickup_datetime").is_some() {
        doc.add_str_field(
            "pickup_datetime".into(),
            value["pickup_datetime"].as_str().unwrap().to_string(),
        );
    }

    if value.get("tolls_amount").is_some() {
        doc.add_field(
            "tolls_amount".to_string(),
            value["tolls_amount"].as_f64().unwrap(),
        );
    }

    if value.get("tip_amount").is_some() {
        doc.add_field(
            "tip_amount".to_string(),
            value["tip_amount"].as_f64().unwrap(),
        );
    }

    if value.get("payment_type").is_some() {
        doc.add_str_field(
            "payment_type".into(),
            value["payment_type"].as_str().unwrap().to_string(),
        );
    }

    if value.get("extra").is_some() {
        doc.add_field("extra".to_string(), value["extra"].as_f64().unwrap());
    }

    if value.get("vendor_id").is_some() {
        doc.add_str_field(
            "vendor_id".into(),
            value["vendor_id"].as_str().unwrap().to_string(),
        );
    }

    if value.get("store_and_fwd_flag").is_some() {
        doc.add_str_field(
            "store_and_fwd_flag".into(),
            value["store_and_fwd_flag"].as_str().unwrap().to_string(),
        );
    }

    if value.get("improvement_surcharge").is_some() {
        doc.add_field(
            "improvement_surcharge".to_string(),
            value["improvement_surcharge"].as_f64().unwrap(),
        );
    }

    if value.get("fare_amount").is_some() {
        doc.add_field(
            "fare_amount".to_string(),
            value["fare_amount"].as_f64().unwrap(),
        );
    }

    if value.get("ehail_fee").is_some() {
        doc.add_field(
            "ehail_fee".to_string(),
            value["ehail_fee"].as_f64().unwrap(),
        );
    }

    if value.get("cab_color").is_some() {
        doc.add_str_field(
            "cab_color".into(),
            value["cab_color"].as_str().unwrap().to_string(),
        );
    }

    if value.get("dropoff_location").is_some() {
        let geo_point_lat = value["dropoff_location"].as_array().unwrap().get(0);
        let geo_point_lon = value["dropoff_location"].as_array().unwrap().get(1);
        doc.add_field(
            "dropoff_location_lat".into(),
            geo_point_lat.unwrap().as_f64().unwrap(),
        );
        doc.add_field(
            "dropoff_location_lon".into(),
            geo_point_lon.unwrap().as_f64().unwrap(),
        );
    }

    if value.get("vendor_name").is_some() {
        doc.add_str_field(
            "vendor_name".into(),
            value["vendor_name"].as_str().unwrap().to_string(),
        );
    }

    if value.get("total_amount").is_some() {
        doc.add_field(
            "total_amount".to_string(),
            value["total_amount"].as_f64().unwrap(),
        );
    }

    if value.get("trip_distance").is_some() {
        doc.add_field(
            "trip_distance".into(),
            value["trip_distance"].as_f64().unwrap(),
        );
    }

    if value.get("pickup_location").is_some() {
        let geo_point_lat = value["pickup_location"].as_array().unwrap().get(0);
        let geo_point_lon = value["pickup_location"].as_array().unwrap().get(1);
        doc.add_field(
            "pickup_location_lat".into(),
            geo_point_lat.unwrap().as_f64().unwrap(),
        );
        doc.add_field(
            "pickup_location_lon".into(),
            geo_point_lon.unwrap().as_f64().unwrap(),
        );
    }

    documents.lock().unwrap().push(doc);

    let mut writers_lock = writers.lock().unwrap();

    if writers_lock.is_empty() {
        let config = Arc::new(IndexWriterConfig::default());

        let path = "/tmp/test_rucene";
        let dir_path = Path::new(path);

        let directory = Arc::new(FSDirectory::with_path(&dir_path)?);

        let writer = IndexWriter::new(directory, config)?;

        writers_lock.push(writer);
    }

    let mut writer = writers_lock.pop().unwrap();

    for doc in documents.lock().unwrap().drain(..) {
        let numeric_doc: Vec<Box<dyn Fieldable>> = doc
            .fields
            .into_iter()
            .map(|(name, value)| {
                Box::new(new_index_numeric_field(name, value)) as Box<dyn Fieldable>
            })
            .collect();

        let str_doc: Vec<Box<dyn Fieldable>> = doc
            .str_fields
            .into_iter()
            .map(|(name, value)| Box::new(new_index_text_field(name, value)) as Box<dyn Fieldable>)
            .collect();

        writer.add_document(numeric_doc).unwrap();
        writer.add_document(str_doc).unwrap();
    }

    if documents.lock().unwrap().len() >= COMMIT_BATCH_SIZE {
        writer.commit().unwrap();
    }

    writers_lock.push(writer);

    Ok(())
}

fn indexing() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
        fs::create_dir(&dir_path)?;
    }

    if let Ok(file) = File::open("/home/hvamsi/code/rucene/data/documents.json") {
        let start_time: Instant = Instant::now();

        let mut reader = BufReader::new(file);

        let batch_size = 1_000_000; // Adjust this value as needed

        let num_threads = 48; // Get the number of CPU cores

        let mut buffer = String::new();
        let documents = Arc::new(Mutex::new(Vec::new()));
        let writers = Arc::new(Mutex::new(Vec::new()));

        // let mut batch = Vec::with_capacity(batch_size);

        let (tx, rx) = unbounded::<String>();

        let workers = (0..num_threads)
            .map(|_| {
                let rx = rx.clone();

                let documents_clone = documents.clone();
                let writers_clone = writers.clone();

                thread::spawn(move || {
                    for batch in rx {
                        println!("{:#?}", batch);
                        if let Err(e) = process_chunk(&batch, &documents_clone, &writers_clone) {
                            eprintln!("Error processing chunk: {}", e);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut i = 0;
        loop {
            buffer.clear();

            match reader.read_line(&mut buffer) {
                Ok(0) => {
                    let writer = writers.clone().lock().unwrap().pop().unwrap();
                    if writer.has_uncommitted_changes() {
                        writer.commit();
                    }
                    break;
                } // End of file

                Ok(_) => {
                    i += 1;
                    tx.send(buffer.clone()).unwrap();
                    if i == COMMIT_BATCH_SIZE {
                        let writer = writers.clone().lock().unwrap().pop().unwrap();
                        writer.commit();
                    }
                }

                Err(e) => panic!("Error reading file: {}", e),
            }
        }

        drop(tx); // Signal the end of input

        for worker in workers {
            worker.join().unwrap();
        }

        let time = Instant::now().duration_since(start_time).as_secs_f32();

        println!("indexing time, {}", time);
    }

    let config = Arc::new(IndexWriterConfig::default());
    Ok(())
}

fn querying() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);

    let directory = Arc::new(FSDirectory::with_path(&dir_path)?);

    // let dir_reader: StandardDirectoryReader<FSDirectory, CodecEnum,
    // SerialMergeScheduler,TieredMergePolicy > = StandardDirectoryReader::open(directory).unwrap();

    // let writer = IndexWriter::new(directory, config)?;
    let reader: StandardDirectoryReader<
        FSDirectory,
        CodecEnum,
        SerialMergeScheduler,
        TieredMergePolicy,
    > = StandardDirectoryReader::open(directory).unwrap();
    let index_searcher = DefaultIndexSearcher::new(Arc::new(reader), None);
    // let warmupCount = cmp::min(1000, queries.len());

    // for i in 0..warmupCount {
    //     let mut collector = TopDocsCollector::new(10);
    //     let query = queries.get(i).unwrap().as_ref().unwrap();
    //     index_searcher.search(&**query, &mut collector);
    // }

    let mut hits: usize = 0;

    let mut collector = TopDocsCollector::new(2000);
    let overall_start = Instant::now();
    let query1 = MatchAllDocsQuery;
    for i in 0..499 {
        index_searcher.search(&query1, &mut collector)?;
        hits += collector.top_docs().total_hits();
    }
    let query = DoublePoint::new_range_query("totalAmount".into(), 5.0, 15.0).unwrap();
    let mut collector = TopDocsCollector::new(2000);
    for i in 0..499 {
        index_searcher.search(&*query, &mut collector)?;
        hits += collector.top_docs().total_hits();
    }
    println!(
        "{}",
        Instant::now().duration_since(overall_start).as_nanos()
    );    println!(
        "Total hits: {}",
        hits
    );
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    querying();

    Ok(())
}
