extern crate rucene;
extern crate serde_json;

use rucene::core::codec::CodecEnum;
use rucene::core::doc::{DocValuesType, Field, FieldType, Fieldable};
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{self, DoublePoint, FloatPoint, LongPoint, Query};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::{self, FSDirectory};
use serde_json::{Error, Value};

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
}

impl FieldableDocument {
    fn new() -> Self {
        FieldableDocument { fields: Vec::new() }
    }

    fn add_field(&mut self, name: String, value: f64) {
        self.fields.push((name, value));
    }
}

// Implement Send and Sync for FieldableDocument

unsafe impl Send for FieldableDocument {}

unsafe impl Sync for FieldableDocument {}

const COMMIT_BATCH_SIZE: usize = 30_000_000;

fn process_chunk(
    lines: &[String],

    documents: &Arc<Mutex<Vec<FieldableDocument>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for line in lines {
        let value: Value = serde_json::from_str(&line)?;

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
        if (value.get("mta_tax").is_some()) {
            doc.add_field("mta_tax".into(), value["mta_tax"].as_f64().unwrap());
        }
        if value.get("tolls_amount").is_some() {
            doc.add_field(
                "tolls_amount".into(),
                value["tolls_amount"].as_f64().unwrap(),
            );
        }

        if value.get("tip_amount").is_some() {
            doc.add_field("tip_amount".into(), value["tip_amount"].as_f64().unwrap());
        }

        if value.get("fare_amount").is_some() {
            doc.add_field("fare_amount".into(), value["fare_amount"].as_f64().unwrap());
        }

        if value.get("improvement_surcharge").is_some() {
            doc.add_field(
                "improvement_surcharge".into(),
                value["improvement_surcharge"].as_f64().unwrap(),
            );
        }

        if value.get("extra").is_some() {
            doc.add_field("extra".into(), value["extra"].as_f64().unwrap());
        }

        if value.get("ehail_fee").is_some() {
            doc.add_field("ehail_fee".into(), value["ehail_fee"].as_f64().unwrap());
        }

        if value.get("total_amount").is_some() {
            doc.add_field(
                "total_amount".into(),
                value["total_amount"].as_f64().unwrap(),
            );
        }

        if value.get("trip_distance").is_some() {
            doc.add_field(
                "trip_distance".into(),
                value["trip_distance"].as_f64().unwrap(),
            );
        }

        documents.lock().unwrap().push(doc);
    }

    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // create index directory
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
        fs::create_dir(&dir_path)?;
    }

    // create index writer
    let config = Arc::new(IndexWriterConfig::default());
    let directory = Arc::new(FSDirectory::with_path(&dir_path)?);
    let writer = IndexWriter::new(directory, config)?;

    if let Ok(file) = File::open("/home/hvamsi/code/rucene/data/documents.json") {
        let start_time: Instant = Instant::now();

        let num_threads = num_cpus::get(); // Use all available CPU cores

        let mut reader = BufReader::new(file);

        let batch_size = 10_000; // Adjust this value as needed

        let mut buffer = String::new();

        let mut batch: Vec<String> = Vec::with_capacity(batch_size);

        let documents: Arc<Mutex<Vec<FieldableDocument>>> = Arc::new(Mutex::new(Vec::new()));
        let writers = Arc::new(Mutex::new(Vec::new()));

        loop {
            buffer.clear();

            match reader.read_line(&mut buffer) {
                Ok(0) => break, // End of file

                Ok(_) => {
                    batch.push(buffer.clone());

                    if batch.len() == batch_size {
                        let documents = Arc::clone(&documents);

                        let batch_lines = std::mem::take(&mut batch);
                        println!("{:#?}", batch_lines.get(0));
                        thread::spawn(move || {
                            if let Err(e) = process_chunk(&batch_lines, &documents) {
                                eprintln!("Error processing chunk: {}", e);
                            }
                        });

                        batch = Vec::with_capacity(batch_size);
                    }
                }

                Err(e) => panic!("Error reading file: {}", e),
            }
        }

        // Process the remaining lines in the batch

        if !batch.is_empty() {
            let documents = Arc::clone(&documents);

            thread::spawn(move || {
                if let Err(e) = process_chunk(&batch, &documents) {
                    eprintln!("Error processing chunk: {}", e);
                }
            });
        }

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let documents = Arc::clone(&documents);
                let writers = Arc::clone(&writers);
                let writer_clone = writer.clone();
                thread::spawn(move || {
                    let mut document_count = 0;

                    for doc in documents.lock().unwrap().drain(..) {
                        let writer_clone = writer_clone.clone();
                        let doc: Vec<Box<dyn Fieldable>> = doc
                            .fields
                            .into_iter()
                            .map(|(name, value)| {
                                Box::new(new_index_numeric_field(name, value)) as Box<dyn Fieldable>
                            })
                            .collect();

                        writer_clone.add_document(doc).unwrap();
                        println!("adding docs");
                        document_count += 1;

                        if document_count >= COMMIT_BATCH_SIZE {
                            writer_clone.commit().unwrap();

                            document_count = 0;

                            writers.lock().unwrap().push(writer_clone);
                        }
                    }
                    if document_count > 0 {
                        // let mut writer = writer.clone();

                        writer_clone.commit().unwrap();
                        writers.lock().unwrap().push(writer_clone);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let time = Instant::now().duration_since(start_time).as_secs_f32();

        println!("indexing time, {}", time);
    }

    Ok(())
}
