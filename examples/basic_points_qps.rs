extern crate rucene;

use rucene::core::codec::CodecEnum;
use rucene::core::doc::{DocValuesType, Field, FieldType, Fieldable};
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{self, LongPoint, Query};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::FSDirectory;

use std::convert::TryInto;
use std::fs::{self, File};
use std::io::{self, BufRead, Error};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cmp, env, thread};

use rucene::error::Result;

fn indexed_numeric_field_type() -> FieldType {
    let mut field_type = FieldType::default();
    field_type.tokenized = false;
    field_type.doc_values_type = DocValuesType::Binary;
    field_type.dimension_count = 1;
    field_type.dimension_num_bytes = 8;
    field_type
}

fn new_index_numeric_field(field_name: String, data: i64) -> Field {
    Field::new_bytes(
        field_name,
        LongPoint::pack(&[data]),
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

struct Worker {
    queries:
        Arc<Mutex<Vec<std::prelude::v1::Result<Box<dyn Query<CodecEnum>>, rucene::error::Error>>>>,
    searcher: Arc<
        DefaultIndexSearcher<
            CodecEnum,
            rucene::core::index::reader::StandardDirectoryReader<
                FSDirectory,
                CodecEnum,
                rucene::core::index::merge::SerialMergeScheduler,
                rucene::core::index::merge::TieredMergePolicy,
            >,
            Arc<
                rucene::core::index::reader::StandardDirectoryReader<
                    FSDirectory,
                    CodecEnum,
                    rucene::core::index::merge::SerialMergeScheduler,
                    rucene::core::index::merge::TieredMergePolicy,
                >,
            >,
            rucene::core::search::DefaultSimilarityProducer,
        >,
    >,
    pool: Arc<Mutex<i32>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    pos: usize,
}

impl Worker {
    fn new(
        queries: Arc<
            Mutex<Vec<std::prelude::v1::Result<Box<dyn Query<CodecEnum>>, rucene::error::Error>>>,
        >,
        searcher: Arc<
            DefaultIndexSearcher<
                CodecEnum,
                rucene::core::index::reader::StandardDirectoryReader<
                    FSDirectory,
                    CodecEnum,
                    rucene::core::index::merge::SerialMergeScheduler,
                    rucene::core::index::merge::TieredMergePolicy,
                >,
                Arc<
                    rucene::core::index::reader::StandardDirectoryReader<
                        FSDirectory,
                        CodecEnum,
                        rucene::core::index::merge::SerialMergeScheduler,
                        rucene::core::index::merge::TieredMergePolicy,
                    >,
                >,
                rucene::core::search::DefaultSimilarityProducer,
            >,
        >,
        start_pos: usize,
        pool: Arc<Mutex<i32>>,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Worker {
            queries,
            searcher,
            pool,
            stop,
            pos: start_pos,
        }
    }

    fn run(&mut self) {
        let cur_state = self.stop.load(std::sync::atomic::Ordering::Relaxed);
        while !cur_state {
            let mut wait = true;
            if let Ok(mut pool) = self.pool.lock() {
                if *pool > 0 {
                    let val = *pool;
                    *pool -= 1;
                    if val >= 0 {
                        let mut manager = TopDocsCollector::new(10);
                        let query_lock = self.queries.lock().unwrap();
                        let query = query_lock.get(self.pos);
                        let q = query.unwrap();
                        let r = q.as_ref().unwrap();
                        match self.searcher.search(&**r, &mut manager) {
                            Ok(_) => {
                                self.pos += 1;
                                if self.pos >= self.queries.lock().unwrap().len() {
                                    self.pos = 0;
                                }
                                wait = false;
                            }
                            Err(e) => {
                                eprintln!("{}", e);
                            }
                        }
                    } else {
                        *pool += 1;
                    }
                }
            }
            if wait {
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn main() -> Result<()> {
    // create index directory
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);
    // if dir_path.exists() {
    //     fs::remove_dir_all(&dir_path)?;
    //     fs::create_dir(&dir_path)?;
    // }

    let worker_count = std::env::args()
        .nth(1)
        .ok_or(Error::new(
            std::io::ErrorKind::Other,
            "Missing worker count argument",
        ))?
        .parse::<usize>()?;

    // create index writer
    let config = Arc::new(IndexWriterConfig::default());
    let directory = Arc::new(FSDirectory::with_path(&dir_path)?);
    let writer = IndexWriter::new(directory, config)?;

    let queries = Arc::new(Mutex::new(vec![]));

    let qps: AtomicI32 = 5000.into();

    let mut sum: u128 = 0;

    if let Ok(mut lines) = read_lines("../range_datapoints") {
        let num_docs: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();
        // Consumes the iterator, returns an (Optional) String

        for n in 0..*num_docs {
            let timestamp: &i64 = &lines.next().unwrap().unwrap().parse().unwrap();
            let numeric_field = new_index_numeric_field("timestamp".into(), *timestamp);
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            // doc.push(Box::new(numeric_field));

            // writer.add_document(doc)?;

            // if n > 0 && n % 1000000 == 0 {
            //     writer.commit()?;
            // }
        }
        let num_queries: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();

        for _ in 0..*num_queries {
            let l = lines.next().unwrap().unwrap();

            let mut range = l.split(',');

            let lower = range.next().unwrap();

            let lower_bound: i64 = lower.parse::<i64>().unwrap();

            let upper = range.next().unwrap();

            let upper_bound: i64 = upper.parse::<i64>().unwrap();

            queries.lock().unwrap().push(LongPoint::new_range_query(
                "timestamp".into(),
                lower_bound,
                upper_bound,
            ));
        }
    }

    let reader = writer.get_reader(true, false)?;
    let index_searcher: Arc<
        DefaultIndexSearcher<
            CodecEnum,
            rucene::core::index::reader::StandardDirectoryReader<
                FSDirectory,
                CodecEnum,
                rucene::core::index::merge::SerialMergeScheduler,
                rucene::core::index::merge::TieredMergePolicy,
            >,
            Arc<
                rucene::core::index::reader::StandardDirectoryReader<
                    FSDirectory,
                    CodecEnum,
                    rucene::core::index::merge::SerialMergeScheduler,
                    rucene::core::index::merge::TieredMergePolicy,
                >,
            >,
            rucene::core::search::DefaultSimilarityProducer,
        >,
    > = Arc::new(DefaultIndexSearcher::new(Arc::new(reader), None));

    let pool = Arc::new(Mutex::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let query_offset = queries.lock().unwrap().len() / worker_count;

    let mut worker_handles = vec![];

    for i in 0..worker_count {
        let queries_clone = queries.clone();
        let searcher_clone = index_searcher.clone();
        let pool_clone = pool.clone();
        let stop_clone = stop.clone();
        let start_pos = query_offset * i;

        let handle: thread::JoinHandle<()> = thread::spawn(move || {
            let mut worker = Worker::new(
                queries_clone,
                searcher_clone,
                start_pos,
                pool_clone,
                stop_clone,
            );
            worker.run();
        });

        worker_handles.push(handle);
    }

    let mut max_qps = 0;

    let mut current_qps = 5000;
    let mut delta_qps = 500;
    let mut found_target = 0;

    loop {
        let mut pool_size = pool.lock().unwrap();
        if *pool_size <= 0 {
            current_qps += delta_qps;
            println!("Increasing QPS to {}", current_qps);
            *pool_size += current_qps;
        } else {
            delta_qps /= 2;
            if delta_qps == 0 {
                found_target += 1;
                if found_target >= 5 {
                    max_qps = current_qps;
                    break;
                }
                delta_qps = 1;
            }
            current_qps -= delta_qps;
            println!("Decreasing QPS to {}", current_qps);
            *pool_size = current_qps;
        }
        drop(pool_size);
        thread::sleep(Duration::from_secs(1));
    }

    println!("Maximum QPS: {}", max_qps);

    for handle in worker_handles {
        handle.join().unwrap();
    }

    Ok(())
}
