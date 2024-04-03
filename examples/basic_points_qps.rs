extern crate rucene;

use rucene::core::codec::CodecEnum;
use rucene::core::doc::{DocValuesType, Field, FieldType, Fieldable};
use rucene::core::index::writer::{IndexWriter, IndexWriterConfig};
use rucene::core::search::collector::TopDocsCollector;
use rucene::core::search::query::{self, LongPoint, Query};
use rucene::core::search::{DefaultIndexSearcher, IndexSearcher};
use rucene::core::store::directory::FSDirectory;

use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::fs::{self, File};
use std::io::{self, BufRead, Error};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{self, AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{cmp, env, ptr, thread};

use rucene::error::Result;

fn indexed_numeric_field_type() -> FieldType {
    let mut field_type = FieldType::default();
    field_type.tokenized = false;
    field_type.doc_values_type = DocValuesType::Null;
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
fn main() -> Result<()> {
    // create index directory
    let path = "/tmp/test_rucene";
    let dir_path = Path::new(path);
    if dir_path.exists() {
        fs::remove_dir_all(&dir_path)?;
        fs::create_dir(&dir_path)?;
    }

    let worker_count = std::env::args()
        .nth(1)
        .ok_or(Error::new(
            std::io::ErrorKind::Other,
            "Missing worker count argument",
        ))?
        .parse::<usize>()?;

    let max_memory_bytes: usize = std::env::args()
        .nth(2)
        .ok_or(Error::new(
            std::io::ErrorKind::Other,
            "Missing memory argument",
        ))?
        .parse::<usize>()? * 1024 * 1024;

    // create index writer
    let config = Arc::new(IndexWriterConfig::default());
    let directory = Arc::new(FSDirectory::with_path(&dir_path)?);
    let writer = IndexWriter::new(directory, config)?;

    let queries = Arc::new(RwLock::new(vec![]));

    if let Ok(mut lines) = read_lines("../range_datapoints") {
        let num_docs: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();
        // Consumes the iterator, returns an (Optional) String

        for n in 0..*num_docs {
            let timestamp: &i64 = &lines.next().unwrap().unwrap().parse().unwrap();
            let numeric_field = new_index_numeric_field("timestamp".into(), *timestamp);
            let mut doc: Vec<Box<dyn Fieldable>> = vec![];
            doc.push(Box::new(numeric_field));

            writer.add_document(doc)?;

            if n > 0 && n % 1000000 == 0 {
                writer.commit()?;
            }
        }
        let num_queries: &i32 = &lines.next().unwrap().unwrap().parse().unwrap();

        for i in 0..*num_queries {
            let l = lines.next().unwrap().unwrap();

            let mut range = l.split(',');

            let lower = range.next().unwrap();

            let lower_bound: i64 = lower.parse::<i64>().unwrap();

            let upper = range.next().unwrap();

            let upper_bound: i64 = upper.parse::<i64>().unwrap();

            queries.write().unwrap().push(LongPoint::new_range_query(
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

    let pool = Arc::new(AtomicI32::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let query_offset = queries.read().unwrap().len() / worker_count;
    let mut sum = 0;

    let stop_clone = stop.clone();

    let mut max_qps = 1;
    let search_count = Arc::new(AtomicUsize::new(0));
    let search_count_time = Arc::new(AtomicUsize::new(0));

    thread::scope(|s| {
        for i in 0..worker_count {
            let stop_clone = stop.clone();
            let pool_clone = pool.clone();
            let searcher_clone = index_searcher.clone();
            let start_pos = query_offset * i;
            let queries_clone = queries.clone();
            let search_count_clone = search_count.clone();
            let search_count_time_clone = search_count_time.clone();

            s.spawn(move || {
                // let mut start_pos_clone = start_pos.clone();
                let mut pos = start_pos.clone();

                while !stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                    let mut wait = true;
                    if pool_clone.load(atomic::Ordering::Acquire) > 0 {
                        if pool_clone.fetch_sub(1, atomic::Ordering::AcqRel) >= 1 {
                            let mut manager = TopDocsCollector::new(10);
                            let query = queries_clone.read().unwrap();
                            let t = query.get(pos);
                            let q = t.unwrap();
                            let r = q.as_ref().unwrap();
                            let start_time: Instant = Instant::now();
                            searcher_clone.search(&**r, &mut manager);
                            let time: Duration = Instant::now().duration_since(start_time);
                            search_count_time_clone.fetch_add(
                                time.as_millis() as usize,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            search_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            pos += 1;
                            // sum += manager.top_docs().total_hits() as i64;
                            if pos >= queries_clone.read().unwrap().len() {
                                pos = 0;
                            }
                            wait = false;
                        } else {
                            pool_clone.fetch_add(1, atomic::Ordering::AcqRel);
                        }
                    }
                    if wait {
                        thread::park_timeout(Duration::from_millis(50));
                    }
                }
            });
        }

        let mut current_qps = 500;
        let mut delta_qps = 500;
        let mut found_target = 0;

        pool.store(current_qps, atomic::Ordering::Release);

        thread::sleep(Duration::from_secs(1));

        // let max_memory_bytes: usize = 1024 * 1024 * 2;

        let bytes_per_entry: usize = 1024;
        let new_stop = stop.clone();
        s.spawn(move || {
            let mut short_lived_objects: Vec<*mut u8> = Vec::new();
            let mut long_lived_objects: Vec<*mut u8> = Vec::new();
            let mut current_cycle_number: usize = 0;
            while !new_stop.load(std::sync::atomic::Ordering::Acquire) {
                println!("allocating mem");
                let num_entries = max_memory_bytes / bytes_per_entry;
                if !short_lived_objects.is_empty() {
                    for ptr in short_lived_objects.drain(..) {
                        unsafe {
                            dealloc(ptr, Layout::from_size_align_unchecked(bytes_per_entry, 1));
                        }
                    }
                } else {
                    for _ in 0..num_entries {
                        let ptr =
                            unsafe { alloc(Layout::from_size_align_unchecked(bytes_per_entry, 1)) };
                        if !ptr.is_null() {
                            unsafe { ptr::write_bytes(ptr, 0, bytes_per_entry) };
                            short_lived_objects.push(ptr);
                        }
                    }
                }
                if current_cycle_number % 5000 == 0 {
                    for ptr in long_lived_objects.drain(..) {
                        unsafe {
                            dealloc(ptr, Layout::from_size_align_unchecked(bytes_per_entry, 1));
                        }
                    }
                    println!("Allocating old gen");
                    for _ in 0..(num_entries / 4) {
                        let ptr =
                            unsafe { alloc(Layout::from_size_align_unchecked(bytes_per_entry, 1)) };
                        if !ptr.is_null() {
                            unsafe { ptr::write_bytes(ptr, 0, bytes_per_entry) };
                            long_lived_objects.push(ptr);
                        }
                    }
                }

                current_cycle_number += 1;
                thread::sleep(Duration::from_secs(1));
            }
        });

        loop {
            println!("pool size, {}", pool.load(atomic::Ordering::Acquire));
            if pool.load(atomic::Ordering::Acquire) <= 0 {
                current_qps += delta_qps;
                println!("Increasing QPS to {}", current_qps);
            } else {
                delta_qps /= 2;
                if delta_qps == 0 {
                    found_target += 1;
                    println!("found target, {}", found_target);
                    if found_target >= 5 {
                        max_qps = current_qps;
                        break;
                    }
                    delta_qps = 1;
                }
                current_qps -= delta_qps;
                println!("Decreasing QPS to {}", current_qps);
            }
            pool.store(current_qps, atomic::Ordering::Release);
            thread::sleep(Duration::from_secs(1));
        }
        stop_clone.store(true, std::sync::atomic::Ordering::Release);
    });

    println!("Maximum QPS: {}", max_qps);

    println!(
        "QPS computed: {}",
        ((search_count.load(Ordering::Relaxed) * 1000) / search_count_time.load(Ordering::Relaxed))
            * worker_count
    );

    Ok(())
}
