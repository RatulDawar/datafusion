// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream_partitioned, ExecutionPlan, ExecutionPlanProperties,
};
use datafusion::prelude::*;
use futures::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Debug)]
struct PollEvent {
    partition: usize,
    thread_name: String,
    thread_id: u64,
    elapsed_us: u128,
}

fn thread_id_to_u64(tid: std::thread::ThreadId) -> u64 {
    let s = format!("{tid:?}");
    s.chars().filter(|c| c.is_ascii_digit()).collect::<String>().parse().unwrap_or(0)
}

fn wrap_stream(
    partition: usize,
    schema: SchemaRef,
    mut inner: SendableRecordBatchStream,
    log: Arc<Mutex<Vec<PollEvent>>>,
    start: Instant,
) -> SendableRecordBatchStream {
    let stream = async_stream::stream! {
        while let Some(batch) = inner.next().await {
            let thread = std::thread::current();
            let tid = thread_id_to_u64(thread.id());
            let tname = thread.name().unwrap_or("unnamed").to_string();
            log.lock().unwrap().push(PollEvent {
                partition,
                thread_name: tname,
                thread_id: tid,
                elapsed_us: start.elapsed().as_micros(),
            });
            yield batch;
        }
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new()
        .with_batch_size(1024)
        .with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);

    let sql = "
        SELECT value, value * 2 as doubled, value % 1000 as bucket
        FROM generate_series(1, 2000000) as t(value)
        WHERE value % 3 = 0
    ";

    println!("Planning query...");
    let df = ctx.sql(sql).await?;
    let plan: Arc<dyn ExecutionPlan> = df.create_physical_plan().await?;

    let schema = plan.schema();
    let num_partitions = plan.output_partitioning().partition_count();
    println!("Plan has {num_partitions} partitions. Executing...");

    let task_ctx = ctx.task_ctx();
    let raw_streams = execute_stream_partitioned(plan, task_ctx)?;

    let log: Arc<Mutex<Vec<PollEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();

    let mut handles = Vec::new();
    for (partition, stream) in raw_streams.into_iter().enumerate() {
        let wrapped = wrap_stream(partition, schema.clone(), stream, log.clone(), start);
        let handle = tokio::spawn(async move {
            let mut s = wrapped;
            let mut total_rows = 0usize;
            while let Some(batch) = s.next().await {
                total_rows += batch?.num_rows();
            }
            Ok::<(usize, usize), datafusion::error::DataFusionError>((partition, total_rows))
        });
        handles.push(handle);
    }

    for h in handles {
        let (p, rows) = h.await.unwrap()?;
        println!("Partition {p}: {rows} rows");
    }

    let events = log.lock().unwrap().clone();
    println!("\n=== Captured {} poll events ===", events.len());

    let mut csv = File::create("thread_migration.csv").unwrap();
    writeln!(csv, "partition,thread_id,thread_name,elapsed_us").unwrap();
    for e in &events {
        writeln!(
            csv,
            "{},{},{},{}",
            e.partition, e.thread_id, e.thread_name, e.elapsed_us
        )
        .unwrap();
    }
    println!("Wrote events to thread_migration.csv");

    let mut partition_threads: HashMap<usize, Vec<(u64, String)>> = HashMap::new();
    for e in &events {
        partition_threads
            .entry(e.partition)
            .or_default()
            .push((e.thread_id, e.thread_name.clone()));
    }

    println!("\n=== Thread Migration Report ===");
    println!(
        "{:<10} {:<15} {:<15} {:<50}",
        "Partition", "Total Polls", "Migrations", "Threads Used"
    );
    println!("{}", "-".repeat(90));

    let mut total_migrations = 0usize;
    let mut total_polls = 0usize;
    for partition in 0..num_partitions {
        if let Some(threads) = partition_threads.get(&partition) {
            let polls = threads.len();
            total_polls += polls;
            let mut migrations = 0usize;
            for w in threads.windows(2) {
                if w[0].0 != w[1].0 {
                    migrations += 1;
                }
            }
            total_migrations += migrations;
            let unique: std::collections::BTreeSet<_> = threads.iter().map(|(id, n)| format!("{n}({id})")).collect();
            let unique_str = unique.into_iter().collect::<Vec<_>>().join(", ");
            let truncated = if unique_str.len() > 48 {
                format!("{}...", &unique_str[..45])
            } else {
                unique_str
            };
            println!(
                "{:<10} {:<15} {:<15} {:<50}",
                partition, polls, migrations, truncated
            );
        }
    }
    println!("{}", "-".repeat(90));
    println!(
        "TOTAL: {} polls across all partitions, {} thread migrations detected",
        total_polls, total_migrations
    );
    println!(
        "Migration rate: {:.2}%",
        (total_migrations as f64 / total_polls.max(1) as f64) * 100.0
    );

    Ok(())
}
