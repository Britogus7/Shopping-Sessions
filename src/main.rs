mod csv;
mod links;
mod pipeline;

use std::env;
use std::path::PathBuf;

fn main() {
    if let Ok(ncpu_str) = env::var("NCPU") {
        if let Ok(ncpu) = ncpu_str.parse::<usize>() {
            if ncpu > 0 {
                let _ = rayon::ThreadPoolBuilder::new()
                    .num_threads(ncpu)
                    .build_global();
            }
        }
    }

    let args: Vec<String> = env::args().skip(1).collect();
    let data_dir = if args.len() >= 2 {
        PathBuf::from(&args[0])
    } else {
        PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string()))
    };
    let out_dir = if args.len() >= 2 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string()))
    };

    pipeline::run_pipeline(&data_dir, &out_dir);
}
