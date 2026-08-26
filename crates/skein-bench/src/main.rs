use skein_bench::run_comparison_benchmark;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running Skein Bandwidth & Latency Benchmarks (Real Video vs Naive)...\n");

    let videos = vec![
        PathBuf::from("/home/aks/vs_stuff/Development/rust_devs/skein/video/spiderman.mp4"),
        PathBuf::from("/home/aks/vs_stuff/Development/rust_devs/skein/video/vegeta.mp4"),
        PathBuf::from("/home/aks/vs_stuff/Development/rust_devs/skein/video/beauty.mp4"),
        PathBuf::from("/home/aks/vs_stuff/Development/rust_devs/skein/video/marvel.mp4"),
    ];

    for video_path in videos {
        if video_path.exists() {
            match run_comparison_benchmark(&video_path, 1024 * 1024).await {
                Ok(report) => {
                    println!("{}", report.display_summary());
                }
                Err(e) => {
                    eprintln!("Error benchmarking {:?}: {}", video_path, e);
                }
            }
        }
    }

    Ok(())
}
