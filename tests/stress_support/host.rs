//! Host and environment measurements for stress manifests.
//!
//! Every field is measured independently; an unmeasurable field is `None`
//! (serialized as `null` with the reason recorded by the manifest writer),
//! never silently replaced by `0`.

use std::{
    process::Command,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default)]
pub struct HostInfo {
    pub os: Option<String>,
    pub kernel: Option<String>,
    pub cpu_model: Option<String>,
    pub cpu_count: Option<usize>,
    pub ram_mb: Option<u64>,
    pub fd_limit: Option<u64>,
    pub disk_free_bytes: Option<u64>,
    pub disk_free_reason: Option<String>,
    pub rustc_version: Option<String>,
    pub clock_source: Option<String>,
    pub clock_resolution_ns: Option<u64>,
    pub clock_resolution_reason: Option<String>,
}

impl HostInfo {
    #[must_use]
    pub fn probe() -> Self {
        let os = command("uname", &["-sr"]);
        let kernel = command("uname", &["-r"]);
        let cpu_model =
            command("sysctl", &["-n", "machdep.cpu.brand_string"]).or_else(read_file_cpuinfo);
        let cpu_count = std::thread::available_parallelism()
            .ok()
            .map(std::num::NonZeroUsize::get);
        let ram_mb = command("sysctl", &["-n", "hw.memsize"])
            .and_then(|value| {
                value
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .map(|bytes| bytes / (1024 * 1024))
            })
            .or_else(read_file_meminfo);
        let fd_limit = command_output("sh", &["-c", "ulimit -n"])
            .and_then(|value| value.trim().parse::<u64>().ok());
        let disk_free_bytes =
            command("df", &["-k", "."]).and_then(|output| parse_df_available(&output));
        let disk_free_reason = disk_free_bytes
            .is_none()
            .then(|| "df not parseable on this host".to_owned());
        let rustc_version = command("rustc", &["--version"]);
        let (clock_resolution_ns, clock_resolution_reason) = measure_clock_resolution();
        Self {
            os,
            kernel,
            cpu_model,
            cpu_count,
            ram_mb,
            fd_limit,
            disk_free_bytes,
            disk_free_reason,
            rustc_version,
            clock_source: Some(clock_source()),
            clock_resolution_ns,
            clock_resolution_reason,
        }
    }
}

fn command(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn read_file_cpuinfo() -> Option<String> {
    let text = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    text.lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split(':').nth(1).map(|value| value.trim().to_owned()))
}

fn read_file_meminfo() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    text.lines()
        .find(|line| line.starts_with("MemTotal"))
        .and_then(|line| {
            line.split_whitespace()
                .nth(1)
                .and_then(|value| value.parse::<u64>().ok())
                .map(|kb| kb / 1024)
        })
}

fn parse_df_available(output: &str) -> Option<u64> {
    let line = output.lines().nth(1)?;
    let column = line.split_whitespace().nth(3)?;
    column.parse::<u64>().ok().map(|kb| kb * 1024)
}

fn clock_source() -> String {
    "std::time::Instant (monotonic)".to_owned()
}

/// Empirically measure the smallest observable non-zero step of
/// `Instant::now()` within a bounded measurement budget. The reported value
/// is an upper bound on the clock step; if the clock never advances inside
/// the window the measurement is `None` with a reason — never a fabricated 1.
fn measure_clock_resolution() -> (Option<u64>, Option<String>) {
    const BUDGET: Duration = Duration::from_millis(50);
    const MAX_ITERATIONS: u32 = 4_000_000;
    let mut smallest: Option<Duration> = None;
    let mut previous = Instant::now();
    let deadline = previous + BUDGET;
    let mut iterations = 0u32;
    while iterations < MAX_ITERATIONS {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let delta = now.duration_since(previous);
        previous = now;
        if delta > Duration::ZERO {
            smallest = Some(match smallest {
                Some(current) if current <= delta => current,
                _ => delta,
            });
        }
        iterations += 1;
    }
    match smallest {
        Some(resolution) => (
            Some(u64::try_from(resolution.as_nanos()).unwrap_or(u64::MAX)),
            None,
        ),
        None => (
            None,
            Some("clock did not advance during the measurement window".to_owned()),
        ),
    }
}
