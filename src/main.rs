use serde::Deserialize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;
use std::sync::Arc;
use structopt::StructOpt;
use std::path::PathBuf;
use rayon::prelude::*;
use std::net::{TcpStream, SocketAddr, UdpSocket, Ipv4Addr};
use std::io::Write;
use prettytable::{Attr, Table, Cell, Row, row, cell, color};
use serde::export::Formatter;

#[derive(Debug, StructOpt)]
#[structopt(name = "port-tester", about = "Oblakogroup port tester")]
struct Opt {
    /// manifest url
    #[structopt(short, long)]
    manifest_url: String,
    /// number of threads
    #[structopt(short, long)]
    jobs: Option<usize>,
    /// output
    #[structopt(parse(from_os_str))]
    out: Option<PathBuf>,
    /// disables colors in stdout
    #[structopt(long)]
    disable_colors: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    domains: Vec<DomainEntry>
}

#[derive(Debug, Deserialize)]
struct DomainEntry {
    url: String,
    address: String,
}

#[derive(Deserialize, Clone, Default)]
struct PortRange {
    min: u16,
    max: u16,
}

#[derive(Deserialize, Clone)]
struct PortList {
    port_range: PortRange,
    port_list: Vec<u16>,
}

struct DomainCheckReport {
    url: String,
    status: DomainCheckStatus,
    port_range: PortRange,
}

enum DomainCheckStatus {
    Alive {
        port_results: Vec<PortCheckResult>
    },
    Dead,
}

struct PortCheckResult {
    port: u16,
    protocol: PortCheckProtocol,
    status: PortCheckStatus,
}

enum PortCheckProtocol {
    TCP,
    UDP,
}

impl std::fmt::Display for PortCheckProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TCP => f.write_str("TCP"),
            Self::UDP => f.write_str("UDP")
        }
    }
}

#[derive(Debug)]
enum PortCheckStatus {
    Success,
    Error(PortCheckError),
}

#[derive(Debug)]
enum PortCheckError {
    IoError(std::io::Error),
    WrongMessage(String),
}

static MSG: [u8; 8] = [1, 7, 12, 74, 123, 96, 255, 11];

fn main() {
    let conf: Opt = Opt::from_args();

    if !conf.manifest_url.starts_with("http://") {
        eprintln!("Not found http scheme in manifest url, please include it!");
        std::process::exit(-1);
    }

    let resp = ureq::get(&conf.manifest_url).build().call();
    if resp.error() {
        eprintln!("Manifest fetching error! If endpoint is down contact Oblakogroup");
        std::process::exit(-1);
    }

    let remote_conf: Config = serde_json::from_reader(resp.into_reader())
        .expect("Manifest parsing error! Contact Oblakogroup");

    let m = Arc::new(MultiProgress::new());
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    rayon::ThreadPoolBuilder::new()
        .num_threads(conf.jobs.unwrap_or_else(|| num_cpus::get() * 2))
        .build_global()
        .unwrap();

    // Parallel processing through rayon
    let entries: Vec<_> = remote_conf.domains.par_iter().map(|entry: &DomainEntry| {
        let address = entry.address.clone();
        match serde_json::from_reader::<_, PortList>(
            ureq::get(&format!("http://{}:1555", address)).call().into_reader()
        ) {
            Ok(pl) => {
                let pb = m.add(ProgressBar::new(pl.port_list.len() as u64 * 2));
                pb.set_style(sty.clone());
                pb.set_message(&entry.url);
                (entry.url.clone(), address, Some(pl), pb)
            }
            Err(_) => {
                let pb = m.add(ProgressBar::new(0));
                pb.set_style(sty.clone());
                pb.set_message(format!("{} (dead)", entry.url).as_str());
                (entry.url.clone(), address, None, pb)
            }
        }
    }).collect::<Vec<_>>();

    // Because we need to call MultiProgress::join to start render which blocks current thread
    // but rayon blocks main thread too, so we start separate render thread for progress bars.
    let m_clone = m.clone();
    let progress_thread = std::thread::spawn(move || {
        m_clone.join().unwrap();
    });

    let reports: Vec<DomainCheckReport> = entries.into_par_iter().map(|v: (String, String, Option<PortList>, ProgressBar)| {
        let (domain, address, pl, pb) = v;
        if let Some(pl) = pl {
            pb.reset_elapsed();
            pb.tick();
            let sock_addr = SocketAddr::from((address.parse::<Ipv4Addr>().unwrap(), 0));

            let results: Vec<PortCheckResult> = pl.port_list.par_iter().flat_map(|port| {
                let port = *port;
                let mut addr = sock_addr.clone();
                addr.set_port(port);

                let res_udp = test_udp(addr.clone());
                pb.inc(1);

                let res_tcp = match test_tcp(addr) {
                    Ok(_) => PortCheckResult {
                        port,
                        protocol: PortCheckProtocol::TCP,
                        status: PortCheckStatus::Success,
                    },
                    Err(e) => PortCheckResult {
                        port,
                        protocol: PortCheckProtocol::TCP,
                        status: PortCheckStatus::Error(e),
                    }
                };
                pb.inc(1);

                vec![res_udp, res_tcp]
            }).collect();

            pb.finish();

            DomainCheckReport {
                url: domain,
                status: DomainCheckStatus::Alive {
                    port_results: results
                },
                port_range: pl.port_range,
            }
        } else {
            pb.finish();
            DomainCheckReport {
                url: domain,
                status: DomainCheckStatus::Dead,
                port_range: PortRange::default(),
            }
        }
    }).collect();
    progress_thread.join().unwrap();

    // Fancy output
    let mut table = Table::new();
    table.add_row(row![Fw => "Port", "Protocol", "Status", "Error"]);
    for domain_report in reports {
        let mut rows = vec![];
        let mut accessible = 0;
        if let DomainCheckStatus::Alive { ref port_results } = domain_report.status {
            for port_result in port_results.iter() {
                match port_result.status {
                    PortCheckStatus::Success => {
                        accessible += 1;
                    }
                    PortCheckStatus::Error(ref e) => {
                        rows.push(row![Fw =>
                            format!("{}", port_result.port),
                            format!("{}", port_result.protocol),
                            "inaccessible",
                            textwrap::fill(&format!("{:#?}", e), 80)
                        ]);
                    }
                }
            }
        }

        match domain_report.status {
            DomainCheckStatus::Alive { ref port_results } => {
                let mut cell = Cell::new(
                    &format!("{} ({} out of {} ports from the range {}:{} are accessible)",
                             domain_report.url,
                             accessible,
                             port_results.len(),
                             domain_report.port_range.min,
                             domain_report.port_range.max
                    )
                ).with_style(Attr::ForegroundColor(color::WHITE))
                    .with_hspan(4);
                if accessible == port_results.len() && !conf.disable_colors {
                    cell = cell.with_style(Attr::BackgroundColor(color::GREEN));
                }
                table.add_row(Row::new(vec![cell]));
            }
            DomainCheckStatus::Dead => {
                let mut cell = Cell::new(&format!("{} (dead)", domain_report.url))
                    .with_style(Attr::ForegroundColor(color::WHITE))
                    .with_hspan(4);
                if !conf.disable_colors {
                    cell = cell.with_style(Attr::BackgroundColor(color::RED))
                }
                table.add_row(Row::new(vec![cell]));
            }
        };

        for r in rows {
            table.add_row(r);
        }
    }
    table.printstd();

    if let Some(path) = conf.out {
        table.row_iter_mut().for_each(|r| r.iter_mut().for_each(|c| c.reset_style()));

        let mut out_file = std::fs::File::create(path).unwrap();
        table.print(&mut out_file).unwrap();
    }
    print!("Enter something to exit...");
    std::io::stdout().flush().unwrap();
    let mut s = String::new();
    std::io::stdin().read_line(&mut s).unwrap();
}

fn test_udp(addr: SocketAddr) -> PortCheckResult {
    let port = addr.port();
    let sock = match UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))) {
        Ok(sock) => sock,
        Err(e) => return PortCheckResult {
            port,
            protocol: PortCheckProtocol::UDP,
            status: PortCheckStatus::Error(PortCheckError::IoError(e)),
        }
    };

    if let Err(e) = sock.connect(addr) {
        return PortCheckResult {
            port,
            protocol: PortCheckProtocol::UDP,
            status: PortCheckStatus::Error(PortCheckError::IoError(e)),
        };
    }

    let mut error_sending = false;
    let mut error_receiving = false;
    let mut curr_res: PortCheckStatus = PortCheckStatus::Success;
    let mut buf = [0u8; 65536];

    // Quickly sending N messages (for potential packet loss), after waiting some time (for potential ping)
    sock.set_read_timeout(Some(Duration::from_millis(50))).unwrap();

    for _ in 0..8 {
        match sock.send(&MSG) {
            Ok(_) => {
                error_sending = false;
                match sock.recv(&mut buf) {
                    Ok(n) => {
                        error_receiving = false;
                        if MSG == buf[0..n] {
                            curr_res = PortCheckStatus::Success;
                            break;
                        } else {
                            curr_res = PortCheckStatus::Error(PortCheckError::WrongMessage(
                                if n <= 32 {
                                    format!("Expected {:x?} but got {:x?}", &MSG, &buf[0..n])
                                } else {
                                    format!("Expected {:x?} but got datagram with len {} where first 32 bytes is {:x?}", &MSG, n, &buf[0..32])
                                }
                            ));
                        }
                    }
                    Err(e) => {
                        error_receiving = true;
                        curr_res = PortCheckStatus::Error(PortCheckError::IoError(e));
                    }
                };
            }
            Err(e) => {
                error_sending = true;
                curr_res = PortCheckStatus::Error(PortCheckError::IoError(e));
            }
        };
    }

    if !error_sending && error_receiving {
        sock.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
        match sock.recv(&mut buf) {
            Ok(n) => {
                if MSG == buf[0..n] {
                    curr_res = PortCheckStatus::Success;
                } else {
                    curr_res = PortCheckStatus::Error(PortCheckError::WrongMessage(
                        if n <= 32 {
                            format!("Expected {:?} but got {:?}", &MSG, &buf[0..n])
                        } else {
                            format!("Expected {:?} but got datagram with len {} first 32 bytes: {:?}", &MSG, n, &buf[0..32])
                        }
                    ))
                }
            }
            Err(e) => curr_res = PortCheckStatus::Error(PortCheckError::IoError(e))
        }
    }

    PortCheckResult {
        port,
        protocol: PortCheckProtocol::UDP,
        status: curr_res,
    }
}

fn test_tcp(addr: SocketAddr) -> Result<(), PortCheckError> {
    TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| PortCheckError::IoError(e))
        ?;

    Ok(())
}