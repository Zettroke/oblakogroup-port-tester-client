use serde::Deserialize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;
use std::path::PathBuf;
use rayon::prelude::*;
use std::net::{SocketAddr, UdpSocket, Ipv4Addr};
use std::io::{Read, Write};
use prettytable::{Attr, Table, Cell, Row, row, cell, color};
use std::error::Error;

#[derive(Debug, StructOpt)]
#[structopt(name = "port-tester", about = "An example of StructOpt usage.")]
struct Opt {
    /// manifest url
    #[structopt(short, long)]
    manifest: String,
    /// number of threads
    #[structopt(short, long)]
    jobs: Option<usize>,
    /// output
    #[structopt(parse(from_os_str))]
    out: Option<PathBuf>,
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
    status: PortCheckStatus,
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

    let resp = ureq::get(&conf.manifest).build().call();

    let remote_conf: Config = serde_json::from_reader(resp.into_reader()).unwrap();
    let m = Arc::new(MultiProgress::new());
    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    rayon::ThreadPoolBuilder::new()
        .num_threads(conf.jobs.unwrap_or_else(|| num_cpus::get()))
        .build_global().unwrap();

    let entries: Vec<_> = remote_conf.domains.par_iter().map(|entry: &DomainEntry| {
        let address = entry.address.clone();
        match serde_json::from_reader::<_, PortList>(
            ureq::get(&format!("http://{}:1555", address)).call().into_reader()
        ) {
            Ok(pl) => {
                let pb = m.add(ProgressBar::new(pl.port_list.len() as u64));
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

            let results: Vec<PortCheckResult> = pl.port_list.par_iter().map(|port| {
                let mut sock = match std::net::UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))) {
                    Ok(sock) => sock,
                    Err(e) => return PortCheckResult { port: *port, status: PortCheckStatus::Error(PortCheckError::IoError(e)) }
                };
                let mut addr = sock_addr.clone();
                addr.set_port(*port);
                if let Err(e) = sock.connect(addr) {
                    return PortCheckResult { port: *port, status: PortCheckStatus::Error(PortCheckError::IoError(e)) };
                }
                let res = check_connection(&mut sock, *port);
                pb.inc(1);

                res
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

    let mut table = Table::new();
    table.add_row(row!["Port", "Status", "Error"]);
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
                        let mut err = format!("{:#?}", e);
                        string_multiline(&mut err);
                        rows.push(row![format!("{}", port_result.port), "inaccessible", err]);
                    }
                }
            }
            // println!("  {} out of {} ports accessible", accessible, port_results.len());
        }

        match domain_report.status {
            DomainCheckStatus::Alive { ref port_results } => {
                let mut cell = Cell::new(
                    &format!("{} ({} out of {} ports accessible from port range {}:{})",
                             domain_report.url,
                             accessible,
                             port_results.len(),
                             domain_report.port_range.min,
                             domain_report.port_range.max
                    )
                ).with_hspan(3);
                if accessible == port_results.len() {
                    cell = cell.with_style(Attr::BackgroundColor(color::GREEN));
                }
                table.add_row(Row::new(vec![cell]));
            }
            DomainCheckStatus::Dead => {
                table.add_row(
                    Row::new(vec![
                        Cell::new(&format!("{} (dead)", domain_report.url))
                            .with_style(Attr::BackgroundColor(color::RED))
                            .with_hspan(3)
                    ])
                );
            }
        };

        for r in rows {
            table.add_row(r);
        }
    }
    table.printstd();

    table.row_iter_mut().for_each(|r| r.iter_mut().for_each(|c| c.reset_style()));
    if let Some(path) = conf.out {
        let mut out_file = std::fs::File::create(path).unwrap();
        table.print(&mut out_file);
    }
    print!("Enter something to exit...");
    std::io::stdout().flush();
    let mut s = String::new();
    std::io::stdin().read_line(&mut s);
}

fn check_connection(sock: &mut UdpSocket, port: u16) -> PortCheckResult {
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

    PortCheckResult { port, status: curr_res }
}

fn string_multiline(s: &mut String) {
    let mut line_len = 0;
    let mut i = 0;
    let mut last_space_ind = 0;
    while i < s.len() {
        if s.as_bytes().get(i).map_or(false, |v| *v == b'\n' || *v == b'\r') {
            line_len = 0;
            last_space_ind = 0;
        } else {
            line_len += 1;
            if line_len > 100 && last_space_ind != 0 {
                s.insert(last_space_ind, '\n');
                line_len = 0;
                last_space_ind = 0;
            }
        }
        if s.as_bytes().get(i).map_or(false, |v| *v == b' ') {
            last_space_ind = i;
        }
        i += 1;
    }
}
