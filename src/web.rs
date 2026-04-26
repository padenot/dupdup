use get_if_addrs::{get_if_addrs, IfAddr};
use std::io::{Read, Seek, SeekFrom};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::thread::JoinHandle;
use tiny_http::{Header, Response, Server};

const INDEX_HTML: &[u8] = include_bytes!("../index.html");

fn header(name: &[u8], value: &[u8]) -> Option<Header> {
    Header::from_bytes(name, value).ok()
}

fn is_tailscale_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000
}

fn is_private_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_private()
}

fn is_link_local_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_link_local()
}

fn best_ipv4() -> Option<Ipv4Addr> {
    let mut best: Option<(u8, Ipv4Addr)> = None;
    let addrs = get_if_addrs().ok()?;
    for iface in addrs {
        let (name, ip) = match iface.addr {
            IfAddr::V4(v4) => (iface.name, v4.ip),
            _ => continue,
        };
        if ip.is_loopback() || ip.is_unspecified() || is_link_local_ipv4(ip) {
            continue;
        }
        let mut priority = 2u8;
        if is_tailscale_ipv4(ip) || name.to_lowercase().contains("tailscale") {
            priority = 0;
        } else if is_private_ipv4(ip) {
            priority = 1;
        }
        match best {
            Some((p, _)) if p <= priority => {}
            _ => best = Some((priority, ip)),
        }
    }
    best.map(|(_, ip)| ip)
}

pub(crate) fn best_ui_url(port: u16) -> String {
    if let Some(ip) = best_ipv4() {
        format!("http://{}:{}/", ip, port)
    } else {
        format!("http://127.0.0.1:{}/", port)
    }
}

pub(crate) fn open_http_ui(url: &str) {
    let _ = open::that(url);
}

pub(crate) fn serve_http(report_path: PathBuf, port: u16) -> Option<JoinHandle<()>> {
    match Server::http(format!("0.0.0.0:{}", port)) {
        Ok(server) => {
            println!("Serving UI on 0.0.0.0:{}", port);
            let handle = std::thread::spawn(move || {
                let report_path_clone = report_path.clone();
                for mut request in server.incoming_requests() {
                    let url = request.url();
                    let (path, query) = url.split_once('?').unwrap_or((url, ""));
                    if path == "/" || path.starts_with("/index") {
                        let mut response = Response::from_data(INDEX_HTML);
                        if let Some(content_type) = header(b"Content-Type", b"text/html") {
                            response = response.with_header(content_type);
                        }
                        let _ = request.respond(response);
                        continue;
                    }
                    if path.starts_with("/report") {
                        let offset = query.split('&').find_map(|kv| kv.split_once('=')).and_then(
                            |(k, v)| {
                                if k == "offset" {
                                    v.parse::<u64>().ok()
                                } else {
                                    None
                                }
                            },
                        );
                        let bytes = if let Some(offset) = offset {
                            match std::fs::File::open(&report_path_clone) {
                                Ok(mut file) => {
                                    let _ = file.seek(SeekFrom::Start(offset));
                                    let mut buf = Vec::new();
                                    if file.read_to_end(&mut buf).is_ok() {
                                        Some(buf)
                                    } else {
                                        None
                                    }
                                }
                                Err(_) => None,
                            }
                        } else {
                            std::fs::read(&report_path_clone).ok()
                        };
                        if let Some(bytes) = bytes {
                            let mut resp = Response::from_data(bytes);
                            if let Some(content_type) = header(b"Content-Type", b"application/json")
                            {
                                resp = resp.with_header(content_type);
                            }
                            if let Some(cache_control) = header(b"Cache-Control", b"no-store") {
                                let _ = resp.add_header(cache_control);
                            }
                            let _ = request.respond(resp);
                        } else {
                            let _ = request.respond(
                                Response::from_string("report not found").with_status_code(404),
                            );
                        }
                        continue;
                    }
                    if path.starts_with("/delete") {
                        let mut body = String::new();
                        let _ = request.as_reader().read_to_string(&mut body);
                        let mut deleted = 0;
                        let mut failed = Vec::new();
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            if let Some(arr) = json.as_array() {
                                for p in arr.iter().filter_map(|v| v.as_str()) {
                                    match std::fs::remove_file(p) {
                                        Ok(_) => deleted += 1,
                                        Err(e) => failed.push(format!("{}: {}", p, e)),
                                    }
                                }
                            }
                        }
                        let resp = serde_json::json!({ "deleted": deleted, "failed": failed });
                        let mut response = Response::from_data(resp.to_string());
                        if let Some(content_type) = header(b"Content-Type", b"application/json") {
                            response = response.with_header(content_type);
                        }
                        let _ = request.respond(response);
                        continue;
                    }
                    if path.starts_with("/reveal") {
                        let mut body = String::new();
                        let _ = request.as_reader().read_to_string(&mut body);
                        let path_str = serde_json::from_str::<serde_json::Value>(&body)
                            .ok()
                            .and_then(|v| {
                                if let Some(s) = v.as_str() {
                                    Some(s.to_string())
                                } else {
                                    v.get("path")
                                        .and_then(|p| p.as_str())
                                        .map(|s| s.to_string())
                                }
                            });
                        if let Some(p) = path_str {
                            let target = std::path::Path::new(&p)
                                .parent()
                                .map(|p| p.to_path_buf())
                                .unwrap_or_else(|| PathBuf::from(&p));
                            let _ = open::that(target);
                            let mut response = Response::from_data("{\"status\":\"ok\"}");
                            if let Some(content_type) = header(b"Content-Type", b"application/json")
                            {
                                response = response.with_header(content_type);
                            }
                            let _ = request.respond(response);
                        } else {
                            let _ = request.respond(
                                Response::from_string("bad request").with_status_code(400),
                            );
                        }
                        continue;
                    }
                    let _ =
                        request.respond(Response::from_string("not found").with_status_code(404));
                }
            });
            Some(handle)
        }
        Err(e) => {
            eprintln!("HTTP server failed: {}", e);
            None
        }
    }
}
