// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

use std::path::PathBuf;

const VERSION_FEATURES: &[(&str, &str)] = &[
    ("v9_0_0", "9.0.0"),
    ("v9_0_1", "9.0.1"),
    ("v9_0_2", "9.0.2"),
    ("v9_0_3", "9.0.3"),
    ("v9_0_4", "9.0.4"),
    ("v9_0_5", "9.0.5"),
];

fn selected_version() -> &'static str {
    let mut selected: Option<&str> = None;
    for &(feature, version) in VERSION_FEATURES {
        if std::env::var(format!("CARGO_FEATURE_{}", feature.to_uppercase())).is_ok() {
            if selected.is_some() {
                panic!(
                    "Multiple tidesdb version features enabled. Select exactly one of: {}",
                    VERSION_FEATURES
                        .iter()
                        .map(|(f, _)| *f)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            selected = Some(version);
        }
    }
    selected.unwrap_or("9.0.5")
}

fn download_and_extract(version: &str, out_dir: &str) -> PathBuf {
    let url = format!(
        "https://github.com/tidesdb/tidesdb/archive/refs/tags/v{version}.tar.gz"
    );
    let tarball_path = PathBuf::from(out_dir).join(format!("tidesdb-{version}.tar.gz"));
    let extract_dir = PathBuf::from(out_dir).join("tidesdb-source");

    // Skip download if already extracted
    let src_dir = extract_dir.join(format!("tidesdb-{version}"));
    if src_dir.exists() {
        return src_dir;
    }

    // Download
    let resp = ureq::get(&url).call().unwrap_or_else(|e| {
        panic!("Failed to download tidesdb v{version} from {url}: {e}")
    });
    let mut tarball = std::fs::File::create(&tarball_path)
        .expect("Failed to create tarball file");
    std::io::copy(&mut resp.into_reader(), &mut tarball)
        .expect("Failed to write tarball");

    // Extract
    let tarball = std::fs::File::open(&tarball_path).expect("Failed to open tarball");
    let decoder = flate2::read::GzDecoder::new(tarball);
    let mut archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(&extract_dir).expect("Failed to create extract directory");
    archive
        .unpack(&extract_dir)
        .expect("Failed to extract tarball");

    // Clean up tarball
    let _ = std::fs::remove_file(&tarball_path);

    src_dir
}

fn build_from_source(version: &str) -> PathBuf {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let src_dir = download_and_extract(version, &out_dir);

    cmake::Config::new(&src_dir)
        .define("TIDESDB_BUILD_TESTS", "OFF")
        .define("BUILD_SHARED_LIBS", "OFF")
        .build()
}

fn brew_prefix() -> Option<String> {
    std::process::Command::new("brew")
        .arg("--prefix")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn main() {
    let version = selected_version();

    // Try pkg-config with exact version match
    if pkg_config::Config::new()
        .exactly_version(version)
        .probe("tidesdb")
        .is_ok()
    {
        return;
    }

    // Build from source
    let dst = build_from_source(version);

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=tidesdb");

    // Link compression dependencies via pkg-config (handles search paths)
    let mut missing = Vec::new();
    for dep in &["libzstd", "liblz4", "snappy"] {
        if pkg_config::probe_library(dep).is_err() {
            let lib_name = dep.strip_prefix("lib").unwrap_or(dep);
            println!("cargo:rustc-link-lib={lib_name}");
            missing.push(lib_name);
        }
    }
    if !missing.is_empty() {
        println!(
            "cargo:warning=Could not find {} via pkg-config, falling back to link by name. \
             If linking fails, install them:\n\
             \x20 Debian/Ubuntu: sudo apt install libzstd-dev liblz4-dev libsnappy-dev\n\
             \x20 macOS:         brew install zstd lz4 snappy\n\
             \x20 Windows:       vcpkg install zstd:x64-windows lz4:x64-windows snappy:x64-windows",
            missing.join(", ")
        );
    }

    // Platform-specific dependencies
    #[cfg(unix)]
    {
        println!("cargo:rustc-link-lib=pthread");
    }

    // Add library search paths
    if let Some(prefix) = brew_prefix() {
        println!("cargo:rustc-link-search=native={prefix}/lib");
    } else {
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-search=native=/usr/lib");
        #[cfg(target_os = "macos")]
        {
            println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
        }
        #[cfg(target_os = "windows")]
        {
            println!("cargo:rustc-link-search=native=C:/msys64/mingw64/lib");
        }
    }
}
