// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

use std::path::PathBuf;

/// Derives the tidesdb version from enabled `v*` Cargo features.
/// Feature `v9_0_5` maps to version `9.0.5`.
fn selected_version() -> String {
    let mut selected: Option<String> = None;
    for (key, _) in std::env::vars() {
        if let Some(feature) = key.strip_prefix("CARGO_FEATURE_V") {
            let version = feature.to_lowercase().replace('_', ".");
            if selected.is_some() {
                panic!("Multiple tidesdb version features enabled. Select exactly one.");
            }
            selected = Some(version);
        }
    }
    selected.unwrap_or_else(|| "9.0.5".to_string())
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

fn with_objectstore() -> bool {
    std::env::var("CARGO_FEATURE_OBJECTSTORE").is_ok()
}

fn build_from_source(version: &str) -> PathBuf {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let src_dir = download_and_extract(version, &out_dir);

    let mut cfg = cmake::Config::new(&src_dir);
    cfg.define("TIDESDB_BUILD_TESTS", "OFF")
        .define("BUILD_SHARED_LIBS", "OFF");

    if with_objectstore() {
        cfg.define("TIDESDB_WITH_S3", "ON");
    }

    cfg.build()
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
        .exactly_version(&version)
        .probe("tidesdb")
        .is_ok()
    {
        return;
    }

    // Build from source
    let dst = build_from_source(&version);

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
    // Link S3/object store dependencies (libcurl + openssl)
    if with_objectstore() {
        if pkg_config::probe_library("libcurl").is_err() {
            println!("cargo:rustc-link-lib=curl");
            missing.push("curl");
        }
        if pkg_config::probe_library("openssl").is_err() {
            println!("cargo:rustc-link-lib=ssl");
            println!("cargo:rustc-link-lib=crypto");
            missing.push("ssl/crypto");
        }
    }

    if !missing.is_empty() {
        let mut msg = format!(
            "cargo:warning=Could not find {} via pkg-config, falling back to link by name. \
             If linking fails, install them:\n\
             \x20 Debian/Ubuntu: sudo apt install libzstd-dev liblz4-dev libsnappy-dev",
            missing.join(", ")
        );
        if with_objectstore() {
            msg.push_str(" libcurl4-openssl-dev libssl-dev");
        }
        msg.push_str("\n\x20 macOS:         brew install zstd lz4 snappy");
        if with_objectstore() {
            msg.push_str(" curl openssl");
        }
        msg.push_str("\n\x20 Windows:       vcpkg install zstd:x64-windows lz4:x64-windows snappy:x64-windows");
        if with_objectstore() {
            msg.push_str(" curl:x64-windows openssl:x64-windows");
        }
        println!("{msg}");
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
