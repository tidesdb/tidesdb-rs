// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

use std::path::PathBuf;

/// Derives the tidesdb version from enabled `v*` Cargo features.
/// Feature `v9_0_5` maps to version `9.0.5`. When multiple version features
/// are enabled (e.g. via dependency unification), the highest is selected.
fn selected_version() -> String {
    let mut selected: Option<(u32, u32, u32)> = None;
    for (key, _) in std::env::vars() {
        if let Some(feature) = key.strip_prefix("CARGO_FEATURE_V") {
            let parts: Vec<u32> = feature.split('_').filter_map(|s| s.parse().ok()).collect();
            if let [a, b, c] = parts.as_slice() {
                let v = (*a, *b, *c);
                selected = Some(match selected {
                    Some(prev) if prev >= v => prev,
                    _ => v,
                });
            }
        }
    }
    match selected {
        Some((a, b, c)) => format!("{a}.{b}.{c}"),
        None => "9.3.6".to_string(),
    }
}

fn parse_version(version: &str) -> Option<(u32, u32, u32)> {
    let parts: Vec<u32> = version.split('.').filter_map(|s| s.parse().ok()).collect();
    match parts.as_slice() {
        [a, b, c] => Some((*a, *b, *c)),
        _ => None,
    }
}

fn version_at_least(version: &str, major: u32, minor: u32, patch: u32) -> bool {
    parse_version(version).is_some_and(|v| v >= (major, minor, patch))
}

// BEGIN GENERATED TIDESDB SOURCE ARCHIVES
cfg_if::cfg_if! {
    if #[cfg(feature = "v9_3_6")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_6::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_5")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_5::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_4")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_4::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_3")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_3::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_2")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_2::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_1")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_1::archive_path()
        }
    }
    else if #[cfg(feature = "v9_3_0")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_3_0::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_5")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_5::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_4")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_4::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_3")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_3::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_2")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_2::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_1")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_1::archive_path()
        }
    }
    else if #[cfg(feature = "v9_2_0")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_2_0::archive_path()
        }
    }
    else if #[cfg(feature = "v9_1_0")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_1_0::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_9")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_9::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_8")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_8::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_7")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_7::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_6")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_6::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_5")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_5::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_4")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_4::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_3")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_3::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_2")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_2::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_1")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_1::archive_path()
        }
    }
    else if #[cfg(feature = "v9_0_0")] {
        fn source_archive() -> PathBuf {
            tidesdb_src_v9_0_0::archive_path()
        }
    }
    else {
        fn source_archive() -> PathBuf {
            panic!("no tidesdb source crate is enabled; enable a vX_Y_Z feature")
        }
    }
}

// END GENERATED TIDESDB SOURCE ARCHIVES

fn extract_archive(version: &str, out_dir: &str) -> PathBuf {
    let archive_path = source_archive();
    let extract_dir = PathBuf::from(out_dir).join("tidesdb-source");
    let src_dir = extract_dir.join(format!("tidesdb-{version}"));

    if src_dir.exists() {
        return src_dir;
    }

    let tarball = std::fs::File::open(&archive_path).unwrap_or_else(|e| {
        panic!(
            "failed to open TidesDB v{version} source archive {}: {e}",
            archive_path.display()
        )
    });
    let decoder = flate2::read::GzDecoder::new(tarball);
    let mut archive = tar::Archive::new(decoder);
    std::fs::create_dir_all(&extract_dir).expect("Failed to create extract directory");
    archive
        .unpack(&extract_dir)
        .expect("Failed to extract TidesDB source archive");

    src_dir
}

fn with_objectstore() -> bool {
    std::env::var("CARGO_FEATURE_OBJECTSTORE").is_ok()
}

fn build_from_source(version: &str) -> PathBuf {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let src_dir = extract_archive(version, &out_dir);

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

#[cfg(target_os = "windows")]
fn version_less_than(version: &str, major: u32, minor: u32, patch: u32) -> bool {
    !version_at_least(version, major, minor, patch)
}

fn main() {
    let version = selected_version();

    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_txn_single_delete)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_compact_range)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_max_concurrent_flushes)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_tombstone_stats)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_cancel_background_work)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_raise_open_file_limit)");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_has_write_amp_stats)");

    // These gates follow C API/ABI boundaries found while testing the version
    // matrix. Keeping them derived from the selected C tag avoids long feature
    // lists and prevents Rust FFI structs from reading fields older tags do not
    // expose.
    if version_at_least(&version, 9, 1, 0) {
        println!("cargo:rustc-cfg=tidesdb_has_txn_single_delete");
    }
    if version_at_least(&version, 9, 2, 0) {
        println!("cargo:rustc-cfg=tidesdb_has_compact_range");
        println!("cargo:rustc-cfg=tidesdb_has_max_concurrent_flushes");
        println!("cargo:rustc-cfg=tidesdb_has_tombstone_stats");
    }
    if version_at_least(&version, 9, 3, 2) {
        println!("cargo:rustc-cfg=tidesdb_has_cancel_background_work");
    }
    if version_at_least(&version, 9, 3, 3) {
        println!("cargo:rustc-cfg=tidesdb_has_raise_open_file_limit");
    }
    if version_at_least(&version, 9, 3, 4) {
        println!("cargo:rustc-cfg=tidesdb_has_write_amp_stats");
    }

    // tidesdb S3 connector uses POSIX-only functions (gmtime_r, fmemopen)
    // that were not available on Windows until v9.0.6
    #[cfg(target_os = "windows")]
    if with_objectstore() && version_less_than(&version, 9, 0, 6) {
        panic!(
            "The `objectstore` feature requires tidesdb >= 9.0.6 on Windows. \
             Versions before 9.0.6 use POSIX-only functions (gmtime_r, fmemopen)."
        );
    }

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
