// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

fn main() {
    // Try pkg-config first
    if pkg_config::probe_library("tidesdb").is_ok() {
        return;
    }

    // Fallback to common library paths
    println!("cargo:rustc-link-lib=tidesdb");
    
    // Common library search paths
    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-search=/usr/lib");
    println!("cargo:rustc-link-search=/opt/tidesdb/lib");
    
    // macOS Homebrew paths
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-search=/opt/homebrew/lib");
    }
    
    // Windows MinGW paths
    #[cfg(target_os = "windows")]
    {
        println!("cargo:rustc-link-search=C:/msys64/mingw64/lib");
    }
}
