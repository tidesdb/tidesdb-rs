// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Links the TidesDB C library.
//!
//! The crate's major version maps to a TidesDB major: crate N.x binds TidesDB (N+9).x, so
//! 1.x binds TidesDB 10.x and 2.x will bind TidesDB 11.x. A system install found through
//! pkg-config is used when it belongs to that major and is at least the bundled release;
//! anything else is ignored with a warning, and the source bundled in `tidesdb/` (a git
//! submodule pinned to `TIDESDB_VERSION`) is built with CMake and linked statically.

use std::path::{Path, PathBuf};

/// The TidesDB release the bindings in src/ffi.rs were written against and the submodule is
/// pinned to. Must match `TIDESDB_VERSION` in src/ffi.rs.
const TIDESDB_VERSION: &str = "10.2.1";

/// Crate major N binds TidesDB major N + this.
const TIDESDB_MAJOR_OFFSET: u64 = 9;

fn feature(name: &str) -> bool {
    std::env::var(format!("CARGO_FEATURE_{}", name.to_uppercase())).is_ok()
}

fn on_off(enabled: bool) -> &'static str {
    if enabled { "ON" } else { "OFF" }
}

/// `major.minor.patch`, ignoring any suffix; missing parts are 0.
fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.trim().split(['.', '-', '+']).map(|p| p.parse::<u64>());
    let major = parts.next()?.ok()?;
    let minor = parts.next().and_then(|p| p.ok()).unwrap_or(0);
    let patch = parts.next().and_then(|p| p.ok()).unwrap_or(0);
    Some((major, minor, patch))
}

/// The TidesDB major this crate version binds, checked against `TIDESDB_VERSION`.
fn tidesdb_major() -> u64 {
    let crate_version = env!("CARGO_PKG_VERSION");
    let crate_major: u64 = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
    let major = crate_major + TIDESDB_MAJOR_OFFSET;
    let bound = parse_version(TIDESDB_VERSION).unwrap().0;
    assert_eq!(
        major, bound,
        "tidesdb crate {crate_version} must bind TidesDB {major}.x (crate N.x binds TidesDB \
         (N+{TIDESDB_MAJOR_OFFSET}).x), but build.rs pins TidesDB {TIDESDB_VERSION}. Bump the \
         crate major or the pinned release so they agree."
    );
    major
}

fn bundled_source() -> PathBuf {
    let dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("tidesdb");
    let cmake_lists = dir.join("CMakeLists.txt");
    let Ok(text) = std::fs::read_to_string(&cmake_lists) else {
        panic!(
            "TidesDB source not found at {}. In a git checkout run \
             `git submodule update --init`, or install TidesDB {TIDESDB_VERSION} (or a later \
             {}.x) with a pkg-config file.",
            dir.display(),
            parse_version(TIDESDB_VERSION).unwrap().0
        );
    };

    // project(tidesdb VERSION x.y.z ...) -- the submodule must be checked out at the pinned tag
    let found = text
        .lines()
        .position(|l| l.trim_start().starts_with("project("))
        .and_then(|start| {
            let call: String = text.lines().skip(start).collect::<Vec<_>>().join(" ");
            let call = call.split(')').next()?.to_string();
            let mut words = call.split_whitespace().skip_while(|w| *w != "VERSION");
            words.nth(1).map(str::to_string)
        });
    if found.as_deref() != Some(TIDESDB_VERSION) {
        panic!(
            "the bundled TidesDB source at {} is version {}, but this crate binds TidesDB \
             {TIDESDB_VERSION}. Check out the pinned release: \
             `git -C tidesdb checkout v{TIDESDB_VERSION}`.",
            dir.display(),
            found.as_deref().unwrap_or("unknown")
        );
    }
    dir
}

fn build_from_source(src: &Path) -> PathBuf {
    let mut cfg = cmake::Config::new(src);
    cfg.define("TIDESDB_BUILD_TESTS", "OFF")
        .define("TIDESDB_BUILD_BENCH", "OFF")
        .define("TIDESDB_BUILD_FUZZERS", "OFF")
        .define("TIDESDB_WARN_STRICT", "OFF")
        .define("BUILD_SHARED_LIBS", "OFF")
        // set explicitly: the C build picks a platform-dependent default for Snappy
        .define("TIDESDB_WITH_SNAPPY", on_off(feature("snappy")))
        .define("TIDESDB_WITH_LZ4", on_off(feature("lz4")))
        .define("TIDESDB_WITH_ZSTD", on_off(feature("zstd")))
        // allocators only change what the final binary links (see link_allocator), so the
        // static archive is the same either way; keep CMake from probing for them
        .define("TIDESDB_WITH_JEMALLOC", "OFF")
        .define("TIDESDB_WITH_MIMALLOC", "OFF")
        .define("TIDESDB_WITH_TCMALLOC", "OFF");
    cfg.build()
}

/// A system library: pkg-config names to try, the name to link when none is found, and the
/// package to suggest for Debian/Ubuntu, Homebrew and vcpkg.
struct SysLib {
    pkg: &'static [&'static str],
    lib: &'static str,
    debian: &'static str,
    brew: &'static str,
    vcpkg: &'static str,
}

/// Link each library through pkg-config, falling back to linking it by name with a warning
/// that lists how to install the ones that were not found.
fn link_system_libs(what: &str, libs: &[SysLib]) {
    let missing: Vec<&SysLib> = libs
        .iter()
        .filter(|l| !l.pkg.iter().any(|p| pkg_config::probe_library(p).is_ok()))
        .collect();
    for l in &missing {
        println!("cargo:rustc-link-lib={}", l.lib);
    }
    if !missing.is_empty() {
        let names: Vec<_> = missing.iter().map(|l| l.lib).collect();
        let join = |f: fn(&SysLib) -> &'static str| {
            missing.iter().map(|l| f(l)).collect::<Vec<_>>().join(" ")
        };
        println!(
            "cargo:warning=Could not find {what} {} via pkg-config, falling back to link by \
             name. If linking fails, install them:\
             \n\x20 Debian/Ubuntu: sudo apt install {}\
             \n\x20 macOS:         brew install {}\
             \n\x20 Windows:       vcpkg install {}",
            names.join(", "),
            join(|l| l.debian),
            join(|l| l.brew),
            join(|l| l.vcpkg),
        );
    }
}

fn link_compression_deps() {
    let mut libs = Vec::new();
    if feature("zstd") {
        libs.push(SysLib {
            pkg: &["libzstd"],
            lib: "zstd",
            debian: "libzstd-dev",
            brew: "zstd",
            vcpkg: "zstd:x64-windows",
        });
    }
    if feature("lz4") {
        libs.push(SysLib {
            pkg: &["liblz4"],
            lib: "lz4",
            debian: "liblz4-dev",
            brew: "lz4",
            vcpkg: "lz4:x64-windows",
        });
    }
    if feature("snappy") {
        libs.push(SysLib {
            pkg: &["snappy"],
            lib: "snappy",
            debian: "libsnappy-dev",
            brew: "snappy",
            vcpkg: "snappy:x64-windows",
        });
    }
    link_system_libs("compression libraries", &libs);
}

/// Link the alternative allocator selected by a feature. Like TidesDB's own
/// `TIDESDB_WITH_*ALLOC` CMake options, this only links the allocator: it replaces
/// `malloc`/`free` for the whole process, the Rust side included.
fn link_allocator() {
    let selected: Vec<&str> = ["jemalloc", "mimalloc", "tcmalloc"]
        .into_iter()
        .filter(|f| feature(f))
        .collect();
    if selected.len() > 1 {
        panic!(
            "the tidesdb features {} each replace the process allocator; enable at most one",
            selected.join(", ")
        );
    }
    let Some(&alloc) = selected.first() else {
        return;
    };
    let windows = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    if windows && alloc != "mimalloc" {
        panic!("the tidesdb `{alloc}` feature is not supported on Windows; use `mimalloc`");
    }
    let lib = match alloc {
        "jemalloc" => SysLib {
            pkg: &["jemalloc"],
            lib: "jemalloc",
            debian: "libjemalloc-dev",
            brew: "jemalloc",
            vcpkg: "jemalloc:x64-windows",
        },
        "mimalloc" => SysLib {
            pkg: &["mimalloc"],
            lib: "mimalloc",
            debian: "libmimalloc-dev",
            brew: "mimalloc",
            vcpkg: "mimalloc:x64-windows",
        },
        _ => SysLib {
            pkg: &["libtcmalloc", "libtcmalloc_minimal"],
            lib: "tcmalloc",
            debian: "libgoogle-perftools-dev",
            brew: "gperftools",
            vcpkg: "gperftools:x64-windows",
        },
    };
    link_system_libs("allocator", &[lib]);
}

/// Use a TidesDB found through pkg-config if it belongs to the bound major and is at least the
/// bundled release. Returns the version linked, or `None` to build the bundled source.
fn system_tidesdb(major: u64) -> Option<String> {
    if std::env::var_os("TIDESDB_NO_PKG_CONFIG").is_some() {
        return None;
    }
    // link metadata only once the version is accepted, but rerun when PKG_CONFIG_* changes
    let found = pkg_config::Config::new()
        .cargo_metadata(false)
        .env_metadata(true)
        .probe("tidesdb")
        .ok()?;
    let crate_version = env!("CARGO_PKG_VERSION");
    let crate_major = major - TIDESDB_MAJOR_OFFSET;
    let usable = match parse_version(&found.version) {
        Some(v) => v.0 == major && v >= parse_version(TIDESDB_VERSION).unwrap(),
        None => false,
    };
    if !usable {
        println!(
            "cargo:warning=Ignoring the system TidesDB {} found via pkg-config: tidesdb crate \
             {crate_version} binds TidesDB {major}.x (at least {TIDESDB_VERSION}). Crate \
             {crate_major}.x maps to TidesDB {major}.x, crate {}.x to TidesDB {}.x. Building the \
             bundled TidesDB {TIDESDB_VERSION} instead.",
            found.version,
            crate_major + 1,
            major + 1,
        );
        return None;
    }

    // probe again, this time emitting the link metadata
    let lib = pkg_config::Config::new()
        .range_version(found.version.as_str()..=found.version.as_str())
        .probe("tidesdb")
        .ok()?;
    if lib.version != TIDESDB_VERSION {
        println!(
            "cargo:warning=Linking the system TidesDB {} found via pkg-config. The bindings \
             were written against TidesDB {TIDESDB_VERSION}; set TIDESDB_NO_PKG_CONFIG=1 to \
             build the bundled {TIDESDB_VERSION} instead.",
            lib.version
        );
    }
    if feature("snappy") || feature("lz4") || feature("zstd") {
        println!(
            "cargo:warning=The tidesdb compression features do not change a system TidesDB: it \
             has whichever codecs it was built with. Check CompressionAlgorithm::is_available."
        );
    }
    Some(lib.version)
}

/// The platform the crate is being built for, from Cargo's target cfg. A build script's own
/// `#[cfg(...)]` describes the machine running the build, which differs when cross-compiling.
struct Target {
    os: String,
    family: String,
    env: String,
    pointer_width: String,
    /// Whether the build runs on the target platform itself (not cross-compiling)
    native: bool,
}

impl Target {
    fn from_env() -> Self {
        let var = |k: &str| std::env::var(k).unwrap_or_default();
        Target {
            os: var("CARGO_CFG_TARGET_OS"),
            family: var("CARGO_CFG_TARGET_FAMILY"),
            env: var("CARGO_CFG_TARGET_ENV"),
            pointer_width: var("CARGO_CFG_TARGET_POINTER_WIDTH"),
            native: var("HOST") == var("TARGET"),
        }
    }
}

/// The system libraries the static TidesDB needs, following the per-platform link recipe in
/// TidesDB's CMakeLists.txt.
fn link_platform_libs(target: &Target) {
    let link = |lib: &str| println!("cargo:rustc-link-lib={lib}");
    match target.os.as_str() {
        // MSVC uses the native Win32 threads in compat.h; MinGW links winpthreads
        "windows" => {
            if target.env == "gnu" {
                link("pthread");
            }
        }
        // Bionic has pthreads and libm in libc and ships no libpthread
        "android" => link("m"),
        "macos" | "ios" | "freebsd" | "openbsd" | "netbsd" | "dragonfly" | "solaris"
        | "illumos" => {
            link("m");
            link("pthread");
        }
        _ if target.family == "unix" => {
            link("pthread");
            link("m");
            // 64-bit atomics on 32-bit targets (ARMv7, PowerPC, MIPS, x86) live in libatomic
            if target.pointer_width == "32" {
                link("atomic");
            }
            // Snappy is C++; its C++ runtime comes along when Snappy is enabled
            if feature("snappy") {
                link("stdc++");
            }
        }
        _ => {}
    }
}

/// Library directories of the build machine's package managers, where the codec and
/// allocator libraries usually live. Only added for native builds: when cross-compiling
/// they hold libraries for the wrong platform, and the target's libraries are found through
/// pkg-config (`PKG_CONFIG_SYSROOT_DIR`, `PKG_CONFIG_ALLOW_CROSS`) or `RUSTFLAGS="-L ..."`.
fn link_host_search_paths(target: &Target) {
    if !target.native {
        return;
    }
    let search = |dir: &str| println!("cargo:rustc-link-search=native={dir}");
    match target.os.as_str() {
        "macos" => match brew_prefix() {
            Some(prefix) => search(&format!("{prefix}/lib")),
            None => {
                search("/opt/homebrew/lib");
                search("/usr/local/lib");
            }
        },
        "windows" => {
            // MSYS2 shells set MINGW_PREFIX as a POSIX path (/mingw64); the native linker
            // needs it under the default MSYS2 root
            if target.env == "gnu" {
                let prefix = std::env::var("MINGW_PREFIX").unwrap_or_else(|_| "/mingw64".into());
                match prefix.strip_prefix('/') {
                    Some(rest) => search(&format!("C:/msys64/{rest}/lib")),
                    None => search(&format!("{prefix}/lib")),
                }
            }
        }
        "netbsd" => search("/usr/pkg/lib"),
        "solaris" | "illumos" => search("/opt/ooce/lib/amd64"),
        _ => search("/usr/local/lib"),
    }
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
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=tidesdb/include/db.h");
    println!("cargo:rerun-if-changed=tidesdb/CMakeLists.txt");
    println!("cargo:rerun-if-env-changed=TIDESDB_NO_PKG_CONFIG");
    println!("cargo:rerun-if-env-changed=MINGW_PREFIX");
    println!("cargo:rustc-check-cfg=cfg(tidesdb_system)");

    let major = tidesdb_major();
    let target = Target::from_env();

    if let Some(version) = system_tidesdb(major) {
        println!("cargo:rustc-cfg=tidesdb_system");
        println!("cargo:rustc-env=TIDESDB_LINKED_VERSION={version}");
        link_allocator();
        link_host_search_paths(&target);
        return;
    }

    let dst = build_from_source(&bundled_source());
    println!("cargo:rustc-env=TIDESDB_LINKED_VERSION={TIDESDB_VERSION}");
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-search=native={}/lib64", dst.display());
    println!("cargo:rustc-link-lib=static=tidesdb");

    link_compression_deps();
    link_allocator();
    link_platform_libs(&target);
    link_host_search_paths(&target);
}
