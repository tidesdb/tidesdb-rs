#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "tomlkit",
#     "requests",
#     "InquirerPy",
# ]
# ///

"""Sync tidesdb version features and source crates.

Fetches TidesDB releases and crates.io state, presents a plan, updates
Cargo.toml/build.rs so each version feature enables a matching tidesdb source
archive crate, and optionally prepares missing or changed source crates. Prepared
crates are checked with cargo publish --dry-run unless --publish is set.

Flow:

  start
    |
    v
  fetch TidesDB GitHub releases
    |
    v
  read current Cargo.toml features/default
    |
    +-- --prepare-local-patches?
    |     |
    |     v
    |   write local source crates for selected versions
    |     |
    |   write optional .cargo/config.toml [patch.crates-io] entries
    |     |
    |     v
    |   done
    |
    v
  choose versions/default/source-crate set
    |            |
    |            +-- --versions: use CLI inputs and defaults,
    |            |               then survey selected source crates
    |            |
    |            +-- no --versions: survey all source crates,
    |                            then use interactive pickers
    |
    v
  print plan
    |
    +-- any selected crate state unknown? -> abort before writes/publish
    |
    +-- --dry-run? -> exit after plan
    |
    v
  confirm plan unless --yes
    |
    +-- not --source-crates-only -> update Cargo.toml, build.rs, and CI default
    |
    v
  root files are current or intentionally skipped
    |
    +-- no source crates selected? -> done
    |
    v
  prepare each selected source crate in work dir
    |
    v
  cargo publish --dry-run each prepared crate unless --publish is set
    |
    v
  clean temp work dir, or keep explicit/requested work dir
    |
    v
  done
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import io
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import requests
import tomlkit
from InquirerPy import inquirer

CARGO_TOML = Path("Cargo.toml")
BUILD_RS = Path("build.rs")
BUILD_AND_TEST_YML = Path(".github/workflows/build_and_test.yml")
LICENSE = Path("LICENSE")
REPO = "tidesdb/tidesdb"
SOURCE_CRATE_INITIAL_VERSION = "0.1.0"
CRATES_IO = "https://crates.io/api/v1/crates"
USER_AGENT = "tidesdb-rs source-crate sync (https://github.com/tidesdb/tidesdb-rs)"


@dataclass
class ArchiveInfo:
    data: bytes
    size: int
    sha256: str


@dataclass
class SourceCrateState:
    status: str
    published_version: str | None = None
    publish_version: str | None = None
    reasons: list[str] = field(default_factory=list)

    @property
    def needs_publish(self) -> bool:
        return self.status in {"missing", "update"}

    @property
    def is_blocking(self) -> bool:
        return self.status == "unknown"


def versions_from_releases(releases: Iterable[dict]) -> list[str]:
    return versions_from_tags(release["tag_name"] for release in releases)


def versions_from_tags(tags: Iterable[str]) -> list[str]:
    versions = []
    for tag in tags:
        if not tag.startswith("v"):
            continue
        version = tag[1:]
        if not is_version(version):
            continue
        if int(version.split(".")[0]) < 9:
            continue
        versions.append(version)

    return sorted(set(versions), key=version_key)


def fetch_versions_with_gh() -> list[str] | None:
    if shutil.which("gh") is None:
        return None

    cmd = [
        "gh",
        "api",
        "--method",
        "GET",
        "--paginate",
        f"repos/{REPO}/releases",
        "-f",
        "per_page=100",
        "--jq",
        ".[].tag_name",
    ]
    try:
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as exc:
        print(
            f"warning: gh release query failed, falling back to GitHub API: {exc.stderr.strip()}",
            file=sys.stderr,
        )
        return None

    return versions_from_tags(output.splitlines())


def fetch_versions_with_requests() -> list[str]:
    url = f"https://api.github.com/repos/{REPO}/releases"
    page = 1
    releases = []
    while True:
        resp = requests.get(
            url,
            params={"per_page": 100, "page": page},
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        releases.extend(data)
        page += 1

    return versions_from_releases(releases)


def fetch_versions() -> list[str]:
    """Fetch all TidesDB release versions from GitHub, sorted ascending."""
    versions = fetch_versions_with_gh()
    if versions is not None:
        return versions
    return fetch_versions_with_requests()


def fetch_crate_api(name: str) -> dict | None:
    resp = requests.get(
        f"{CRATES_IO}/{name}",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def latest_published_version(crate: dict) -> str | None:
    versions = [
        version["num"]
        for version in crate.get("versions", [])
        if not version.get("yanked", False)
    ]
    if not versions:
        return None
    return sorted(versions, key=version_key)[-1]


def bump_patch(version: str) -> str:
    major, minor, patch = version_key(version)
    return f"{major}.{minor}.{patch + 1}"


def fetch_published_crate_bytes(name: str, version: str) -> bytes:
    url = f"{CRATES_IO}/{name}/{version}/download"
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    return resp.content


def read_member_bytes(crate_bytes: bytes, member_name: str) -> bytes | None:
    with tarfile.open(fileobj=io.BytesIO(crate_bytes), mode="r:gz") as archive:
        for member in archive.getmembers():
            if member.name.endswith(f"/{member_name}"):
                file = archive.extractfile(member)
                return file.read() if file else None
    return None


def expected_manifest(version: str, package_version: str) -> str:
    crate_name = source_crate_name(version)
    cargo = tomlkit.document()
    package = tomlkit.table()
    package["name"] = crate_name
    package["version"] = package_version
    package["edition"] = "2024"
    package["authors"] = ["TidesDB <hello@tidesdb.com>"]
    package["description"] = f"Vendored TidesDB C source archive for v{version}"
    package["license"] = "MPL-2.0"
    package["repository"] = "https://github.com/tidesdb/tidesdb-rs"
    package["homepage"] = "https://tidesdb.com"
    package["readme"] = "README.md"
    package["keywords"] = ["database", "key-value", "tidesdb", "source"]
    package["categories"] = ["database", "external-ffi-bindings"]
    package["include"] = ["src/**", "vendor/**", "Cargo.toml", "README.md", "LICENSE"]
    cargo["package"] = package
    cargo["lib"] = {"path": "src/lib.rs"}
    return tomlkit.dumps(cargo)


def expected_lib_rs(version: str) -> str:
    archive_name = source_archive_name(version)
    return (
        "use std::path::{Path, PathBuf};\n\n"
        f"pub const TIDESDB_VERSION: &str = \"{version}\";\n"
        f"pub const ARCHIVE_NAME: &str = \"{archive_name}\";\n\n"
        "pub fn archive_path() -> PathBuf {\n"
        "    Path::new(env!(\"CARGO_MANIFEST_DIR\"))\n"
        "        .join(\"vendor\")\n"
        "        .join(ARCHIVE_NAME)\n"
        "}\n"
    )


def comparable_manifest(content: bytes, package_version: str) -> dict:
    data = tomlkit.loads(content.decode("utf-8"))
    package = to_plain(data["package"])
    package.pop("version", None)
    package["version"] = package_version
    return {
        "package": package,
        "lib": to_plain(data.get("lib", {})),
    }


def to_plain(value):
    if isinstance(value, dict):
        return {str(key): to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    return value.unwrap() if hasattr(value, "unwrap") else value


def archive_info(data: bytes) -> ArchiveInfo:
    return ArchiveInfo(
        data=data,
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


def compare_published_crate(
    version: str,
    published_version: str,
    published_bytes: bytes,
    source_archive: ArchiveInfo,
) -> list[str]:
    reasons = []

    manifest = (
        read_member_bytes(published_bytes, "Cargo.toml.orig")
        or read_member_bytes(published_bytes, "Cargo.toml")
    )
    if manifest is None:
        reasons.append("published crate is missing Cargo.toml")
    else:
        expected = comparable_manifest(
            expected_manifest(version, published_version).encode("utf-8"),
            published_version,
        )
        actual = comparable_manifest(manifest, published_version)
        if actual != expected:
            reasons.append("package metadata differs")

    lib_rs = read_member_bytes(published_bytes, "src/lib.rs")
    if lib_rs is None:
        reasons.append("published crate is missing src/lib.rs")
    elif lib_rs.decode("utf-8") != expected_lib_rs(version):
        reasons.append("src/lib.rs metadata differs")

    archive = read_member_bytes(published_bytes, f"vendor/{source_archive_name(version)}")
    if archive is None:
        reasons.append("published crate is missing vendored archive")
    else:
        published_archive = archive_info(archive)
        if published_archive.size != source_archive.size:
            reasons.append(
                f"vendored archive size differs ({published_archive.size} != {source_archive.size})"
            )
        if published_archive.sha256 != source_archive.sha256:
            reasons.append(
                "vendored archive sha256 differs "
                f"({published_archive.sha256} != {source_archive.sha256})"
            )

    return reasons


def fetch_crates_state(versions: Iterable[str]) -> dict[str, SourceCrateState]:
    state = {}
    for version in versions:
        name = source_crate_name(version)
        try:
            crate = fetch_crate_api(name)
            if crate is None:
                state[version] = SourceCrateState(
                    status="missing",
                    publish_version=SOURCE_CRATE_INITIAL_VERSION,
                    reasons=["crate is not published"],
                )
                continue

            published_version = latest_published_version(crate)
            if published_version is None:
                state[version] = SourceCrateState(
                    status="missing",
                    publish_version=SOURCE_CRATE_INITIAL_VERSION,
                    reasons=["crate has no non-yanked published versions"],
                )
                continue

            source_archive = download_archive_info(version)
            published_bytes = fetch_published_crate_bytes(name, published_version)
            reasons = compare_published_crate(
                version,
                published_version,
                published_bytes,
                source_archive,
            )
            if reasons:
                state[version] = SourceCrateState(
                    status="update",
                    published_version=published_version,
                    publish_version=bump_patch(published_version),
                    reasons=reasons,
                )
            else:
                state[version] = SourceCrateState(
                    status="current",
                    published_version=published_version,
                    publish_version=published_version,
                )
        except requests.HTTPError as exc:
            print(f"warning: failed to query crates.io for {name}: {exc}", file=sys.stderr)
            state[version] = SourceCrateState(
                status="unknown",
                reasons=[f"crates.io query failed: {exc}"],
            )
    return state


def is_version(value: str) -> bool:
    parts = value.split(".")
    if len(parts) != 3:
        return False
    return all(part.isdigit() for part in parts)


def version_key(version: str) -> tuple[int, int, int]:
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


def version_to_feature(version: str) -> str:
    return "v" + version.replace(".", "_")


def feature_to_version(feature: str) -> str:
    return feature[1:].replace("_", ".")


def source_crate_name(version: str) -> str:
    return "tidesdb-src-v" + version.replace(".", "-")


def source_crate_rust_name(version: str) -> str:
    return source_crate_name(version).replace("-", "_")


def source_archive_name(version: str) -> str:
    return f"tidesdb-{version}.tar.gz"


def parse_versions(value: str, current: list[str], all_versions: list[str]) -> list[str]:
    if value == "current":
        return current
    if value == "all":
        return all_versions

    selected = []
    for raw in value.split(","):
        version = raw.strip().removeprefix("v").replace("_", ".")
        if not version:
            continue
        if not is_version(version):
            raise SystemExit(f"invalid version: {raw}")
        selected.append(version)
    return sorted(set(selected), key=version_key)


def parse_publish_versions(value: str, selected: list[str], needed: list[str], missing: list[str]) -> list[str]:
    if value == "none":
        return []
    if value == "needed":
        return needed
    if value == "missing":
        return missing
    if value == "selected":
        return [version for version in selected if version in needed]

    versions = parse_versions(value, current=selected, all_versions=selected)
    unselected = sorted(set(versions) - set(selected), key=version_key)
    if unselected:
        raise SystemExit(
            "--publish-versions must be a subset of --versions: "
            + ", ".join(unselected)
        )
    not_needed = sorted(set(versions) - set(needed), key=version_key)
    if not_needed:
        raise SystemExit(
            "selected source crates do not need publish/update: "
            + ", ".join(not_needed)
        )
    return versions


def load_cargo_toml() -> tomlkit.TOMLDocument:
    with CARGO_TOML.open("r") as f:
        return tomlkit.load(f)


def save_cargo_toml(data: tomlkit.TOMLDocument):
    with CARGO_TOML.open("w") as f:
        tomlkit.dump(data, f)


def current_version_features(cargo: dict) -> set[str]:
    return {
        key
        for key in cargo.get("features", {})
        if re.match(r"^v\d+_\d+_\d+$", key)
    }


def current_versions(cargo: dict) -> list[str]:
    return sorted(
        [feature_to_version(feature) for feature in current_version_features(cargo)],
        key=version_key,
    )


def current_default_feature(cargo: dict) -> str | None:
    for feature in cargo.get("features", {}).get("default", []):
        if re.match(r"^v\d+_\d+_\d+$", feature):
            return feature
    return None


def source_crate_requirement(version: str, state: SourceCrateState | None) -> str:
    package_version = (
        state.publish_version
        if state and state.publish_version
        else SOURCE_CRATE_INITIAL_VERSION
    )
    major, minor, _ = version_key(package_version)
    return f"{major}.{minor}"


def dependency_value(version: str, state: SourceCrateState | None):
    value = tomlkit.inline_table()
    value["version"] = source_crate_requirement(version, state)
    value["optional"] = True
    return value


def update_cargo_toml(
    cargo: tomlkit.TOMLDocument,
    versions: list[str],
    default_version: str,
    crates_state: dict[str, SourceCrateState],
):
    build_deps = cargo.setdefault("build-dependencies", tomlkit.table())
    if "ureq" in build_deps:
        del build_deps["ureq"]
    build_deps["cfg-if"] = "1"

    for key in list(build_deps.keys()):
        if re.match(r"^tidesdb-src-v\d+-\d+-\d+$", key):
            del build_deps[key]

    deps = cargo.setdefault("dependencies", tomlkit.table())
    for key in list(deps.keys()):
        if re.match(r"^tidesdb-src-v\d+-\d+-\d+$", key):
            del deps[key]

    for version in versions:
        build_deps[source_crate_name(version)] = dependency_value(version, crates_state.get(version))

    features = cargo.setdefault("features", tomlkit.table())
    for key in list(features.keys()):
        if re.match(r"^v\d+_\d+_\d+$", key):
            del features[key]

    new_default = [
        feature
        for feature in features.get("default", [])
        if not re.match(r"^v\d+_\d+_\d+$", feature)
    ]
    new_default.append(version_to_feature(default_version))
    features["default"] = new_default

    for version in versions:
        features[version_to_feature(version)] = [f"dep:{source_crate_name(version)}"]


def render_cargo_toml(
    cargo: tomlkit.TOMLDocument,
    versions: list[str],
    default_version: str,
    crates_state: dict[str, SourceCrateState],
) -> str:
    rendered = copy.deepcopy(cargo)
    update_cargo_toml(rendered, versions, default_version, crates_state)
    return tomlkit.dumps(rendered)


def source_archive_function(versions: list[str]) -> str:
    branches = []
    for version in reversed(versions):
        feature = version_to_feature(version)
        crate = source_crate_rust_name(version)
        keyword = "if" if not branches else "else if"
        branches.append(
            f'    {keyword} #[cfg(feature = "{feature}")] {{\n'
            "        fn source_archive() -> PathBuf {\n"
            f"            {crate}::archive_path()\n"
            "        }\n"
            "    }"
        )
    branch_text = "\n".join(branches)
    return (
        "cfg_if::cfg_if! {\n"
        f"{branch_text}\n"
        "    else {\n"
        "        fn source_archive() -> PathBuf {\n"
        "            panic!(\"no tidesdb source crate is enabled; enable a vX_Y_Z feature\")\n"
        "        }\n"
        "    }\n"
        "}\n"
    )


def extraction_functions(versions: list[str]) -> str:
    return (
        "// BEGIN GENERATED TIDESDB SOURCE ARCHIVES\n"
        f"{source_archive_function(versions)}\n"
        "// END GENERATED TIDESDB SOURCE ARCHIVES\n\n"
        "fn extract_archive(version: &str, out_dir: &str) -> PathBuf {\n"
        "    let archive_path = source_archive();\n"
        "    let extract_dir = PathBuf::from(out_dir).join(\"tidesdb-source\");\n"
        "    let src_dir = extract_dir.join(format!(\"tidesdb-{version}\"));\n\n"
        "    if src_dir.exists() {\n"
        "        return src_dir;\n"
        "    }\n\n"
        "    let tarball = std::fs::File::open(&archive_path).unwrap_or_else(|e| {\n"
        "        panic!(\n"
        "            \"failed to open TidesDB v{version} source archive {}: {e}\",\n"
        "            archive_path.display()\n"
        "        )\n"
        "    });\n"
        "    let decoder = flate2::read::GzDecoder::new(tarball);\n"
        "    let mut archive = tar::Archive::new(decoder);\n"
        "    std::fs::create_dir_all(&extract_dir).expect(\"Failed to create extract directory\");\n"
        "    archive\n"
        "        .unpack(&extract_dir)\n"
        "        .expect(\"Failed to extract TidesDB source archive\");\n\n"
        "    src_dir\n"
        "}\n"
    )


def render_build_rs(content: str, versions: list[str], default_version: str) -> str:
    content = re.sub(
        r'None => "[\d.]+"\.to_string\(\)',
        f'None => "{default_version}".to_string()',
        content,
    )

    generated = extraction_functions(versions)
    if "BEGIN GENERATED TIDESDB SOURCE ARCHIVES" in content:
        content = re.sub(
            r"// BEGIN GENERATED TIDESDB SOURCE ARCHIVES\n.*?// END GENERATED TIDESDB SOURCE ARCHIVES\n\n"
            r"fn extract_archive\(version: &str, out_dir: &str\) -> PathBuf \{\n.*?\n\}\n",
            generated,
            content,
            flags=re.S,
        )
    else:
        content = re.sub(
            r"fn download_and_extract\(version: &str, out_dir: &str\) -> PathBuf \{\n.*?\n\}\n\n",
            generated + "\n",
            content,
            flags=re.S,
        )

    content = content.replace(
        "let src_dir = download_and_extract(version, &out_dir);",
        "let src_dir = extract_archive(version, &out_dir);",
    )

    return content


def update_build_rs(versions: list[str], default_version: str):
    BUILD_RS.write_text(render_build_rs(BUILD_RS.read_text(), versions, default_version))


def files_to_modify_for_plan(
    cargo: tomlkit.TOMLDocument,
    selected_versions: list[str],
    default_version: str,
    crates_state: dict[str, SourceCrateState],
) -> list[Path]:
    files = []
    if render_cargo_toml(cargo, selected_versions, default_version, crates_state) != CARGO_TOML.read_text():
        files.append(CARGO_TOML)
    if render_build_rs(BUILD_RS.read_text(), selected_versions, default_version) != BUILD_RS.read_text():
        files.append(BUILD_RS)
    if BUILD_AND_TEST_YML.exists():
        current_default = current_default_feature(cargo)
        if current_default != version_to_feature(default_version):
            files.append(BUILD_AND_TEST_YML)
    return files


def update_ci_default(version: str):
    if not BUILD_AND_TEST_YML.exists():
        return

    content = BUILD_AND_TEST_YML.read_text()
    content = re.sub(
        r"(repository: tidesdb/tidesdb\s+ref: )v[\d.]+",
        rf"\g<1>v{version}",
        content,
    )
    BUILD_AND_TEST_YML.write_text(content)


def download_archive(version: str, destination: Path):
    info = download_archive_info(version)
    destination.write_bytes(info.data)


def download_archive_info(version: str) -> ArchiveInfo:
    url = f"https://github.com/{REPO}/archive/refs/tags/v{version}.tar.gz"
    with requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        data = b"".join(chunk for chunk in resp.iter_content(chunk_size=1024 * 1024) if chunk)
    return archive_info(data)


def write_source_crate(
    version: str,
    package_version: str,
    root: Path,
    *,
    include_archive: bool = True,
):
    crate_name = source_crate_name(version)
    crate_dir = root / crate_name
    if crate_dir.exists():
        shutil.rmtree(crate_dir)
    vendor_dir = crate_dir / "vendor"
    src_dir = crate_dir / "src"
    vendor_dir.mkdir(parents=True)
    src_dir.mkdir(parents=True)

    archive_name = source_archive_name(version)
    if include_archive:
        download_archive(version, vendor_dir / archive_name)

    (crate_dir / "Cargo.toml").write_text(expected_manifest(version, package_version))

    (crate_dir / "README.md").write_text(
        f"# {crate_name}\n\n"
        f"This crate packages the TidesDB C source archive for `v{version}`.\n\n"
        "It is intended as a build-dependency of the `tidesdb` Rust bindings.\n"
    )

    if LICENSE.exists():
        shutil.copyfile(LICENSE, crate_dir / "LICENSE")

    (src_dir / "lib.rs").write_text(expected_lib_rs(version))

    return crate_dir


def dependency_version(cargo: tomlkit.TOMLDocument, version: str) -> str:
    crate_name = source_crate_name(version)
    dependency = cargo.get("build-dependencies", {}).get(crate_name)
    if isinstance(dependency, str):
        requirement = dependency
    elif isinstance(dependency, dict):
        requirement = dependency.get("version", SOURCE_CRATE_INITIAL_VERSION)
    else:
        requirement = SOURCE_CRATE_INITIAL_VERSION

    match = re.fullmatch(r"(\d+)\.(\d+)(?:\.(\d+))?", str(requirement))
    if not match:
        return SOURCE_CRATE_INITIAL_VERSION

    major, minor, patch = match.groups()
    return f"{major}.{minor}.{patch or 0}"


def parse_local_patch_archive_versions(
    value: str,
    *,
    selected_versions: list[str],
    default_version: str,
) -> set[str]:
    if value == "all":
        return set(selected_versions)
    if value == "current":
        return set(selected_versions)
    if value == "default":
        return {default_version}
    if value == "none":
        return set()
    return set(parse_versions(value, current=selected_versions, all_versions=selected_versions))


def write_cargo_patch_config(config_path: Path, crate_dirs: dict[str, Path]):
    if config_path.exists():
        config = tomlkit.parse(config_path.read_text())
    else:
        config = tomlkit.document()

    patch = config.setdefault("patch", tomlkit.table())
    crates_io = patch.setdefault("crates-io", tomlkit.table())

    for key in list(crates_io.keys()):
        if re.match(r"^tidesdb-src-v\d+-\d+-\d+$", key):
            del crates_io[key]

    for version, crate_dir in sorted(crate_dirs.items(), key=lambda item: version_key(item[0])):
        value = tomlkit.inline_table()
        value["path"] = crate_dir.resolve().as_posix()
        crates_io[source_crate_name(version)] = value

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(tomlkit.dumps(config))


def prepare_local_patches(
    cargo: tomlkit.TOMLDocument,
    versions: list[str],
    work_root: Path,
    config_path: Path | None,
    archive_versions: set[str],
):
    work_root.mkdir(parents=True, exist_ok=True)
    crate_dirs = {}

    for version in versions:
        package_version = dependency_version(cargo, version)
        include_archive = version in archive_versions
        crate_dir = write_source_crate(
            version,
            package_version,
            work_root,
            include_archive=include_archive,
        )
        crate_dirs[version] = crate_dir
        if include_archive:
            print(f"Prepared {source_crate_name(version)}@{package_version} with vendored archive")
        else:
            print(f"Prepared {source_crate_name(version)}@{package_version} for dependency patching")

    if config_path:
        write_cargo_patch_config(config_path, crate_dirs)
        print(f"Wrote Cargo patch config to {config_path}")


def cargo_publish(crate_dir: Path, dry_run: bool):
    cmd = ["cargo", "publish", "--manifest-path", str(crate_dir / "Cargo.toml")]
    if dry_run:
        cmd.append("--dry-run")
    subprocess.run(cmd, check=True)


def summarize(
    selected_versions: list[str],
    default_version: str,
    existing_features: set[str],
    crates_state: dict[str, SourceCrateState],
    publish_versions: list[str],
    files_to_modify: list[Path],
    publish: bool,
    source_crates_only: bool,
):
    selected_features = {version_to_feature(v) for v in selected_versions}
    added = selected_features - existing_features
    removed = existing_features - selected_features
    current_crates = [
        f"{source_crate_name(v)}@{crates_state[v].published_version}"
        for v in selected_versions
        if crates_state.get(v) and crates_state[v].status == "current"
    ]
    missing_crates = [
        f"{source_crate_name(v)} -> {crates_state[v].publish_version}"
        for v in selected_versions
        if crates_state.get(v) and crates_state[v].status == "missing"
    ]
    unknown_crates = [
        source_crate_name(v)
        for v in selected_versions
        if crates_state.get(v) and crates_state[v].status == "unknown"
    ]
    update_crates = [
        f"{source_crate_name(v)} {crates_state[v].published_version} -> {crates_state[v].publish_version}"
        for v in selected_versions
        if crates_state.get(v) and crates_state[v].status == "update"
    ]

    print("\nPlan:")
    print(f"  Version features: {', '.join(version_to_feature(v) for v in selected_versions)}")
    print(f"  Default:          {version_to_feature(default_version)} ({default_version})")
    if source_crates_only:
        print("  Root changes:     skipped")
    elif added:
        print(f"  Add features:     {', '.join(sorted(added))}")
    if not source_crates_only and removed:
        print(f"  Remove features:  {', '.join(sorted(removed))}")
    if current_crates:
        print(f"  Current crates:   {', '.join(current_crates)}")
    if missing_crates:
        print(f"  Missing crates:   {', '.join(missing_crates)}")
    if unknown_crates:
        print(f"  Unknown crates:   {', '.join(unknown_crates)}")
        for version in selected_versions:
            state = crates_state.get(version)
            if state and state.status == "unknown":
                print(f"    {source_crate_name(version)}: {'; '.join(state.reasons)}")
    if update_crates:
        print(f"  Update crates:    {', '.join(update_crates)}")
        for version in selected_versions:
            state = crates_state.get(version)
            if state and state.status == "update":
                print(f"    {source_crate_name(version)}: {'; '.join(state.reasons)}")
    if publish_versions:
        action = "Publish crates" if publish else "Check crates"
        print(
            f"  {action}:     "
            + ", ".join(
                f"{source_crate_name(v)}@{crates_state[v].publish_version}"
                for v in publish_versions
            )
        )
    else:
        print("  Publish/check:    none")
    if source_crates_only:
        files = "none (--source-crates-only)"
    else:
        files = ", ".join(str(path) for path in files_to_modify) if files_to_modify else "none"
    print(f"  Files to modify:  {files}")


def choose_interactively(
    all_versions: list[str],
    current: list[str],
    current_default: str | None,
    crates_state: dict[str, SourceCrateState],
) -> tuple[list[str], str, list[str]]:
    choices = []
    for version in all_versions:
        feature = version_to_feature(version)
        state = crates_state[version]
        if state.status == "current":
            status = f"published {state.published_version}"
        elif state.status == "update":
            status = f"update {state.published_version} -> {state.publish_version}"
        elif state.status == "unknown":
            status = "unknown; registry check failed"
        else:
            status = f"missing -> {state.publish_version}"
        name = f"{feature} ({version}, {status})"
        if feature == current_default:
            name += " [current default]"
        choices.append(
            {
                "name": name,
                "value": version,
                "enabled": version in current,
            }
        )

    selected_versions = inquirer.checkbox(
        message="Select version features:",
        choices=choices,
        instruction="(Space to toggle, Enter to confirm)",
    ).execute()

    if not selected_versions:
        raise SystemExit("No versions selected, aborting.")

    selected_versions = sorted(selected_versions, key=version_key)
    default_current = feature_to_version(current_default) if current_default else None
    default_default = default_current if default_current in selected_versions else selected_versions[-1]

    default_version = inquirer.select(
        message="Select default version:",
        choices=[
            {"name": f"{version_to_feature(v)} ({v})", "value": v}
            for v in selected_versions
        ],
        default=default_default,
    ).execute()

    needed = [version for version in selected_versions if crates_state[version].needs_publish]
    publish_versions = []
    if needed:
        publish_versions = inquirer.checkbox(
            message="Select source crates to publish/update:",
            choices=[
                {
                    "name": (
                        f"{source_crate_name(version)} "
                        f"({crates_state[version].status}, "
                        f"{crates_state[version].published_version or 'none'}"
                        f" -> {crates_state[version].publish_version})"
                    ),
                    "value": version,
                    "enabled": True,
                }
                for version in needed
            ],
            instruction="(Space to toggle, Enter to confirm)",
        ).execute()
        publish_versions = sorted(publish_versions, key=version_key)

    return selected_versions, default_version, publish_versions


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Show the plan without writing files or publishing crates")
    parser.add_argument("--yes", action="store_true", help="Apply the plan without an interactive final confirmation")
    parser.add_argument(
        "--versions",
        help="Comma-separated versions, 'current', or 'all'. Without this, use the interactive picker.",
    )
    parser.add_argument("--default-version", help="Default TidesDB version feature to enable")
    parser.add_argument(
        "--publish-versions",
        default=None,
        help="Comma-separated versions to prepare, 'needed', 'missing', 'selected', or 'none'. Defaults to needed in non-interactive mode.",
    )
    parser.add_argument("--skip-publish", action="store_true", help="Update files and prepare no crates")
    parser.add_argument("--publish", action="store_true", help="Actually upload prepared crates to crates.io")
    parser.add_argument("--publish-dry-run", action="store_true", help="Force cargo publish --dry-run for prepared crates")
    parser.add_argument(
        "--prepare-local-patches",
        type=Path,
        help="Prepare local source crates for Cargo dependency patching and exit",
    )
    parser.add_argument(
        "--write-cargo-patch-config",
        type=Path,
        help="Write [patch.crates-io] entries pointing at prepared local source crates",
    )
    parser.add_argument(
        "--local-patch-archive-versions",
        default="default",
        help="Versions whose local patch crates should include vendored archives: default, all, current, none, or a comma-separated list",
    )
    parser.add_argument(
        "--source-crates-only",
        action="store_true",
        help="Only prepare/check/publish source crates; do not update Cargo.toml, build.rs, or CI",
    )
    parser.add_argument("--keep-work-dir", action="store_true", help="Keep prepared source crates instead of cleaning them up")
    parser.add_argument("--work-dir", type=Path, help="Directory for prepared source crates")
    args = parser.parse_args()

    cargo = load_cargo_toml()
    current = current_versions(cargo)
    existing_features = current_version_features(cargo)
    current_default = current_default_feature(cargo)
    default_current = feature_to_version(current_default) if current_default else None

    if args.prepare_local_patches:
        selected_versions = parse_versions(
            args.versions or "current",
            current=current,
            all_versions=current,
        )
        default_version = args.default_version or (
            default_current if default_current in selected_versions else selected_versions[-1]
        )
        if default_version not in selected_versions:
            raise SystemExit("--default-version must be one of the selected versions")
        archive_versions = parse_local_patch_archive_versions(
            args.local_patch_archive_versions,
            selected_versions=selected_versions,
            default_version=default_version,
        )
        prepare_local_patches(
            cargo,
            selected_versions,
            args.prepare_local_patches,
            args.write_cargo_patch_config,
            archive_versions,
        )
        return

    if args.write_cargo_patch_config:
        raise SystemExit("--write-cargo-patch-config requires --prepare-local-patches")

    print(f"Fetching releases from {REPO}...")
    all_versions = fetch_versions()
    print(f"Found {len(all_versions)} releases.")

    if args.versions:
        selected_versions = parse_versions(args.versions, current=current, all_versions=all_versions)
        unknown = set(selected_versions) - set(all_versions)
        if unknown:
            raise SystemExit(f"selected versions are not TidesDB releases: {', '.join(sorted(unknown))}")
        print("Surveying crates.io source crates...")
        crates_state = fetch_crates_state(selected_versions)
        default_version = args.default_version or (
            feature_to_version(current_default)
            if current_default and feature_to_version(current_default) in selected_versions
            else selected_versions[-1]
        )
        if default_version not in selected_versions:
            raise SystemExit("--default-version must be one of --versions")
        needed = [version for version in selected_versions if crates_state[version].needs_publish]
        missing = [version for version in selected_versions if crates_state[version].status == "missing"]
        if args.skip_publish:
            publish_versions = []
        elif args.publish_versions is None:
            publish_versions = needed
        else:
            publish_versions = parse_publish_versions(
                args.publish_versions,
                selected_versions,
                needed,
                missing,
            )
    else:
        print("Surveying crates.io source crates...")
        crates_state = fetch_crates_state(all_versions)
        selected_versions, default_version, publish_versions = choose_interactively(
            all_versions,
            current,
            current_default,
            crates_state,
        )
        if args.skip_publish:
            publish_versions = []
        elif args.publish_versions is not None:
            needed = [version for version in selected_versions if crates_state[version].needs_publish]
            missing = [version for version in selected_versions if crates_state[version].status == "missing"]
            publish_versions = parse_publish_versions(
                args.publish_versions,
                selected_versions,
                needed,
                missing,
            )

    files_to_modify = []
    if not args.source_crates_only:
        files_to_modify = files_to_modify_for_plan(
            cargo,
            selected_versions,
            default_version,
            crates_state,
        )

    summarize(
        selected_versions,
        default_version,
        existing_features,
        crates_state,
        publish_versions,
        files_to_modify,
        args.publish and not args.publish_dry_run,
        args.source_crates_only,
    )

    blocking_versions = [
        version for version in selected_versions if crates_state[version].is_blocking
    ]
    if blocking_versions:
        print("\nCannot continue while source crate state is unknown:")
        for version in blocking_versions:
            state = crates_state[version]
            print(f"  {source_crate_name(version)}: {'; '.join(state.reasons)}")
        raise SystemExit(1)

    if args.dry_run:
        print("\n--dry-run: no files modified and no crates published.")
        return

    if not args.yes:
        confirmed = inquirer.confirm(message="Apply this plan?", default=False).execute()
        if not confirmed:
            print("Aborted.")
            return

    if not args.source_crates_only:
        update_cargo_toml(cargo, selected_versions, default_version, crates_state)
        save_cargo_toml(cargo)
        update_build_rs(selected_versions, default_version)
        update_ci_default(default_version)

    if not publish_versions:
        print("Done.")
        return

    if args.work_dir:
        work_root = args.work_dir
        work_root.mkdir(parents=True, exist_ok=True)
        cleanup = False
    else:
        tempdir = tempfile.TemporaryDirectory(prefix="tidesdb-src-crates-")
        work_root = Path(tempdir.name)
        cleanup = not args.keep_work_dir

    try:
        for version in publish_versions:
            state = crates_state[version]
            crate_dir = write_source_crate(version, state.publish_version, work_root)
            publish = args.publish and not args.publish_dry_run
            action = "Publishing" if publish else "Checking"
            print(f"{action} {source_crate_name(version)}@{state.publish_version} from {crate_dir}...", flush=True)
            cargo_publish(crate_dir, dry_run=not publish)
    finally:
        if cleanup and "tempdir" in locals():
            tempdir.cleanup()
        else:
            print(f"Kept prepared crates in {work_root}")

    print("Done.")


if __name__ == "__main__":
    main()
