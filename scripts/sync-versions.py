#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "tomlkit",
#     "requests",
#     "InquirerPy",
# ]
# ///

"""Sync tidesdb version feature gates with GitHub releases.

Fetches all releases from tidesdb/tidesdb, presents an interactive picker
for which versions to include as Cargo feature gates, and updates Cargo.toml
and the build.rs fallback default accordingly.
"""

import argparse
import re
import tomlkit
import requests
from InquirerPy import inquirer

CARGO_TOML = "Cargo.toml"
BUILD_RS = "build.rs"
BUILD_AND_TEST_YML = ".github/workflows/build_and_test.yml"
REPO = "tidesdb/tidesdb"


def fetch_versions() -> list[str]:
    """Fetch all release versions from GitHub, sorted ascending."""
    versions = []
    url = f"https://api.github.com/repos/{REPO}/releases"
    page = 1
    while True:
        resp = requests.get(url, params={"per_page": 100, "page": page})
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for release in data:
            tag = release["tag_name"]
            if not tag.startswith("v"):
                continue
            ver = tag[1:]
            parts = ver.split(".")
            if len(parts) != 3:
                continue
            try:
                major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
            except ValueError:
                continue
            if major < 9:
                continue
            versions.append(ver)
        page += 1

    versions.sort(key=lambda v: list(map(int, v.split("."))))
    return versions


def version_to_feature(version: str) -> str:
    return "v" + version.replace(".", "_")


def feature_to_version(feature: str) -> str:
    return feature[1:].replace("_", ".")


def load_cargo_toml() -> tomlkit.TOMLDocument:
    with open(CARGO_TOML, "r") as f:
        return tomlkit.load(f)


def save_cargo_toml(data: tomlkit.TOMLDocument):
    with open(CARGO_TOML, "w") as f:
        tomlkit.dump(data, f)


def current_version_features(cargo: dict) -> set[str]:
    """Return the set of version features currently in Cargo.toml."""
    features = set()
    for key in cargo.get("features", {}):
        if re.match(r"^v\d+_\d+_\d+$", key):
            features.add(key)
    return features


def current_default_version(cargo: dict) -> str | None:
    """Return the current default version feature, if any."""
    for feat in cargo.get("features", {}).get("default", []):
        if re.match(r"^v\d+_\d+_\d+$", feat):
            return feat
    return None


def update_build_rs_default(version: str):
    """Update the fallback default version in build.rs."""
    with open(BUILD_RS, "r") as f:
        content = f.read()

    content = re.sub(
        r'selected\.unwrap_or_else\(\|\| "[\d.]+"\.to_string\(\)\)',
        f'selected.unwrap_or_else(|| "{version}".to_string())',
        content,
    )

    with open(BUILD_RS, "w") as f:
        f.write(content)


def update_ci_default(version: str):
    """Update the pinned tidesdb ref in build_and_test CI workflow."""
    import os
    if not os.path.exists(BUILD_AND_TEST_YML):
        return

    with open(BUILD_AND_TEST_YML, "r") as f:
        content = f.read()

    content = re.sub(
        r"(repository: tidesdb/tidesdb\s+ref: )v[\d.]+",
        rf"\g<1>v{version}",
        content,
    )

    with open(BUILD_AND_TEST_YML, "w") as f:
        f.write(content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be changed without writing files",
    )
    args = parser.parse_args()

    print(f"Fetching releases from {REPO}...")
    all_versions = fetch_versions()
    print(f"Found {len(all_versions)} releases.\n")

    cargo = load_cargo_toml()
    existing_features = current_version_features(cargo)
    current_default = current_default_version(cargo)

    # Build choices for the checkbox picker
    choices = []
    for version in all_versions:
        feature = version_to_feature(version)
        name = f"{feature} ({version})"
        if feature == current_default:
            name += " [current default]"
        choices.append({
            "name": name,
            "value": version,
            "enabled": feature in existing_features,
        })

    selected_versions = inquirer.checkbox(
        message="Select version features:",
        choices=choices,
        instruction="(Space to toggle, Enter to confirm)",
    ).execute()

    if not selected_versions:
        print("No versions selected, aborting.")
        return

    selected_features = [version_to_feature(v) for v in selected_versions]

    # Pick the default version
    default_current = (
        feature_to_version(current_default) if current_default else None
    )
    default_default = (
        default_current if default_current in selected_versions
        else selected_versions[-1]
    )

    default_version = inquirer.select(
        message="Select default version:",
        choices=[
            {"name": f"{version_to_feature(v)} ({v})", "value": v}
            for v in selected_versions
        ],
        default=default_default,
    ).execute()
    default_feature = version_to_feature(default_version)

    # Show summary
    print(f"\nVersion features: {', '.join(selected_features)}")
    print(f"Default: {default_feature} ({default_version})")

    # Compute changes
    added = set(selected_features) - existing_features
    removed = existing_features - set(selected_features)
    default_changed = current_default != default_feature

    if not added and not removed and not default_changed:
        print("\nNo changes needed.")
        return

    # Show what would change
    print("\nChanges:")
    files_to_modify = []
    if added:
        print(f"  Add features:    {', '.join(sorted(added))}")
    if removed:
        print(f"  Remove features: {', '.join(sorted(removed))}")
    if added or removed or default_changed:
        files_to_modify.append(CARGO_TOML)
    if default_changed:
        print(f"  Default:         {current_default or '(none)'} -> {default_feature}")
        files_to_modify.append(BUILD_RS)
        files_to_modify.append(BUILD_AND_TEST_YML)

    print(f"\nFiles to modify: {', '.join(files_to_modify)}")

    if args.dry_run:
        print("\n--dry-run: no files modified.")
        return

    # Update Cargo.toml
    features = cargo.setdefault("features", {})

    # Remove old version features
    for key in list(features.keys()):
        if re.match(r"^v\d+_\d+_\d+$", key):
            del features[key]

    # Update default
    new_default = [f for f in features.get("default", [])
                   if not re.match(r"^v\d+_\d+_\d+$", f)]
    new_default.append(default_feature)
    features["default"] = new_default

    # Add selected version features (sorted)
    for feature in selected_features:
        features[feature] = []

    save_cargo_toml(cargo)

    if default_changed:
        update_build_rs_default(default_version)
        update_ci_default(default_version)

    print("Done.")


if __name__ == "__main__":
    main()
