// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn test_config(dir: &TempDir) -> Config {
    Config::new(dir.path())
        .num_flush_threads(2)
        .num_compaction_threads(2)
        .log_level(LogLevel::Warn)
        .block_cache_size(16 * 1024 * 1024)
        .max_open_sstables(256)
}

fn create_test_db() -> (TidesDB, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db = TidesDB::open(test_config(&temp_dir)).unwrap();
    (db, temp_dir)
}

fn create_cf<'db>(db: &'db TidesDB, name: &str) -> ColumnFamily<'db> {
    db.create_column_family(name, ColumnFamilyConfig::default())
        .unwrap();
    db.get_column_family(name).unwrap()
}

fn put_all(db: &TidesDB, cf: &ColumnFamily<'_>, pairs: &[(&[u8], &[u8])]) {
    let mut txn = db.begin_transaction().unwrap();
    for (k, v) in pairs {
        txn.put(cf, k, v).unwrap();
    }
    txn.commit().unwrap();
}

fn collect_keys(txn: &Transaction<'_>, cf: &ColumnFamily<'_>) -> Vec<Vec<u8>> {
    let mut iter = txn.new_iterator(cf).unwrap();
    iter.seek_to_first().unwrap();
    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(iter.key().unwrap());
        iter.next().unwrap();
    }
    keys
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

// ===== lifecycle and column families =====

#[test]
fn test_version_constant() {
    assert_eq!(TIDESDB_VERSION, "10.2.1");
}

#[test]
fn test_open_close() {
    let (db, _dir) = create_test_db();
    drop(db);
}

#[test]
fn test_second_open_of_same_dir_is_locked() {
    let (_db, dir) = create_test_db();
    let err = TidesDB::open(test_config(&dir)).err().unwrap();
    assert_eq!(err.code(), Some(ErrorCode::Locked));
}

#[test]
fn test_reopen_persists_data() {
    let dir = TempDir::new().unwrap();
    {
        let db = TidesDB::open(test_config(&dir)).unwrap();
        let cf = create_cf(&db, "cf");
        put_all(&db, &cf, &[(b"k", b"v")]);
    }
    let db = TidesDB::open(test_config(&dir)).unwrap();
    let cf = db.get_column_family("cf").unwrap();
    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&cf, b"k").unwrap(), b"v");
}

#[test]
fn test_create_list_drop_column_family() {
    let (mut db, _dir) = create_test_db();
    {
        let cf = create_cf(&db, "test_cf");
        assert_eq!(cf.name(), "test_cf");
    }
    assert!(
        db.list_column_families()
            .unwrap()
            .contains(&"test_cf".to_string())
    );

    db.drop_column_family("test_cf").unwrap();
    assert!(
        !db.list_column_families()
            .unwrap()
            .contains(&"test_cf".to_string())
    );
    assert!(db.get_column_family("test_cf").unwrap_err().is_not_found());
}

#[test]
fn test_create_duplicate_column_family() {
    let (db, _dir) = create_test_db();
    create_cf(&db, "dup");
    let err = db
        .create_column_family("dup", ColumnFamilyConfig::default())
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::Exists));
}

#[test]
fn test_drop_column_family_unchecked() {
    let (db, _dir) = create_test_db();
    create_cf(&db, "shared");
    unsafe { db.drop_column_family_unchecked("shared").unwrap() };
    assert!(db.get_column_family("shared").is_err());
}

#[test]
fn test_rename_column_family_keeps_handle_valid() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "old");
    put_all(&db, &cf, &[(b"k", b"v")]);

    db.rename_column_family("old", "new").unwrap();
    assert!(db.get_column_family("old").is_err());

    // the old handle still refers to the same family
    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&cf, b"k").unwrap(), b"v");
    let renamed = db.get_column_family("new").unwrap();
    assert_eq!(txn.get(&renamed, b"k").unwrap(), b"v");

    // the handle keeps its looked-up name; the stats report the current one
    assert_eq!(cf.name(), "old");
    assert_eq!(cf.get_stats().unwrap().name, "new");
}

#[test]
fn test_clone_column_family() {
    let (db, _dir) = create_test_db();
    let src = create_cf(&db, "src");
    put_all(&db, &src, &[(b"a", b"1"), (b"b", b"2")]);

    db.clone_column_family("src", "dst").unwrap();
    let dst = db.get_column_family("dst").unwrap();

    // later writes to the source are not in the clone
    put_all(&db, &src, &[(b"c", b"3")]);

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&dst, b"a").unwrap(), b"1");
    assert_eq!(txn.get(&dst, b"b").unwrap(), b"2");
    assert!(txn.get(&dst, b"c").unwrap_err().is_not_found());
}

#[test]
fn test_clone_column_family_errors() {
    let (db, _dir) = create_test_db();
    create_cf(&db, "a");
    create_cf(&db, "b");
    assert!(
        db.clone_column_family("missing", "x")
            .unwrap_err()
            .is_not_found()
    );
    assert_eq!(
        db.clone_column_family("a", "b").unwrap_err().code(),
        Some(ErrorCode::Exists)
    );
}

#[test]
fn test_cf_from_other_database_is_rejected() {
    let (db1, _d1) = create_test_db();
    let (db2, _d2) = create_test_db();
    let cf2 = create_cf(&db2, "cf");
    let txn = db1.begin_transaction().unwrap();
    let err = txn.put(&cf2, b"k", b"v").unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidArgs));
}

// ===== reads and writes =====

#[test]
fn test_put_get_delete() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    put_all(&db, &cf, &[(b"key", b"value")]);
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"key").unwrap(),
        b"value"
    );

    let mut txn = db.begin_transaction().unwrap();
    txn.delete(&cf, b"key").unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert!(txn.get(&cf, b"key").unwrap_err().is_not_found());
    assert!(!txn.contains(&cf, b"key").unwrap());
}

#[test]
fn test_empty_value_is_distinct_from_absent() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"empty", b"")]);

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&cf, b"empty").unwrap(), Vec::<u8>::new());
    assert!(txn.contains(&cf, b"empty").unwrap());

    let mut iter = txn.new_iterator(&cf).unwrap();
    iter.seek_to_first().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key_value().unwrap(), (b"empty".to_vec(), Vec::new()));
    assert_eq!(iter.value().unwrap(), Vec::<u8>::new());
}

#[test]
fn test_empty_key_is_rejected() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let txn = db.begin_transaction().unwrap();
    assert!(txn.put(&cf, b"", b"v").is_err());
}

#[test]
fn test_large_value_roundtrip() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let big = vec![0xAB_u8; 1 << 20];
    put_all(&db, &cf, &[(b"big", &big)]);
    db.flush_memtable().unwrap();
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"big").unwrap(),
        big
    );
}

#[test]
fn test_get_notrack_and_contains() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get_notrack(&cf, b"k").unwrap(), b"v");
    assert!(txn.contains(&cf, b"k").unwrap());
    assert!(!txn.contains(&cf, b"missing").unwrap());
    assert!(txn.get_notrack(&cf, b"missing").unwrap_err().is_not_found());
}

#[test]
fn test_multi_operation_transaction() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(
        &db,
        &cf,
        &[
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
        ],
    );

    let txn = db.begin_transaction().unwrap();
    for i in 1..=3 {
        let value = txn.get(&cf, format!("key{i}").as_bytes()).unwrap();
        assert_eq!(value, format!("value{i}").as_bytes());
    }
}

#[test]
fn test_read_your_own_writes() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"pending").unwrap();
    assert_eq!(txn.get(&cf, b"k").unwrap(), b"pending");
}

#[test]
fn test_multi_column_family_transaction() {
    let (db, _dir) = create_test_db();
    let users = create_cf(&db, "users");
    let orders = create_cf(&db, "orders");

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&users, b"u1", b"alice").unwrap();
    txn.put(&orders, b"o1", b"u1").unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&users, b"u1").unwrap(), b"alice");
    assert_eq!(txn.get(&orders, b"o1").unwrap(), b"u1");
    assert!(txn.get(&users, b"o1").unwrap_err().is_not_found());
}

#[test]
fn test_rollback() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"v").unwrap();
    txn.rollback().unwrap();
    drop(txn);

    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );
}

#[test]
fn test_drop_uncommitted_transaction_discards_writes() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    {
        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"k", b"v").unwrap();
    }
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );
}

#[test]
fn test_savepoints() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k1", b"v1").unwrap();
    txn.savepoint("sp1").unwrap();
    txn.put(&cf, b"k2", b"v2").unwrap();
    txn.rollback_to_savepoint("sp1").unwrap();
    txn.savepoint("sp2").unwrap();
    txn.release_savepoint("sp2").unwrap();
    assert!(
        txn.rollback_to_savepoint("nope")
            .unwrap_err()
            .is_not_found()
    );
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&cf, b"k1").unwrap(), b"v1");
    assert!(txn.get(&cf, b"k2").unwrap_err().is_not_found());
}

#[test]
fn test_transaction_reset() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"a", b"1").unwrap();
    txn.commit().unwrap();

    txn.reset(IsolationLevel::Serializable).unwrap();
    txn.put(&cf, b"b", b"2").unwrap();
    txn.commit().unwrap();

    txn.reset(IsolationLevel::ReadCommitted).unwrap();
    txn.put(&cf, b"c", b"3").unwrap();
    txn.rollback().unwrap();
    txn.reset(IsolationLevel::ReadCommitted).unwrap();

    assert_eq!(txn.get(&cf, b"a").unwrap(), b"1");
    assert_eq!(txn.get(&cf, b"b").unwrap(), b"2");
    assert!(txn.get(&cf, b"c").unwrap_err().is_not_found());
}

#[test]
fn test_single_delete() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"once", b"v"), (b"other", b"w")]);

    let mut txn = db.begin_transaction().unwrap();
    txn.single_delete(&cf, b"once").unwrap();
    txn.commit().unwrap();
    db.flush_memtable().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert!(txn.get(&cf, b"once").unwrap_err().is_not_found());
    assert_eq!(txn.get(&cf, b"other").unwrap(), b"w");
}

// ===== range and prefix deletes =====

#[test]
fn test_delete_range() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(
        &db,
        &cf,
        &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
    );

    let mut txn = db.begin_transaction().unwrap();
    txn.delete_range(&cf, b"b", Some(b"d")).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert_eq!(collect_keys(&txn, &cf), vec![b"a".to_vec(), b"d".to_vec()]);
}

#[test]
fn test_delete_range_open_ended_and_survived_by_newer_write() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"a", b"1"), (b"m", b"2"), (b"z", b"3")]);
    db.flush_memtable().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    txn.delete_range(&cf, b"b", None).unwrap();
    txn.commit().unwrap();

    // a write committed after the range delete survives it
    put_all(&db, &cf, &[(b"q", b"new")]);

    let txn = db.begin_transaction().unwrap();
    assert_eq!(collect_keys(&txn, &cf), vec![b"a".to_vec(), b"q".to_vec()]);
}

#[test]
fn test_delete_prefix() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(
        &db,
        &cf,
        &[(b"user:1", b"a"), (b"user:2", b"b"), (b"order:1", b"c")],
    );

    let mut txn = db.begin_transaction().unwrap();
    txn.delete_prefix(&cf, b"user:").unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    assert_eq!(collect_keys(&txn, &cf), vec![b"order:1".to_vec()]);
}

#[test]
fn test_range_delete_bound_validation() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let txn = db.begin_transaction().unwrap();
    assert!(txn.delete_range(&cf, b"", None).is_err());
    assert!(txn.delete_prefix(&cf, b"").is_err());
    assert!(txn.delete_prefix(&cf, &[b'x'; 257]).is_err());
    txn.delete_prefix(&cf, &[b'x'; 256]).unwrap();
}

// ===== TTL =====

#[test]
fn test_put_with_ttl_is_relative() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put_with_ttl(&cf, b"temp", b"v", Duration::from_secs(60))
        .unwrap();
    txn.put(&cf, b"forever", b"v").unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    let expires = txn.get_expiry(&cf, b"temp").unwrap().unwrap();
    let now = now_secs();
    assert!(
        expires >= now + 58 && expires <= now + 61,
        "expires={expires} now={now}"
    );
    assert_eq!(txn.get_expiry(&cf, b"forever").unwrap(), None);
    assert!(txn.get_expiry(&cf, b"missing").unwrap_err().is_not_found());
}

#[test]
fn test_ttl_expires() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put_with_ttl(&cf, b"short", b"v", Duration::from_millis(1))
        .unwrap();
    txn.commit().unwrap();

    // sub-second lifetimes round up to one second
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"short").unwrap(),
        b"v"
    );
    std::thread::sleep(Duration::from_millis(2100));
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"short")
            .unwrap_err()
            .is_not_found()
    );
}

#[test]
fn test_zero_ttl_rejected() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let txn = db.begin_transaction().unwrap();
    let err = txn
        .put_with_ttl(&cf, b"k", b"v", Duration::ZERO)
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidArgs));
}

#[test]
fn test_iterator_ttl() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    txn.put_with_ttl(&cf, b"a", b"1", Duration::from_secs(100))
        .unwrap();
    txn.put(&cf, b"b", b"2").unwrap();
    txn.commit().unwrap();

    let txn = db.begin_transaction().unwrap();
    let mut iter = txn.new_iterator(&cf).unwrap();
    iter.seek_to_first().unwrap();
    assert!(iter.expires_at().unwrap().unwrap() > now_secs());
    iter.next().unwrap();
    assert_eq!(iter.expires_at().unwrap(), None);
}

// ===== iterators =====

#[test]
fn test_iterator_forward_backward() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

    let txn = db.begin_transaction().unwrap();
    assert_eq!(
        collect_keys(&txn, &cf),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );

    let mut iter = txn.new_iterator(&cf).unwrap();
    iter.seek_to_last().unwrap();
    let mut keys = Vec::new();
    while iter.is_valid() {
        keys.push(iter.key().unwrap());
        iter.prev().unwrap();
    }
    assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
}

#[test]
fn test_iterator_seek() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"b", b"1"), (b"d", b"2"), (b"f", b"3")]);

    let txn = db.begin_transaction().unwrap();
    let mut iter = txn.new_iterator(&cf).unwrap();

    iter.seek(b"c").unwrap();
    assert_eq!(iter.key().unwrap(), b"d");
    iter.seek_for_prev(b"e").unwrap();
    assert_eq!(iter.key().unwrap(), b"d");
    iter.seek(b"z").unwrap();
    assert!(!iter.is_valid());
    iter.seek_for_prev(b"a").unwrap();
    assert!(!iter.is_valid());
    assert!(iter.key().unwrap_err().is_not_found());
}

#[test]
fn test_iterator_empty_family() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let txn = db.begin_transaction().unwrap();
    assert!(collect_keys(&txn, &cf).is_empty());
}

#[test]
fn test_iterator_over_flushed_and_memtable_data() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"a", b"1"), (b"c", b"3")]);
    db.flush_memtable().unwrap();
    put_all(&db, &cf, &[(b"b", b"2"), (b"c", b"33")]);

    let txn = db.begin_transaction().unwrap();
    let mut iter = txn.new_iterator(&cf).unwrap();
    iter.seek_to_first().unwrap();
    let mut pairs = Vec::new();
    while iter.is_valid() {
        pairs.push(iter.key_value().unwrap());
        iter.next().unwrap();
    }
    assert_eq!(
        pairs,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"33".to_vec()),
        ]
    );
}

#[test]
fn test_range_iterator() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
        .map(|i| (format!("k{i:03}").into_bytes(), b"v".to_vec()))
        .collect();
    let refs: Vec<(&[u8], &[u8])> = pairs.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    put_all(&db, &cf, &refs);
    db.flush_memtable().unwrap();

    let txn = db.begin_transaction().unwrap();
    let mut iter = txn.new_range_iterator(&cf, b"k010", b"k019").unwrap();
    iter.seek(b"k010").unwrap();
    let mut keys = Vec::new();
    while iter.is_valid() {
        let k = iter.key().unwrap();
        if k.as_slice() > b"k019".as_slice() {
            break;
        }
        keys.push(k);
        iter.next().unwrap();
    }
    assert_eq!(keys.len(), 10);
    assert_eq!(keys[0], b"k010");
    assert_eq!(keys[9], b"k019");
}

#[test]
fn test_iterator_does_not_leak_on_many_reads() {
    // Exercises the key/value buffer frees; under a leak checker this would report.
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);
    let txn = db.begin_transaction().unwrap();
    let mut iter = txn.new_iterator(&cf).unwrap();
    iter.seek_to_first().unwrap();
    for _ in 0..10_000 {
        let _ = iter.key().unwrap();
        let _ = iter.value().unwrap();
        let _ = iter.key_value().unwrap();
    }
}

// ===== isolation, snapshots, point-in-time reads =====

#[test]
fn test_isolation_levels_begin() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    for level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Snapshot,
        IsolationLevel::Serializable,
    ] {
        let mut txn = db.begin_transaction_with_isolation(level).unwrap();
        txn.put(&cf, b"k", b"v").unwrap();
        txn.commit().unwrap();
    }
}

#[test]
fn test_repeatable_read_does_not_see_later_commits() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"old")]);

    let reader = db
        .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
        .unwrap();
    assert_eq!(reader.get(&cf, b"k").unwrap(), b"old");

    put_all(&db, &cf, &[(b"k", b"new")]);
    assert_eq!(reader.get(&cf, b"k").unwrap(), b"old");

    let rc = db.begin_transaction().unwrap();
    assert_eq!(rc.get(&cf, b"k").unwrap(), b"new");
}

#[test]
fn test_snapshot_isolation_write_conflict() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"0")]);

    let mut t1 = db
        .begin_transaction_with_isolation(IsolationLevel::Snapshot)
        .unwrap();
    let mut t2 = db
        .begin_transaction_with_isolation(IsolationLevel::Snapshot)
        .unwrap();
    t1.put(&cf, b"k", b"1").unwrap();
    t2.put(&cf, b"k", b"2").unwrap();
    t1.commit().unwrap();
    let err = t2.commit().unwrap_err();
    assert!(err.is_conflict(), "{err}");

    let stats = db.get_db_stats().unwrap();
    assert!(stats.txn_conflicts >= 1);
}

#[test]
fn test_begin_transaction_cf_uses_family_isolation() {
    let (db, _dir) = create_test_db();
    db.create_column_family(
        "rr",
        ColumnFamilyConfig::default().default_isolation_level(IsolationLevel::RepeatableRead),
    )
    .unwrap();
    let cf = db.get_column_family("rr").unwrap();
    put_all(&db, &cf, &[(b"k", b"old")]);

    let txn = db.begin_transaction_cf(&cf).unwrap();
    assert_eq!(txn.get(&cf, b"k").unwrap(), b"old");
    put_all(&db, &cf, &[(b"k", b"new")]);
    assert_eq!(txn.get(&cf, b"k").unwrap(), b"old");
}

#[test]
fn test_snapshot_point_in_time_read() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v1")]);

    let snap = db.snapshot().unwrap();
    assert!(snap.seq() > 0);

    put_all(&db, &cf, &[(b"k", b"v2"), (b"other", b"x")]);
    db.flush_memtable().unwrap();

    let old = db.begin_transaction_at_snapshot(&snap).unwrap();
    assert_eq!(old.get(&cf, b"k").unwrap(), b"v1");
    assert!(old.get(&cf, b"other").unwrap_err().is_not_found());

    let now = db.begin_transaction().unwrap();
    assert_eq!(now.get(&cf, b"k").unwrap(), b"v2");
}

#[test]
fn test_snapshot_shared_across_threads() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v1")]);
    let snap = db.snapshot().unwrap();
    put_all(&db, &cf, &[(b"k", b"v2")]);

    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let txn = db.begin_transaction_at_snapshot(&snap).unwrap();
                assert_eq!(txn.get(&cf, b"k").unwrap(), b"v1");
            });
        }
    });
}

#[test]
fn test_begin_at_seq_and_read_snapshot() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v1")]);

    // hold the floor with a repeatable read transaction and remember its sequence
    let holder = db
        .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
        .unwrap();
    let seq = holder.read_snapshot();
    assert!(seq > 0 && seq != u64::MAX);
    assert!(db.oldest_readable_seq() <= seq);

    put_all(&db, &cf, &[(b"k", b"v2")]);

    let at = db.begin_transaction_at_seq(seq).unwrap();
    assert_eq!(at.get(&cf, b"k").unwrap(), b"v1");

    let ru = db
        .begin_transaction_with_isolation(IsolationLevel::ReadUncommitted)
        .unwrap();
    assert_eq!(ru.read_snapshot(), u64::MAX);
}

// ===== transaction lifecycle, timeouts, aborts =====

#[test]
fn test_transaction_state() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(txn.state().unwrap(), TxnState::Active);
    txn.put(&cf, b"k", b"v").unwrap();
    txn.commit().unwrap();
    assert_eq!(txn.state().unwrap(), TxnState::Committed);

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"v").unwrap();
    txn.rollback().unwrap();
    assert_eq!(txn.state().unwrap(), TxnState::Aborted);
}

#[test]
fn test_transaction_timeout() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    txn.set_timeout(Some(Duration::from_secs(1))).unwrap();
    txn.put(&cf, b"k", b"v").unwrap();
    std::thread::sleep(Duration::from_millis(3200));
    let err = txn.put(&cf, b"k2", b"v").unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::TxnExpired));

    let mut txn = db.begin_transaction().unwrap();
    txn.set_timeout(Some(Duration::from_secs(1))).unwrap();
    txn.set_timeout(None).unwrap();
    std::thread::sleep(Duration::from_millis(3200));
    txn.put(&cf, b"k", b"v").unwrap();
    txn.commit().unwrap();
}

#[test]
fn test_db_level_txn_timeout() {
    let dir = TempDir::new().unwrap();
    let db = TidesDB::open(test_config(&dir).txn_timeout_seconds(1)).unwrap();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    std::thread::sleep(Duration::from_millis(3200));
    // the first operation past the deadline expires the transaction
    assert_eq!(
        txn.put(&cf, b"k", b"v").unwrap_err().code(),
        Some(ErrorCode::TxnExpired)
    );
    assert!(txn.commit().is_err());
}

#[test]
fn test_abort_handle_from_other_thread() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"v").unwrap();

    let handle = txn.abort_handle();
    std::thread::scope(|s| {
        s.spawn(move || handle.request_abort());
    });

    let err = txn.commit().unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::TxnAborted));
    drop(txn);
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );
}

#[test]
fn test_abort_handle_outliving_transaction() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let handle;
    {
        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"k", b"v").unwrap();
        handle = txn.abort_handle();
    }
    // the transaction was rolled back on drop; aborting it now is a no-op
    handle.request_abort();
    drop(handle);
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );
}

// ===== two-phase commit =====

#[test]
fn test_two_phase_commit() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"v").unwrap();
    txn.prepare(b"xid-1").unwrap();
    assert_eq!(txn.state().unwrap(), TxnState::Prepared);

    // prepared writes are invisible
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );

    txn.commit_prepared().unwrap();
    assert_eq!(txn.state().unwrap(), TxnState::Committed);
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"k").unwrap(),
        b"v"
    );
}

#[test]
fn test_two_phase_rollback() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"k", b"v").unwrap();
    txn.prepare(b"xid-2").unwrap();
    txn.rollback_prepared().unwrap();
    drop(txn);
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );
}

#[test]
fn test_commit_prepared_requires_prepare() {
    let (db, _dir) = create_test_db();
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(
        txn.commit_prepared().unwrap_err().code(),
        Some(ErrorCode::InvalidArgs)
    );
}

#[test]
fn test_recover_prepared_after_restart() {
    let dir = TempDir::new().unwrap();
    {
        let db = TidesDB::open(test_config(&dir)).unwrap();
        let cf = create_cf(&db, "cf");
        let mut txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"k", b"v").unwrap();
        txn.prepare(b"xid-recover").unwrap();
        // the coordinator goes away: dropping a prepared transaction leaves it in doubt on disk
        drop(txn);
    }

    let db = TidesDB::open(test_config(&dir)).unwrap();
    let cf = db.get_column_family("cf").unwrap();
    assert!(
        db.begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap_err()
            .is_not_found()
    );

    let mut recovered = db.recover_prepared().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].xid, b"xid-recover");
    assert_eq!(recovered[0].txn.state().unwrap(), TxnState::Prepared);
    recovered[0].txn.commit_prepared().unwrap();
    drop(recovered);

    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"k").unwrap(),
        b"v"
    );
}

// ===== commit hooks =====

#[test]
fn test_commit_hook_puts_and_deletes() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    type Batches = Arc<Mutex<Vec<(Vec<CommitOp>, u64)>>>;
    let seen: Batches = Arc::default();
    let sink = seen.clone();
    cf.set_commit_hook(move |ops, seq| {
        sink.lock().unwrap().push((ops.to_vec(), seq));
        0
    })
    .unwrap();

    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"a", b"1").unwrap();
    txn.put_with_ttl(&cf, b"b", b"", Duration::from_secs(100))
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    txn.delete(&cf, b"a").unwrap();
    txn.commit().unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    let (batch, seq1) = &seen[0];
    assert_eq!(batch.len(), 2);
    let a = batch.iter().find(|op| op.key == b"a").unwrap();
    assert_eq!(a.value.as_deref(), Some(&b"1"[..]));
    assert_eq!(a.expires_at, None);
    let b = batch.iter().find(|op| op.key == b"b").unwrap();
    assert_eq!(b.value.as_deref(), Some(&b""[..]));
    assert!(b.expires_at.unwrap() > now_secs());

    let (batch, seq2) = &seen[1];
    assert!(batch[0].is_delete && batch[0].value.is_none());
    assert_eq!(batch[0].range_end, None);
    assert!(seq2 > seq1);
    assert_eq!(a.range_end, None);
}

#[test]
fn test_commit_hook_range_and_prefix_deletes() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let seen: Arc<Mutex<Vec<CommitOp>>> = Arc::default();
    let sink = seen.clone();
    cf.set_commit_hook(move |ops, _| {
        sink.lock().unwrap().extend_from_slice(ops);
        0
    })
    .unwrap();

    let commit = |f: &dyn Fn(&Transaction<'_>) -> Result<()>| {
        let mut txn = db.begin_transaction().unwrap();
        f(&txn).unwrap();
        txn.commit().unwrap();
    };
    commit(&|t| t.delete_range(&cf, b"b", Some(b"d")));
    commit(&|t| t.delete_prefix(&cf, b"user:"));
    commit(&|t| t.delete_prefix(&cf, b"ab\xff\xff"));
    commit(&|t| t.delete_range(&cf, b"x", None));
    commit(&|t| t.delete_prefix(&cf, b"\xff"));
    commit(&|t| t.single_delete(&cf, b"s"));

    let seen = seen.lock().unwrap();
    let ops: Vec<_> = seen
        .iter()
        .map(|op| {
            assert!(op.is_delete);
            assert_eq!(op.value, None);
            (op.key.as_slice(), op.range_end.as_deref())
        })
        .collect();
    assert_eq!(
        ops,
        vec![
            (&b"b"[..], Some(&b"d"[..])),
            (&b"user:"[..], Some(&b"user;"[..])),
            (&b"ab\xff\xff"[..], Some(&b"ac"[..])),
            // unbounded above: indistinguishable from a point delete, as documented
            (&b"x"[..], None),
            (&b"\xff"[..], None),
            (&b"s"[..], None),
        ]
    );
}

#[test]
fn test_delete_range_rejects_empty_upper_bound() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    txn.put(&cf, b"m", b"v").unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let err = txn.delete_range(&cf, b"a", Some(b"")).unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidArgs));
    assert_eq!(
        txn.delete_range(&cf, b"z", Some(b"a")).unwrap_err().code(),
        Some(ErrorCode::InvalidArgs)
    );
    txn.commit().unwrap();
    assert!(db.begin_transaction().unwrap().contains(&cf, b"m").unwrap());
}

#[test]
fn test_commit_hook_replace_and_clear() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));

    let f = first.clone();
    cf.set_commit_hook(move |_, _| {
        f.fetch_add(1, Ordering::SeqCst);
        0
    })
    .unwrap();
    put_all(&db, &cf, &[(b"k", b"1")]);

    let s = second.clone();
    cf.set_commit_hook(move |_, _| {
        s.fetch_add(1, Ordering::SeqCst);
        0
    })
    .unwrap();
    put_all(&db, &cf, &[(b"k", b"2")]);

    cf.clear_commit_hook().unwrap();
    put_all(&db, &cf, &[(b"k", b"3")]);

    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 1);
}

#[test]
fn test_commit_hook_outlives_handle() {
    let (db, _dir) = create_test_db();
    let count = Arc::new(AtomicUsize::new(0));
    {
        let cf = create_cf(&db, "cf");
        let c = count.clone();
        cf.set_commit_hook(move |_, _| {
            c.fetch_add(1, Ordering::SeqCst);
            0
        })
        .unwrap();
    }
    // a fresh handle to the same family: the hook is still installed
    let cf = db.get_column_family("cf").unwrap();
    put_all(&db, &cf, &[(b"k", b"v")]);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_commit_hook_panic_does_not_abort_commit() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    cf.set_commit_hook(|_, _| panic!("hook failure")).unwrap();
    put_all(&db, &cf, &[(b"k", b"v")]);
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"k").unwrap(),
        b"v"
    );
}

#[test]
fn test_commit_hook_concurrent_commits() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let total = Arc::new(AtomicU64::new(0));
    let t = total.clone();
    cf.set_commit_hook(move |ops, _| {
        t.fetch_add(ops.len() as u64, Ordering::SeqCst);
        0
    })
    .unwrap();

    std::thread::scope(|s| {
        for thread in 0..4 {
            let (db, cf) = (&db, &cf);
            s.spawn(move || {
                for i in 0..50 {
                    let mut txn = db.begin_transaction().unwrap();
                    txn.put(cf, format!("t{thread}-{i}").as_bytes(), b"v")
                        .unwrap();
                    txn.commit().unwrap();
                }
            });
        }
    });
    assert_eq!(total.load(Ordering::SeqCst), 200);
}

// ===== configuration =====

#[test]
fn test_default_config_tracks_library() {
    let config = Config::default();
    assert!(config.db_path.is_empty());
    assert!(config.num_flush_threads > 0);
    assert_eq!(config.txn_timeout_seconds, 0);

    let cf = ColumnFamilyConfig::default();
    assert!(cf.level_size_ratio > 0);
    assert!(cf.enable_bloom_filter);
    assert!(cf.encoding_pipeline.is_empty());
    assert_eq!(cf.default_isolation_level, IsolationLevel::ReadCommitted);
}

#[test]
fn test_full_db_config_opens() {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path())
        .num_flush_threads(1)
        .num_compaction_threads(1)
        .log_level(LogLevel::None)
        .log_to_file(true)
        .log_truncation_at(1 << 20)
        .memtable_write_buffer_size(4 << 20)
        .memtable_skip_list_max_level(10)
        .memtable_skip_list_probability(0.25)
        .memtable_sync_mode(SyncMode::Interval)
        .memtable_sync_interval_us(10_000)
        .value_separation_threshold(1024)
        .vlog_segment_size(64 << 20)
        .memtable_l0_queue_stall_threshold(16)
        .memtable_idle_flush_seconds(30)
        .txn_timeout_seconds(0);
    let db = TidesDB::open(config).unwrap();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);
    db.sync_wal().unwrap();
}

#[test]
fn test_custom_cf_config_roundtrip_through_stats() {
    let (db, _dir) = create_test_db();
    let config = ColumnFamilyConfig::new()
        .level_size_ratio(8)
        .min_levels(3)
        .dividing_level_offset(1)
        .keep_values_inline(true)
        .btree_klog_block_size(8192)
        .enable_bloom_filter(true)
        .bloom_fpr(0.001)
        .default_isolation_level(IsolationLevel::Snapshot)
        .l1_file_count_trigger(6)
        .tombstone_density_trigger(0.4)
        .tombstone_density_min_entries(512);
    db.create_column_family("custom", config.clone()).unwrap();
    let cf = db.get_column_family("custom").unwrap();
    assert_eq!(cf.get_stats().unwrap().config, config);
}

#[test]
fn test_compression_pipeline() {
    let (db, _dir) = create_test_db();
    assert!(CompressionAlgorithm::None.is_available());

    let available: Vec<_> = [
        CompressionAlgorithm::Snappy,
        CompressionAlgorithm::Lz4,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Lz4Fast,
    ]
    .into_iter()
    .filter(|a| a.is_available())
    .collect();
    // the features only choose the codecs of the bundled build; a system TidesDB has its own
    if cfg!(not(tidesdb_system)) {
        assert_eq!(
            CompressionAlgorithm::Lz4.is_available(),
            cfg!(feature = "lz4")
        );
        assert_eq!(
            CompressionAlgorithm::Zstd.is_available(),
            cfg!(feature = "zstd")
        );
        assert_eq!(
            CompressionAlgorithm::Snappy.is_available(),
            cfg!(feature = "snappy")
        );
    }

    for algo in available {
        let name = format!("{algo:?}");
        let config = ColumnFamilyConfig::new().compression_algorithm(algo);
        db.create_column_family(&name, config).unwrap();
        let cf = db.get_column_family(&name).unwrap();
        assert_eq!(cf.get_stats().unwrap().config.encoding_pipeline, vec![algo]);

        let value = vec![b'x'; 64 * 1024];
        put_all(&db, &cf, &[(b"k", &value)]);
        db.flush_memtable().unwrap();
        assert_eq!(
            db.begin_transaction().unwrap().get(&cf, b"k").unwrap(),
            value
        );
    }

    let none = ColumnFamilyConfig::new().compression_algorithm(CompressionAlgorithm::None);
    assert!(none.encoding_pipeline.is_empty());
}

#[test]
fn test_encoding_pipeline_too_long() {
    let (db, _dir) = create_test_db();
    let config = ColumnFamilyConfig::new().encoding_pipeline(vec![CompressionAlgorithm::Lz4; 9]);
    let err = db.create_column_family("long", config).unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::InvalidArgs));
}

#[test]
fn test_update_runtime_config() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let updated = ColumnFamilyConfig::default()
        .l1_file_count_trigger(9)
        .bloom_fpr(0.02);
    cf.update_runtime_config(&updated, true).unwrap();
    let stats = cf.get_stats().unwrap();
    assert_eq!(stats.config.l1_file_count_trigger, 9);
    assert_eq!(stats.config.bloom_fpr, 0.02);
}

#[test]
fn test_raise_open_file_limit() {
    let current = raise_open_file_limit(0);
    assert!(current > 0);
    assert!(raise_open_file_limit(current) >= current);
}

// ===== maintenance =====

#[test]
fn test_flush_compact_and_status() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    for batch in 0..4 {
        let mut txn = db.begin_transaction().unwrap();
        for i in 0..100 {
            txn.put(&cf, format!("key{batch}-{i:03}").as_bytes(), b"value")
                .unwrap();
        }
        txn.commit().unwrap();
        db.flush_memtable().unwrap();
    }
    let _ = db.is_flushing();
    let _ = cf.is_compacting();

    match cf.compact() {
        Ok(()) => {}
        Err(e) if e.is_locked() => {}
        Err(e) => panic!("compact failed: {e}"),
    }

    let txn = db.begin_transaction().unwrap();
    assert_eq!(txn.get(&cf, b"key2-050").unwrap(), b"value");
    assert_eq!(collect_keys(&txn, &cf).len(), 400);
}

#[test]
fn test_compact_range() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"a", b"1"), (b"m", b"2"), (b"z", b"3")]);
    db.flush_memtable().unwrap();

    match cf.compact_range(Some(b"a"), Some(b"n")) {
        Ok(()) => {}
        Err(e) if e.is_locked() => {}
        Err(e) => panic!("compact_range failed: {e}"),
    }
    cf.compact_range(Some(b"m"), None).ok();
    assert_eq!(
        cf.compact_range(None, None).unwrap_err().code(),
        Some(ErrorCode::InvalidArgs)
    );
    assert_eq!(
        db.begin_transaction().unwrap().get(&cf, b"m").unwrap(),
        b"2"
    );
}

#[test]
fn test_sync_wal_and_checkpoint() {
    let dir = TempDir::new().unwrap();
    let db = TidesDB::open(test_config(&dir).memtable_sync_mode(SyncMode::None)).unwrap();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);
    db.sync_wal().unwrap();
    db.checkpoint().unwrap();
    assert!(cf.get_stats().unwrap().total_keys >= 1);
}

#[test]
fn test_backup_is_openable() {
    let (db, dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);

    let backup_dir = dir.path().join("backup");
    db.backup(&backup_dir).unwrap();

    // a non-empty destination is refused
    assert!(db.backup(&backup_dir).is_err());

    let restored = TidesDB::open(Config::new(&backup_dir).log_level(LogLevel::Warn)).unwrap();
    let cf = restored.get_column_family("cf").unwrap();
    assert_eq!(
        restored
            .begin_transaction()
            .unwrap()
            .get(&cf, b"k")
            .unwrap(),
        b"v"
    );
}

// ===== statistics =====

#[test]
fn test_cf_stats_after_flush() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..200 {
        txn.put(&cf, format!("key{i:04}").as_bytes(), b"value")
            .unwrap();
    }
    txn.commit().unwrap();

    let before = cf.get_stats().unwrap();
    assert!(before.unflushed_key_count > 0);
    assert!(before.user_bytes_written > 0);

    db.flush_memtable().unwrap();
    let stats = cf.get_stats().unwrap();
    assert!(stats.num_levels >= 1);
    assert_eq!(stats.level_sizes.len(), stats.num_levels as usize);
    assert_eq!(stats.level_num_sstables.len(), stats.num_levels as usize);
    assert_eq!(stats.level_key_counts.len(), stats.num_levels as usize);
    assert_eq!(
        stats.level_tombstone_counts.len(),
        stats.num_levels as usize
    );
    assert_eq!(stats.total_keys, 200);
    assert!(stats.total_data_size > 0);
    assert!(stats.avg_key_size > 0.0);

    // the flush counter is bumped just after the sstable is published, so it can lag the
    // flush_memtable return by a moment
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while cf.get_stats().unwrap().flush_bytes_written == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "flush_bytes_written stayed 0"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn test_tombstone_stats_after_deletes() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..50 {
        txn.put(&cf, format!("k{i:02}").as_bytes(), b"v").unwrap();
    }
    txn.commit().unwrap();
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..25 {
        txn.delete(&cf, format!("k{i:02}").as_bytes()).unwrap();
    }
    txn.commit().unwrap();
    db.flush_memtable().unwrap();

    let stats = cf.get_stats().unwrap();
    assert!(stats.total_tombstones > 0);
    assert!(stats.tombstone_ratio > 0.0);
    assert!(stats.max_sst_density > 0.0);
    assert!(stats.max_sst_density_level >= 1);
}

#[test]
fn test_estimate_cardinality_and_range_stats() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..100 {
        txn.put(&cf, format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    txn.commit().unwrap();

    // memtable-aware: counts unflushed keys too
    let rs = cf.range_stats(b"k010", b"k020").unwrap();
    assert_eq!(rs.estimated_keys, 10);
    assert!(rs.keys_exact);

    db.flush_memtable().unwrap();
    let estimate = cf.estimate_cardinality().unwrap();
    assert!((50..=200).contains(&estimate), "estimate={estimate}");
    let rs = cf.range_stats(b"k000", b"k100").unwrap();
    assert!(rs.sstables_overlapping >= 1);
    assert!(rs.estimated_keys > 0);
}

#[test]
fn test_db_stats() {
    let (db, _dir) = create_test_db();
    let a = create_cf(&db, "a");
    let b = create_cf(&db, "b");
    put_all(&db, &a, &[(b"k", b"v")]);
    put_all(&db, &b, &[(b"k", b"v")]);

    let stats = db.get_db_stats().unwrap();
    assert_eq!(stats.num_column_families, 2);
    assert!(stats.global_seq > 0);
    assert!(stats.txn_commits >= 2);
    assert!(stats.memtable_bytes > 0);
    assert!(stats.user_bytes_written > 0);
    assert!(stats.wal_bytes_written > 0);

    db.flush_memtable().unwrap();
    let stats = db.get_db_stats().unwrap();
    assert!(stats.flush_count >= 1);
    assert!(stats.total_sstable_count >= 1);
}

#[test]
fn test_cache_stats() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);
    db.flush_memtable().unwrap();
    for _ in 0..5 {
        db.begin_transaction().unwrap().get(&cf, b"k").unwrap();
    }
    let stats = db.get_cache_stats().unwrap();
    assert!(stats.enabled);
    assert!(stats.num_partitions > 0);
    assert!(stats.hits + stats.misses > 0);
}

#[test]
fn test_stall_and_io_stats() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    put_all(&db, &cf, &[(b"k", b"v")]);
    db.flush_memtable().unwrap();

    let stalls = db.get_stall_stats().unwrap();
    let names: Vec<_> = stalls.iter().map(|(name, _)| name).collect();
    assert_eq!(names.len(), 5);
    assert!(
        names.iter().all(|n| !n.is_empty() && *n != "unknown"),
        "{names:?}"
    );

    let io = db.get_io_stats().unwrap();
    let names: Vec<_> = io.iter().map(|(name, _)| name).collect();
    assert_eq!(names.len(), 3);
    assert!(names.iter().all(|n| *n != "unknown"), "{names:?}");
    assert!(io.sstable.bytes > 0);
}

#[test]
fn test_encoding_stats() {
    let (db, _dir) = create_test_db();
    let cf = create_cf(&db, "cf");
    let value = vec![b'v'; 8192];
    put_all(&db, &cf, &[(b"k", &value)]);
    db.flush_memtable().unwrap();

    let klog = db.get_klog_encoding_stats().unwrap();
    assert!(!klog.is_empty());
    let verbatim = klog.iter().find(|e| e.ids.is_empty()).unwrap();
    assert!(verbatim.item_count >= 1);
    assert!(verbatim.logical_bytes > 0);

    // just exercise the call; whether anything separated depends on the threshold
    let _ = db.get_vlog_encoding_stats().unwrap();
}

// ===== errors =====

#[test]
fn test_error_codes_roundtrip() {
    for code in -17..=-1 {
        let ec = ErrorCode::from_code(code);
        assert_eq!(ec.code(), code, "{ec:?}");
        assert!(!ec.to_string().is_empty());
    }
    assert_eq!(ErrorCode::from_code(-99), ErrorCode::Unknown);
    assert_eq!(ErrorCode::from_code(-14), ErrorCode::TxnExpired);
    assert_eq!(ErrorCode::from_code(-15), ErrorCode::NoSpace);
    assert_eq!(ErrorCode::from_code(-16), ErrorCode::TxnAborted);
    assert_eq!(ErrorCode::from_code(-17), ErrorCode::TooOld);
}

#[test]
fn test_error_display_uses_library_message() {
    let err = Error::from_code(-3, "lookup");
    let text = err.to_string();
    assert!(text.starts_with("lookup: "), "{text}");
    assert!(text.len() > "lookup: ".len());
}

#[test]
fn test_linked_version_matches_major() {
    // crate N.x binds TidesDB (N+9).x
    let crate_major: u32 = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
    let major = |v: &str| v.split('.').next().unwrap().parse::<u32>().unwrap();
    assert_eq!(major(TIDESDB_VERSION), crate_major + 9);
    assert_eq!(major(TIDESDB_LINKED_VERSION), major(TIDESDB_VERSION));
    if cfg!(not(tidesdb_system)) {
        assert_eq!(TIDESDB_LINKED_VERSION, TIDESDB_VERSION);
    }
}

/// The allocator features must actually replace the process allocator.
#[cfg(target_os = "linux")]
#[test]
fn test_allocator_feature_is_linked() {
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
    for (enabled, name) in [
        (cfg!(feature = "jemalloc"), "libjemalloc"),
        (cfg!(feature = "mimalloc"), "libmimalloc"),
        (cfg!(feature = "tcmalloc"), "libtcmalloc"),
    ] {
        assert_eq!(maps.contains(name), enabled, "{name} mapped: {}", !enabled);
    }
}
