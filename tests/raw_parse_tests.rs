//! Tests for parse_raw functionality.
//!
//! These tests verify that parse_raw produces equivalent results to parse.
//! Tests are split into modules for maintainability.

#![allow(non_snake_case)]
#![cfg(test)]

#[macro_use]
mod support;

mod raw_parse;

// Re-export the benchmark test at the top level
use pg_query::{deparse, deparse_raw, parse, parse_raw, parse_raw_iter};
use std::time::{Duration, Instant};

/// Benchmark comparing parse_raw vs parse performance
#[test]
fn benchmark_parse_raw_vs_parse() {
    // Complex query with multiple features: CTEs, JOINs, subqueries, window functions, etc.
    let query = r#"
        WITH RECURSIVE
            category_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM categories c
                INNER JOIN category_tree ct ON c.parent_id = ct.id
                WHERE ct.depth < 10
            ),
            recent_orders AS (
                SELECT
                    o.id,
                    o.user_id,
                    o.total_amount,
                    o.created_at,
                    ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at DESC) as rn
                FROM orders o
                WHERE o.created_at > NOW() - INTERVAL '30 days'
                    AND o.status IN ('completed', 'shipped', 'delivered')
            )
        SELECT
            u.id AS user_id,
            u.email,
            u.first_name || ' ' || u.last_name AS full_name,
            COALESCE(ua.city, 'Unknown') AS city,
            COUNT(DISTINCT ro.id) AS order_count,
            SUM(ro.total_amount) AS total_spent,
            AVG(ro.total_amount) AS avg_order_value,
            MAX(ro.created_at) AS last_order_date,
            now(),
            current_timestamp,
            CASE
                WHEN SUM(ro.total_amount) > 10000 THEN 'platinum'
                WHEN SUM(ro.total_amount) > 5000 THEN 'gold'
                WHEN SUM(ro.total_amount) > 1000 THEN 'silver'
                ELSE 'bronze'
            END AS customer_tier,
            (
                SELECT COUNT(*)
                FROM user_reviews ur
                WHERE ur.user_id = u.id AND ur.rating >= 4
            ) AS positive_reviews,
            ARRAY_AGG(DISTINCT ct.name ORDER BY ct.name) FILTER (WHERE ct.depth = 1) AS top_categories
        FROM users u
        LEFT JOIN user_addresses ua ON ua.user_id = u.id AND ua.is_primary = true
        LEFT JOIN recent_orders ro ON ro.user_id = u.id AND ro.rn <= 5
        LEFT JOIN order_items oi ON oi.order_id = ro.id
        LEFT JOIN products p ON p.id = oi.product_id
        LEFT JOIN category_tree ct ON ct.id = p.category_id
        WHERE u.is_active = true
            AND u.created_at < NOW() - INTERVAL '7 days'
            AND EXISTS (
                SELECT 1 FROM user_logins ul
                WHERE ul.user_id = u.id
                AND ul.logged_in_at > NOW() - INTERVAL '90 days'
            )
        GROUP BY u.id, u.email, u.first_name, u.last_name, ua.city
        HAVING COUNT(DISTINCT ro.id) > 0
        ORDER BY total_spent DESC NULLS LAST, u.created_at ASC
        LIMIT 100
        OFFSET 0
        FOR UPDATE OF u SKIP LOCKED"#;

    // Warm up
    for _ in 0..10 {
        let _ = parse_raw_iter(query).unwrap();
        let _ = parse_raw(query).unwrap();
        let _ = parse(query).unwrap();
    }

    // Run for a fixed duration to get stable measurements
    let target_duration = Duration::from_secs(5);

    // Benchmark parse_raw
    let mut raw_iterations = 0u64;
    let raw_start = Instant::now();
    while raw_start.elapsed() < target_duration {
        for _ in 0..100 {
            let _ = parse_raw(query).unwrap();
            raw_iterations += 1;
        }
    }
    let raw_elapsed = raw_start.elapsed();
    let raw_ns_per_iter = raw_elapsed.as_nanos() as f64 / raw_iterations as f64;

    // Benchmark parse_raw_iter
    let mut raw_iter_iterations = 0u64;
    let raw_start = Instant::now();
    while raw_start.elapsed() < target_duration {
        for _ in 0..100 {
            let _ = parse_raw_iter(query).unwrap();
            raw_iter_iterations += 1;
        }
    }
    let raw_iter_elapsed = raw_start.elapsed();
    let raw_iter_ns_per_iter = raw_iter_elapsed.as_nanos() as f64 / raw_iter_iterations as f64;

    // Benchmark parse (protobuf)
    let mut proto_iterations = 0u64;
    let proto_start = Instant::now();
    while proto_start.elapsed() < target_duration {
        for _ in 0..100 {
            let _ = parse(query).unwrap();
            proto_iterations += 1;
        }
    }
    let proto_elapsed = proto_start.elapsed();
    let proto_ns_per_iter = proto_elapsed.as_nanos() as f64 / proto_iterations as f64;

    // Calculate speedup and time saved
    let speedup = proto_ns_per_iter / raw_ns_per_iter;
    let time_saved_ns = proto_ns_per_iter - raw_ns_per_iter;
    let time_saved_us = time_saved_ns / 1000.0;

    // Calculate speedup and time saved
    let speedup_iter = raw_ns_per_iter / raw_iter_ns_per_iter;
    let time_saved_ns_iter = raw_ns_per_iter - raw_iter_ns_per_iter;
    let time_saved_us_iter = time_saved_ns_iter / 1000.0;

    // Calculate throughput (queries per second)
    let raw_iter_qps = 1_000_000_000.0 / raw_iter_ns_per_iter;
    let raw_qps = 1_000_000_000.0 / raw_ns_per_iter;
    let proto_qps = 1_000_000_000.0 / proto_ns_per_iter;

    println!("\n");
    println!("============================================================");
    println!("            parse_raw vs parse Benchmark                    ");
    println!("============================================================");
    println!("Query: {} chars (CTEs + JOINs + subqueries + window functions)", query.len());
    println!();
    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│                    RESULTS                              │");
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  parse_raw (direct C struct reading):                   │");
    println!("│    Iterations:    {:>10}                            │", raw_iterations);
    println!("│    Total time:    {:>10.2?}                            │", raw_elapsed);
    println!("│    Per iteration: {:>10.2} μs                         │", raw_ns_per_iter / 1000.0);
    println!("│    Throughput:    {:>10.0} queries/sec                │", raw_qps);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  parse_raw_iter (direct C struct reading):              │");
    println!("│    Iterations:    {:>10}                            │", raw_iter_iterations);
    println!("│    Total time:    {:>10.2?}                            │", raw_iter_elapsed);
    println!("│    Per iteration: {:>10.2} μs                         │", raw_iter_ns_per_iter / 1000.0);
    println!("│    Throughput:    {:>10.0} queries/sec                │", raw_iter_qps);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  parse (protobuf serialization):                        │");
    println!("│    Iterations:    {:>10}                            │", proto_iterations);
    println!("│    Total time:    {:>10.2?}                            │", proto_elapsed);
    println!("│    Per iteration: {:>10.2} μs                         │", proto_ns_per_iter / 1000.0);
    println!("│    Throughput:    {:>10.0} queries/sec                │", proto_qps);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  COMPARISON                                             │");
    println!("│    Speedup:       {:>10.2}x faster                    │", speedup);
    println!("│    Speedup iter:  {:>10.2}x faster                    │", speedup_iter);
    println!("│    Time saved:    {:>10.2} μs per parse               │", time_saved_us);
    println!("│    Extra queries: {:>10.0} more queries/sec           │", raw_qps - proto_qps);
    println!("└─────────────────────────────────────────────────────────┘");
    println!();
}

/// Benchmark comparing deparse_raw vs deparse performance
#[test]
fn benchmark_deparse_raw_vs_deparse() {
    // Complex query with multiple features: CTEs, JOINs, subqueries, window functions, etc.
    let query = r#"
        WITH RECURSIVE
            category_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM categories
                WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM categories c
                INNER JOIN category_tree ct ON c.parent_id = ct.id
                WHERE ct.depth < 10
            ),
            recent_orders AS (
                SELECT
                    o.id,
                    o.user_id,
                    o.total_amount,
                    o.created_at,
                    ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.created_at DESC) as rn
                FROM orders o
                WHERE o.created_at > NOW() - INTERVAL '30 days'
                    AND o.status IN ('completed', 'shipped', 'delivered')
            )
        SELECT
            u.id AS user_id,
            u.email,
            u.first_name || ' ' || u.last_name AS full_name,
            COALESCE(ua.city, 'Unknown') AS city,
            COUNT(DISTINCT ro.id) AS order_count,
            SUM(ro.total_amount) AS total_spent,
            AVG(ro.total_amount) AS avg_order_value,
            MAX(ro.created_at) AS last_order_date,
            now(),
            current_timestamp,
            CASE
                WHEN SUM(ro.total_amount) > 10000 THEN 'platinum'
                WHEN SUM(ro.total_amount) > 5000 THEN 'gold'
                WHEN SUM(ro.total_amount) > 1000 THEN 'silver'
                ELSE 'bronze'
            END AS customer_tier,
            (
                SELECT COUNT(*)
                FROM user_reviews ur
                WHERE ur.user_id = u.id AND ur.rating >= 4
            ) AS positive_reviews,
            ARRAY_AGG(DISTINCT ct.name ORDER BY ct.name) FILTER (WHERE ct.depth = 1) AS top_categories
        FROM users u
        LEFT JOIN user_addresses ua ON ua.user_id = u.id AND ua.is_primary = true
        LEFT JOIN recent_orders ro ON ro.user_id = u.id AND ro.rn <= 5
        LEFT JOIN order_items oi ON oi.order_id = ro.id
        LEFT JOIN products p ON p.id = oi.product_id
        LEFT JOIN category_tree ct ON ct.id = p.category_id
        WHERE u.is_active = true
            AND u.created_at < NOW() - INTERVAL '7 days'
            AND EXISTS (
                SELECT 1 FROM user_logins ul
                WHERE ul.user_id = u.id
                AND ul.logged_in_at > NOW() - INTERVAL '90 days'
            )
        GROUP BY u.id, u.email, u.first_name, u.last_name, ua.city
        HAVING COUNT(DISTINCT ro.id) > 0
        ORDER BY total_spent DESC NULLS LAST, u.created_at ASC
        LIMIT 100
        OFFSET 0
        FOR UPDATE OF u SKIP LOCKED"#;

    // Parse the query once to get the protobuf result
    let parsed = parse(query).unwrap();

    // Warm up
    for _ in 0..10 {
        let _ = deparse_raw(&parsed.protobuf).unwrap();
        let _ = deparse(&parsed.protobuf).unwrap();
    }

    // Run for a fixed duration to get stable measurements
    let target_duration = Duration::from_secs(2);

    // Benchmark deparse_raw
    let mut raw_iterations = 0u64;
    let raw_start = Instant::now();
    while raw_start.elapsed() < target_duration {
        for _ in 0..100 {
            let _ = deparse_raw(&parsed.protobuf).unwrap();
            raw_iterations += 1;
        }
    }
    let raw_elapsed = raw_start.elapsed();
    let raw_ns_per_iter = raw_elapsed.as_nanos() as f64 / raw_iterations as f64;

    // Benchmark deparse (protobuf)
    let mut proto_iterations = 0u64;
    let proto_start = Instant::now();
    while proto_start.elapsed() < target_duration {
        for _ in 0..100 {
            let _ = deparse(&parsed.protobuf).unwrap();
            proto_iterations += 1;
        }
    }
    let proto_elapsed = proto_start.elapsed();
    let proto_ns_per_iter = proto_elapsed.as_nanos() as f64 / proto_iterations as f64;

    // Calculate speedup and time saved
    let speedup = proto_ns_per_iter / raw_ns_per_iter;
    let time_saved_ns = proto_ns_per_iter - raw_ns_per_iter;
    let time_saved_us = time_saved_ns / 1000.0;

    // Calculate throughput (queries per second)
    let raw_qps = 1_000_000_000.0 / raw_ns_per_iter;
    let proto_qps = 1_000_000_000.0 / proto_ns_per_iter;

    println!("\n");
    println!("============================================================");
    println!("           deparse_raw vs deparse Benchmark                 ");
    println!("============================================================");
    println!("Query: {} chars (CTEs + JOINs + subqueries + window functions)", query.len());
    println!();
    println!("┌─────────────────────────────────────────────────────────┐");
    println!("│                    RESULTS                              │");
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  deparse_raw (direct C struct building):                │");
    println!("│    Iterations:    {:>10}                            │", raw_iterations);
    println!("│    Total time:    {:>10.2?}                            │", raw_elapsed);
    println!("│    Per iteration: {:>10.2} μs                         │", raw_ns_per_iter / 1000.0);
    println!("│    Throughput:    {:>10.0} queries/sec                 │", raw_qps);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  deparse (protobuf serialization):                      │");
    println!("│    Iterations:    {:>10}                            │", proto_iterations);
    println!("│    Total time:    {:>10.2?}                            │", proto_elapsed);
    println!("│    Per iteration: {:>10.2} μs                         │", proto_ns_per_iter / 1000.0);
    println!("│    Throughput:    {:>10.0} queries/sec                 │", proto_qps);
    println!("├─────────────────────────────────────────────────────────┤");
    println!("│  COMPARISON                                             │");
    println!("│    Speedup:       {:>10.2}x faster                     │", speedup);
    println!("│    Time saved:    {:>10.2} μs per deparse              │", time_saved_us);
    println!("│    Extra queries: {:>10.0} more queries/sec           │", raw_qps - proto_qps);
    println!("└─────────────────────────────────────────────────────────┘");
    println!();
}
