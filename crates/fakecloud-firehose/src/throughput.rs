use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Default)]
pub(crate) struct ThroughputLimits {
    pub requests: Option<f64>,
    pub records: Option<f64>,
    pub mib: Option<f64>,
}

/// Stream-local, one-second ingestion accounting; callers supply monotonic time.
///
/// The first successful admission, and a successful admission at or after the
/// boundary, starts a fresh window at `now`.
/// Windows do not accrue idle credits, but full capacity immediately before and
/// after a boundary can produce a short burst of twice the per-window allowance.
pub(crate) struct ThroughputWindow {
    started_at: Instant,
    requests: u64,
    records: usize,
    bytes: usize,
}

impl ThroughputWindow {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            requests: 0,
            records: 0,
            bytes: 0,
        }
    }

    pub(crate) fn try_admit(
        &mut self,
        now: Instant,
        limits: ThroughputLimits,
        records: usize,
        bytes: usize,
    ) -> bool {
        if limits.requests.is_none() && limits.records.is_none() && limits.mib.is_none() {
            return true;
        }

        let rollover = now.duration_since(self.started_at) >= Duration::from_secs(1);
        let (used_requests, used_records, used_bytes) = if rollover {
            (0_u64, 0_usize, 0_usize)
        } else {
            (self.requests, self.records, self.bytes)
        };
        // Track every dimension when any limit is enabled, including dimensions
        // that might become enabled by a later same-window quota change.
        let Some(requests) = used_requests.checked_add(1) else {
            return false;
        };
        let Some(records) = used_records.checked_add(records) else {
            return false;
        };
        let Some(bytes) = used_bytes.checked_add(bytes) else {
            return false;
        };

        // Limits are finite and non-negative (validated by quota setters). Scale
        // usage, not limits: huge finite MiB limits must not overflow a capacity.
        if limits
            .requests
            .is_some_and(|limit| count_as_f64_ceil(requests as u128) > limit)
            || limits
                .records
                .is_some_and(|limit| count_as_f64_ceil(records as u128) > limit)
            || limits
                .mib
                .is_some_and(|limit| count_as_f64_ceil(bytes as u128) / 1_048_576.0 > limit)
        {
            return false;
        }

        // Commit only after every check passes, including rollover. Rejections
        // must not reset either the old counters or the old window's anchor.
        if rollover || self.requests == 0 {
            self.started_at = now;
        }
        self.requests = requests;
        self.records = records;
        self.bytes = bytes;
        true
    }
}

/// Smallest representable f64 not below the count. An ordinary cast can round
/// down above 2^53 and incorrectly admit one unit over a representable limit.
/// Dividing this by the power-of-two bytes-per-MiB scale preserves that bound.
fn count_as_f64_ceil(count: u128) -> f64 {
    let rounded = count as f64;
    if (rounded as u128) < count {
        f64::from_bits(rounded.to_bits() + 1)
    } else {
        rounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn usage(window: &ThroughputWindow) -> (Instant, u64, usize, usize) {
        (
            window.started_at,
            window.requests,
            window.records,
            window.bytes,
        )
    }

    #[test]
    fn request_capacity_recovers_at_one_second() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(1.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 1, 3));
        assert!(!window.try_admit(now, limits, 1, 3));
        assert!(!window.try_admit(now + Duration::from_millis(999), limits, 1, 3));
        assert!(window.try_admit(now + Duration::from_secs(1), limits, 1, 3));
    }

    #[test]
    fn first_success_anchors_window_after_initial_rejection() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(1.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(!window.try_admit(now, limits, 2, 1));
        assert!(window.try_admit(now + Duration::from_millis(500), limits, 1, 1));
        assert!(!window.try_admit(now + Duration::from_secs(1), limits, 1, 1));
        assert!(window.try_admit(now + Duration::from_millis(1_500), limits, 1, 1));
    }

    #[test]
    fn records_accept_exact_limit_and_reject_one_over() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(3.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 2, 10));
        assert!(window.try_admit(now, limits, 1, 10));
        assert!(!window.try_admit(now, limits, 1, 0));
        assert!(window.try_admit(now + Duration::from_secs(1), limits, 3, 10));
    }

    #[test]
    fn fractional_mib_accepts_exact_bytes_and_rejects_one_over() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            mib: Some(0.5),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 1, 262_144));
        assert!(window.try_admit(now, limits, 1, 262_144));
        assert!(!window.try_admit(now, limits, 1, 1));
        assert!(window.try_admit(now + Duration::from_secs(1), limits, 1, 524_288));
    }

    #[test]
    fn rejected_bytes_leave_capacity_for_smaller_request() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(2.0),
            records: Some(3.0),
            mib: Some(0.5),
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 1, 262_144));
        assert!(!window.try_admit(now, limits, 2, 262_145));
        assert!(window.try_admit(now, limits, 2, 262_144));
        assert!(!window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn each_enabled_dimension_can_reject_without_consuming_other_capacity() {
        let now = Instant::now();
        let generous = ThroughputLimits {
            requests: Some(3.0),
            records: Some(3.0),
            mib: Some(1.0),
        };
        for strict in [
            ThroughputLimits {
                requests: Some(0.0),
                ..generous
            },
            ThroughputLimits {
                records: Some(0.0),
                ..generous
            },
            ThroughputLimits {
                mib: Some(0.0),
                ..generous
            },
        ] {
            let mut window = ThroughputWindow::new(now);
            assert!(!window.try_admit(now, strict, 1, 1));
            assert!(window.try_admit(now, generous, 3, 1_048_576));
        }
    }

    #[test]
    fn empty_payload_still_consumes_a_request() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(1.0),
            records: Some(0.0),
            mib: Some(0.0),
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 0, 0));
        assert!(!window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn zero_record_and_byte_limits_allow_only_zero_usage() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(0.0),
            mib: Some(0.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(!window.try_admit(now, limits, 1, 0));
        assert!(!window.try_admit(now, limits, 0, 1));
        assert!(window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn zero_request_limit_rejects_even_empty_payloads() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(0.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(!window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn disabled_limits_bypass_without_changing_usage_or_window() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(1.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 1, 1));
        let before = usage(&window);
        assert!(window.try_admit(now, ThroughputLimits::default(), usize::MAX, usize::MAX));
        assert!(window.try_admit(
            now + Duration::from_secs(2),
            ThroughputLimits::default(),
            usize::MAX,
            usize::MAX
        ));
        assert_eq!(usage(&window), before);
        assert!(!window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn ignored_dimensions_are_counted_when_another_dimension_is_enabled() {
        let now = Instant::now();
        for initially_enabled in [
            ThroughputLimits {
                requests: Some(10.0),
                ..Default::default()
            },
            ThroughputLimits {
                records: Some(10.0),
                ..Default::default()
            },
            ThroughputLimits {
                mib: Some(10.0),
                ..Default::default()
            },
        ] {
            let mut window = ThroughputWindow::new(now);
            assert!(window.try_admit(now, initially_enabled, 3, 524_288));
            assert!(!window.try_admit(
                now,
                ThroughputLimits {
                    requests: Some(1.0),
                    ..Default::default()
                },
                0,
                0
            ));
            assert!(!window.try_admit(
                now,
                ThroughputLimits {
                    records: Some(3.0),
                    ..Default::default()
                },
                1,
                0
            ));
            assert!(!window.try_admit(
                now,
                ThroughputLimits {
                    mib: Some(0.5),
                    ..Default::default()
                },
                0,
                1
            ));
        }
    }

    #[test]
    fn runtime_limit_increases_and_decreases_preserve_same_window_usage() {
        let now = Instant::now();
        let mut window = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            requests: Some(1.0),
            records: Some(2.0),
            mib: Some(0.5),
        };
        assert!(window.try_admit(now, limits, 2, 524_288));
        assert!(!window.try_admit(now, limits, 0, 0));
        let raised = ThroughputLimits {
            requests: Some(3.0),
            records: Some(4.0),
            mib: Some(1.0),
        };
        assert!(window.try_admit(now, raised, 1, 262_144));
        assert!(!window.try_admit(now, limits, 0, 0));
        assert!(window.try_admit(now, raised, 1, 262_144));
        assert!(!window.try_admit(now, raised, 0, 0));
    }

    #[test]
    fn rejected_rollover_preserves_usage_and_success_anchors_at_now() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            records: Some(1.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, 1, 1));
        let before = usage(&window);
        assert!(!window.try_admit(now + Duration::from_secs(1), limits, 2, 1));
        assert_eq!(usage(&window), before);
        assert!(window.try_admit(now + Duration::from_millis(1_100), limits, 1, 1));
        assert!(!window.try_admit(now + Duration::from_secs(2), limits, 1, 1));
        assert!(window.try_admit(now + Duration::from_millis(2_100), limits, 1, 1));
    }

    #[test]
    fn idle_time_does_not_accumulate_credits() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(1.0),
            ..Default::default()
        };
        let mut window = ThroughputWindow::new(now);
        let later = now + Duration::from_secs(100);
        assert!(window.try_admit(later, limits, 1, 1));
        assert!(!window.try_admit(later, limits, 1, 1));
        assert!(!window.try_admit(later + Duration::from_millis(999), limits, 1, 1));
        assert!(window.try_admit(later + Duration::from_secs(1), limits, 1, 1));
    }

    #[test]
    fn windows_have_independent_usage() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(1.0),
            ..Default::default()
        };
        let mut first = ThroughputWindow::new(now);
        let mut second = ThroughputWindow::new(now);
        assert!(first.try_admit(now, limits, 1, 1));
        assert!(!first.try_admit(now, limits, 1, 1));
        assert!(second.try_admit(now, limits, 1, 1));
    }

    #[test]
    fn fractional_count_limits_do_not_round_up() {
        let now = Instant::now();
        for limits in [
            ThroughputLimits {
                requests: Some(0.5),
                ..Default::default()
            },
            ThroughputLimits {
                records: Some(0.5),
                ..Default::default()
            },
        ] {
            let mut window = ThroughputWindow::new(now);
            assert!(!window.try_admit(now, limits, 1, 0));
        }
    }

    #[test]
    fn sub_byte_mib_limit_does_not_allow_a_whole_byte() {
        let now = Instant::now();
        let mut window = ThroughputWindow::new(now);
        let limits = ThroughputLimits {
            mib: Some(f64::MIN_POSITIVE),
            ..Default::default()
        };
        assert!(!window.try_admit(now, limits, 0, 1));
        assert!(window.try_admit(now, limits, 0, 0));
    }

    #[test]
    fn huge_finite_limits_do_not_overflow_capacity_calculations() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(f64::MAX),
            records: Some(f64::MAX),
            mib: Some(f64::MAX),
        };
        let mut window = ThroughputWindow::new(now);
        assert!(window.try_admit(now, limits, usize::MAX, usize::MAX));
        assert!(!window.try_admit(now, limits, 1, 0));
    }

    #[test]
    fn count_and_byte_overflow_reject_atomically_even_when_dimension_is_ignored() {
        let now = Instant::now();
        let limits = ThroughputLimits {
            requests: Some(10.0),
            ..Default::default()
        };
        for (records, bytes, extra_records, extra_bytes) in
            [(usize::MAX, 0, 1, 0), (0, usize::MAX, 0, 1)]
        {
            let mut window = ThroughputWindow::new(now);
            assert!(window.try_admit(now, limits, records, bytes));
            let before = usage(&window);
            assert!(!window.try_admit(now, limits, extra_records, extra_bytes));
            assert_eq!(usage(&window), before);
            assert!(window.try_admit(now, limits, 0, 0));
            assert!(window.try_admit(
                now + Duration::from_secs(1),
                limits,
                extra_records,
                extra_bytes
            ));
        }
    }

    #[test]
    fn request_counter_overflow_rejects_without_mutation() {
        let now = Instant::now();
        let mut window = ThroughputWindow::new(now);
        window.requests = u64::MAX;
        let limits = ThroughputLimits {
            records: Some(1.0),
            ..Default::default()
        };
        let before = usage(&window);
        assert!(!window.try_admit(now, limits, 1, 1));
        assert_eq!(usage(&window), before);
        assert!(window.try_admit(now + Duration::from_secs(1), limits, 1, 1));
    }

    #[test]
    fn large_request_count_is_not_rounded_down_to_the_limit() {
        let now = Instant::now();
        let mut window = ThroughputWindow::new(now);
        window.requests = 9_007_199_254_740_992;
        let limits = ThroughputLimits {
            requests: Some(9_007_199_254_740_992.0),
            ..Default::default()
        };
        assert!(!window.try_admit(now, limits, 0, 0));
        assert!(window.try_admit(
            now,
            ThroughputLimits {
                requests: Some(9_007_199_254_740_994.0),
                ..Default::default()
            },
            0,
            0
        ));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn large_record_and_byte_counts_are_not_rounded_down_to_limits() {
        let now = Instant::now();
        for limits in [
            ThroughputLimits {
                records: Some(9_007_199_254_740_992.0),
                ..Default::default()
            },
            ThroughputLimits {
                mib: Some(8_589_934_592.0),
                ..Default::default()
            },
        ] {
            let mut window = ThroughputWindow::new(now);
            assert!(window.try_admit(now, limits, 9_007_199_254_740_992, 9_007_199_254_740_992));
            assert!(!window.try_admit(now, limits, 1, 1));
        }
    }
}
