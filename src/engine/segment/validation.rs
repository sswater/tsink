use std::path::{Path, PathBuf};

use tracing::warn;

use crate::engine::fs_utils::{path_exists_no_follow, rename_and_sync_parents, stage_dir_path};
use crate::{Result, TsinkError};

pub(crate) const STARTUP_SEGMENT_QUARANTINE_PURPOSE: &str = "startup-segment-quarantine";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentLoadPolicy {
    SkipInvalid,
    FailOnInvalid(SegmentValidationContext),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SegmentValidationContext {
    Startup,
    RuntimeRefresh,
    Compaction,
    Maintenance,
}

#[derive(Debug, Clone)]
pub(crate) struct QuarantinedSegmentRoot {
    pub(crate) path: PathBuf,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) sync_failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StartupQuarantinedSegment {
    pub(crate) original_root: PathBuf,
    pub(crate) quarantined: QuarantinedSegmentRoot,
    pub(crate) details: String,
}

pub(crate) fn segment_validation_error_message(err: &TsinkError) -> Option<String> {
    match err {
        TsinkError::DataCorruption(msg) | TsinkError::Compression(msg) => Some(msg.clone()),
        _ => None,
    }
}

pub(crate) fn segment_validation_error(
    segment_root: &Path,
    context: SegmentValidationContext,
    details: &str,
) -> TsinkError {
    let context_label = match context {
        SegmentValidationContext::Startup => "startup",
        SegmentValidationContext::RuntimeRefresh => "runtime refresh",
        SegmentValidationContext::Compaction => "compaction",
        SegmentValidationContext::Maintenance => "maintenance",
    };
    TsinkError::DataCorruption(format!(
        "persisted segment {} failed {context_label} validation: {}",
        segment_root.display(),
        details
    ))
}

pub(crate) fn runtime_refresh_disappearance_error(
    segment_root: &Path,
    err: &TsinkError,
) -> TsinkError {
    TsinkError::Other(format!(
        "persisted segment {} disappeared during runtime refresh (transient disappearance race): {}",
        segment_root.display(),
        err
    ))
}

pub(crate) fn quarantine_segment_root(
    segment_root: &Path,
    purpose: &str,
) -> Result<QuarantinedSegmentRoot> {
    let quarantine_root = stage_dir_path(segment_root, purpose)?;
    match rename_and_sync_parents(segment_root, &quarantine_root) {
        Ok(()) => Ok(QuarantinedSegmentRoot {
            path: quarantine_root,
            sync_failed: false,
        }),
        Err(err) => {
            let quarantine_exists = path_exists_no_follow(&quarantine_root)?;
            let source_exists = path_exists_no_follow(segment_root)?;
            if quarantine_exists && !source_exists {
                Ok(QuarantinedSegmentRoot {
                    path: quarantine_root,
                    sync_failed: true,
                })
            } else {
                Err(err)
            }
        }
    }
}

pub(crate) fn quarantine_invalid_startup_segment(
    segment_root: &Path,
    details: &str,
) -> Result<StartupQuarantinedSegment> {
    let quarantined = quarantine_segment_root(segment_root, STARTUP_SEGMENT_QUARANTINE_PURPOSE)
        .map_err(|quarantine_err| {
            TsinkError::Other(format!(
                "failed to quarantine corrupt persisted segment {} during startup: {}; original validation error: {}",
                segment_root.display(),
                quarantine_err,
                details
            ))
        })?;
    warn!(
        path = %segment_root.display(),
        quarantine_path = %quarantined.path.display(),
        sync_failed = quarantined.sync_failed,
        error = %details,
        "Startup quarantined invalid persisted segment"
    );
    Ok(StartupQuarantinedSegment {
        original_root: segment_root.to_path_buf(),
        quarantined,
        details: details.to_string(),
    })
}

pub(crate) fn handle_segment_dir_load_error(
    dir: &Path,
    err: TsinkError,
    policy: SegmentLoadPolicy,
    disappeared_message: &'static str,
    invalid_message: &'static str,
) -> Result<()> {
    match err {
        err if is_not_found_error(&err) => {
            warn!(
                path = %dir.display(),
                error = %err,
                "{}",
                disappeared_message
            );
            Ok(())
        }
        err => {
            if let Some(msg) = segment_validation_error_message(&err) {
                match policy {
                    SegmentLoadPolicy::SkipInvalid => {
                        warn!(path = %dir.display(), error = %msg, "{}", invalid_message);
                        Ok(())
                    }
                    SegmentLoadPolicy::FailOnInvalid(context) => {
                        Err(segment_validation_error(dir, context, &msg))
                    }
                }
            } else {
                Err(err)
            }
        }
    }
}

pub(crate) fn is_not_found_error(err: &TsinkError) -> bool {
    matches!(err, TsinkError::Io(io_err) if io_err.kind() == std::io::ErrorKind::NotFound)
}

/// A source that vanished (root directory removed, or one of its required
/// files already unlinked) between an inventory snapshot and the read that
/// follows is a segment mid-removal: compaction or retention retired it
/// concurrently. Callers that skip vanished sources surface neither error
/// shape, so both are recognized here.
pub(crate) fn is_vanished_source_error(err: &TsinkError) -> bool {
    is_not_found_error(err)
        || matches!(err, TsinkError::DataCorruption(message) if message.starts_with("missing "))
}

#[cfg(test)]
mod vanished_predicate_tests {
    use super::*;

    #[test]
    fn vanished_source_error_matches_raw_missing_and_not_found() {
        assert!(is_vanished_source_error(&TsinkError::DataCorruption(
            "missing manifest.bin".to_string()
        )));
        assert!(is_vanished_source_error(&TsinkError::DataCorruption(
            "missing chunks.bin".to_string()
        )));
        assert!(is_vanished_source_error(&TsinkError::Io(
            std::io::Error::from(std::io::ErrorKind::NotFound)
        )));
        assert!(!is_vanished_source_error(&TsinkError::DataCorruption(
            "bad magic".to_string()
        )));
        assert!(!is_vanished_source_error(&TsinkError::Io(
            std::io::Error::from(std::io::ErrorKind::PermissionDenied)
        )));
    }
}
