use std::sync::Arc;

use rr_core::types::GroupId;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::group_manager::RaftGroupManager;

/// Handle for the background output runner task.
pub struct OutputHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    join: JoinHandle<()>,
}

impl OutputHandle {
    /// Stop the background task and wait for termination.
    pub async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join.await;
    }
}

/// Spawn a no-op output runner placeholder.
///
/// Output delivery is not wired in this crate revision yet; this task
/// remains idle until stopped so group lifecycle can still own a handle.
pub fn spawn(
    _manager: Arc<RaftGroupManager>,
    _group_id: GroupId,
    _rr_client: Option<Arc<rr_client::RrClient>>,
) -> OutputHandle {
    let (stop_tx, stop_rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        let _ = stop_rx.await;
    });
    OutputHandle {
        stop_tx: Some(stop_tx),
        join,
    }
}
