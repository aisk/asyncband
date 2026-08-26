// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Coordination primitives for graceful task shutdown.
//!
//! This module provides [`new_pair`] to create a coordinator and an initial participant:
//!
//! * [`Shutdown`] can request shutdown and wait for all participants to finish.
//! * [`ShutdownGuard`] registers a participant until the guard is dropped and can observe the
//!   shutdown request.
//! * [`ShutdownWatcher`] can observe the shutdown request without delaying completion.
//!
//! Internally, the shutdown signal is implemented using a countdown latch, and the task completion
//! is tracked using a wait group. [`Shutdown`] is cloneable, allowing multiple control handles to
//! request shutdown or wait for completion; [`ShutdownGuard`] is also cloneable, allowing multiple
//! tasks to participate in the same shutdown process.
//!
//! [`Shutdown::wait_for_completion`] waits until all [`ShutdownGuard`] handles have been dropped.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! let (shutdown, guard) = asyncband::shutdown::new_pair();
//!
//! for i in 0..3 {
//!     let guard = guard.clone();
//!     tokio::spawn(async move {
//!         println!("Task {} starting", i);
//!         guard.shutdown_requested().await;
//!         println!("Task {} done", i);
//!     });
//! }
//! drop(guard);
//!
//! shutdown.request_shutdown();
//! shutdown.wait_for_completion().await;
//! # }
//! ```

use std::future::Future;
use std::future::IntoFuture;
use std::sync::Arc;

use crate::latch::Latch;
use crate::waitgroup::Wait;
use crate::waitgroup::WaitGroup;

/// Creates a graceful shutdown coordinator and an initial participant guard.
///
/// See the [module level documentation](self) for more.
pub fn new_pair() -> (Shutdown, ShutdownGuard) {
    let latch = Arc::new(Latch::new(1));
    let wg = WaitGroup::new();
    let shutdown = Shutdown {
        latch: latch.clone(),
        wait: wg.clone().into_future(),
    };
    let guard = ShutdownGuard {
        latch,
        _wait_group: wg,
    };
    (shutdown, guard)
}

/// Coordinates a graceful shutdown request and participant completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct Shutdown {
    latch: Arc<Latch>,
    wait: Wait,
}

impl Shutdown {
    /// Requests shutdown for all [`ShutdownGuard`] and [`ShutdownWatcher`] handles.
    ///
    /// The request is sticky and this method is idempotent. Current and future observers from this
    /// pair will see the request.
    pub fn request_shutdown(&self) {
        self.latch.count_down();
    }

    /// Waits for all [`ShutdownGuard`] handles to be dropped.
    ///
    /// This only waits for participant completion; it does not request shutdown. Other clones of
    /// this control handle can wait for the same completion independently.
    pub async fn wait_for_completion(self) {
        self.wait.await;
    }
}

/// Registers a graceful shutdown participant until the handle is dropped.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownGuard {
    latch: Arc<Latch>,
    // Keeps this guard registered as a shutdown participant until it is dropped.
    _wait_group: WaitGroup,
}

impl ShutdownGuard {
    /// Returns a handle that observes the shutdown request without participating in completion.
    ///
    /// The returned handle does not block [`Shutdown::wait_for_completion`], but this guard remains
    /// registered. Use [`into_watcher`](Self::into_watcher) to stop participating in completion.
    pub fn watcher(&self) -> ShutdownWatcher {
        ShutdownWatcher {
            latch: self.latch.clone(),
        }
    }

    /// Converts this participant into a watcher that does not delay completion.
    pub fn into_watcher(self) -> ShutdownWatcher {
        let Self {
            latch,
            _wait_group: _,
        } = self;
        ShutdownWatcher { latch }
    }

    /// Returns whether shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Waits until shutdown is requested.
    pub async fn shutdown_requested(&self) {
        self.latch.wait().await;
    }

    /// Returns an owned future that resolves when shutdown is requested.
    ///
    /// The returned future has no lifetime constraints and does not keep this participant
    /// registered for completion.
    pub fn shutdown_requested_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}

/// Observes graceful shutdown requests without participating in completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownWatcher {
    latch: Arc<Latch>,
}

impl ShutdownWatcher {
    /// Returns whether shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.latch.try_wait().is_ok()
    }

    /// Waits until shutdown is requested.
    pub async fn shutdown_requested(&self) {
        self.latch.wait().await;
    }

    /// Returns an owned future that resolves when shutdown is requested.
    ///
    /// The returned future has no lifetime constraints.
    pub fn shutdown_requested_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}
