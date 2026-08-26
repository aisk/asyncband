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
//! This module provides [`new_pair`] to create a coordinator and an initial completion guard:
//!
//! * [`Shutdown`] can request shutdown and wait for all guards to be dropped.
//! * [`ShutdownGuard`] keeps shutdown completion pending until it is dropped and can observe the
//!   shutdown request.
//! * [`ShutdownWatch`] can observe the shutdown request without delaying completion.
//!
//! Internally, the shutdown signal is implemented using a countdown latch, and the task completion
//! is tracked using a wait group. [`Shutdown`] is cloneable, allowing multiple control handles to
//! request shutdown or wait for completion. [`ShutdownGuard`] is also cloneable; each clone keeps
//! completion pending independently until it is dropped.
//!
//! [`Shutdown::wait`] waits until all [`ShutdownGuard`] handles have been dropped, while
//! [`Shutdown::shutdown`] requests shutdown before waiting.
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
//! shutdown.shutdown().await;
//! # }
//! ```

use std::future::Future;
use std::future::IntoFuture;
use std::sync::Arc;

use crate::latch::Latch;
use crate::waitgroup::Wait;
use crate::waitgroup::WaitGroup;

/// Creates a graceful shutdown coordinator and an initial completion guard.
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
        wait_group: wg,
    };
    (shutdown, guard)
}

/// Coordinates a graceful shutdown request and completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct Shutdown {
    latch: Arc<Latch>,
    wait: Wait,
}

impl Shutdown {
    /// Requests shutdown for all [`ShutdownGuard`] and [`ShutdownWatch`] handles.
    ///
    /// The request is sticky and this method is idempotent. Current and future observers from this
    /// pair will see the request.
    pub fn request_shutdown(&self) {
        self.latch.count_down();
    }

    /// Requests shutdown and waits for all [`ShutdownGuard`] handles to be dropped.
    ///
    /// This is equivalent to calling [`request_shutdown`](Self::request_shutdown) followed by
    /// [`wait`](Self::wait).
    pub async fn shutdown(self) {
        self.request_shutdown();
        self.wait().await;
    }

    /// Waits for all [`ShutdownGuard`] handles to be dropped.
    ///
    /// This does not request shutdown. Other clones of this control handle can wait for the same
    /// completion independently.
    pub async fn wait(self) {
        self.wait.await;
    }
}

/// Keeps shutdown completion pending until the guard is dropped.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownGuard {
    latch: Arc<Latch>,
    #[expect(
        dead_code,
        reason = "keeps shutdown completion pending until this guard is dropped"
    )]
    wait_group: WaitGroup,
}

impl ShutdownGuard {
    /// Returns a handle that observes the shutdown request without participating in completion.
    ///
    /// The returned handle does not delay [`Shutdown::wait`], but this guard still does. Use
    /// [`into_watch`](Self::into_watch) to stop keeping shutdown completion pending.
    pub fn watch(&self) -> ShutdownWatch {
        ShutdownWatch {
            latch: self.latch.clone(),
        }
    }

    /// Converts this guard into a watch that does not delay completion.
    pub fn into_watch(self) -> ShutdownWatch {
        let Self { latch, .. } = self;
        ShutdownWatch { latch }
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
    /// The returned future has no lifetime constraints and does not keep shutdown completion
    /// pending.
    pub fn shutdown_requested_owned(&self) -> impl Future<Output = ()> + 'static {
        self.latch.clone().wait_owned()
    }
}

/// Observes graceful shutdown requests without participating in completion.
///
/// See the [module level documentation](self) for more.
#[derive(Debug, Clone)]
pub struct ShutdownWatch {
    latch: Arc<Latch>,
}

impl ShutdownWatch {
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
