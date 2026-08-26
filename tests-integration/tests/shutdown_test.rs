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

use std::future::pending;
use std::pin::pin;

use asyncband::shutdown::*;
use tests_integration::poll_once;
use tests_integration::test_runtime;

#[test]
fn test_single_pair() {
    let (shutdown, token) = new_pair();
    let handle = test_runtime().spawn(async move { token.shutdown_requested().await });
    shutdown.request_shutdown();
    pollster::block_on(shutdown.wait_for_completion());
    pollster::block_on(handle).unwrap();
}

#[test]
fn test_multiple_tasks() {
    let (shutdown, token) = new_pair();
    for _i in 0..100 {
        let token = token.clone();
        test_runtime().spawn(async move { token.shutdown_requested().await });
    }
    drop(token);
    shutdown.request_shutdown();
    pollster::block_on(shutdown.wait_for_completion());
}

#[test]
fn test_multiple_control_handles() {
    let (shutdown, token) = new_pair();
    for _i in 0..100 {
        let token = token.clone();
        test_runtime().spawn(async move { token.shutdown_requested().await });
    }
    drop(token);
    let shutdown_clone = shutdown.clone();
    shutdown.request_shutdown();
    pollster::block_on(shutdown.wait_for_completion());
    pollster::block_on(shutdown_clone.wait_for_completion());
}

#[test]
fn test_is_shutdown_requested() {
    let (shutdown, token) = new_pair();
    assert!(!token.is_shutdown_requested());
    shutdown.request_shutdown();
    assert!(token.is_shutdown_requested());
}

#[test]
fn test_shutdown_requested_owned_does_not_capture_self() {
    struct State {
        token: ShutdownToken,
    }

    async fn run_state(_state: &mut State) {
        pending::<()>().await;
    }

    let (shutdown, token) = new_pair();
    let mut state = State { token };
    test_runtime().spawn(async move {
        let shutdown_requested = state.token.shutdown_requested_owned();
        tokio::select! {
            _ = shutdown_requested => (),
            _ = run_state(&mut state) => (),
        }
    });
    shutdown.request_shutdown();
    pollster::block_on(shutdown.wait_for_completion());
}

#[test]
fn test_watch_does_not_block_completion() {
    let (shutdown, token) = new_pair();
    let watch = token.into_watch();

    shutdown.request_shutdown();
    assert!(watch.is_shutdown_requested());
    pollster::block_on(shutdown.wait_for_completion());
}

#[test]
fn test_wait_for_completion_does_not_request_shutdown() {
    let (shutdown, token) = new_pair();
    let watch = token.watch();
    let completion = shutdown.wait_for_completion();
    let mut completion = pin!(completion);

    assert!(poll_once(completion.as_mut()).is_pending());
    assert!(!watch.is_shutdown_requested());

    drop(token);
    assert!(poll_once(completion.as_mut()).is_ready());
    assert!(!watch.is_shutdown_requested());
}

#[test]
fn test_watch_observes_shutdown_request() {
    let (shutdown, token) = new_pair();
    let watch = token.watch();
    let handle = test_runtime().spawn(async move { watch.shutdown_requested().await });
    drop(token);

    shutdown.request_shutdown();
    pollster::block_on(shutdown.wait_for_completion());
    pollster::block_on(handle).unwrap();
}
