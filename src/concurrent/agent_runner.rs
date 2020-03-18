/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::concurrent::logbuffer::term_reader::ErrorHandler;
use crate::concurrent::strategies::Strategy;
use crate::utils::errors::AeronError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

// The trait to be implemented by agents run within AgentRunner
pub trait Agent {
    fn on_start(&mut self) -> Result<(), AeronError>;
    fn do_work(&mut self) -> Result<i32, AeronError>;
    fn on_close(&mut self) -> Result<(), AeronError>;
}

pub struct AgentRunner<
    A: 'static + std::marker::Send + std::marker::Sync + Agent,
    I: 'static + std::marker::Send + std::marker::Sync + Strategy,
> {
    agent: Arc<Mutex<A>>, // need mutable Agent here as AgentRunner will change Agent state while running it
    idle_strategy: Arc<I>,
    exception_handler: ErrorHandler,
    is_running: AtomicBool,
    is_closed: AtomicBool,
    thread: Option<thread::JoinHandle<()>>,
    name: String,
}

impl<
        A: 'static + std::marker::Send + std::marker::Sync + Agent,
        I: 'static + std::marker::Send + std::marker::Sync + Strategy,
    > AgentRunner<A, I>
{
    pub fn new(agent: Arc<Mutex<A>>, idle_strategy: Arc<I>, exception_handler: ErrorHandler, name: &str) -> Self {
        Self {
            agent,
            idle_strategy,
            exception_handler,
            is_running: AtomicBool::from(false),
            is_closed: AtomicBool::from(false),
            thread: None,
            name: String::from(name),
        }
    }

    /**
     * Name given to the thread running the agent.
     *
     * @return the name given to the thread running the agent.
     */
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn set_name(&mut self, new_name: &str) {
        self.name = String::from(new_name);
    }

    /**
     * Is the Agent running?
     *
     * @return is the Agent been started successfully and not closed?
     */
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /**
     * Has the Agent been closed?
     *
     * @return has the Agent been closed?
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }

    /**
     * Start the Agent running
     *
     * Will spawn a std::thread.
     */
    pub fn start(this: Arc<Mutex<Self>>) -> Result<(), AeronError> {
        let self_for_thread = this.clone();
        let mut selfy = this.lock().expect("mutex is poisoned");
        let th = thread::Builder::new().name(selfy.name.clone()).spawn(move || {
            self_for_thread.lock().expect("mutex is poisoned").run();
        });

        if let Ok(handle) = th {
            selfy.thread = Some(handle);
        } else {
            return Err(AeronError::GenericError(format!("Agent start failed: {:?}", th.err())));
        }

        Ok(())
    }

    /**
     * Run the Agent duty cycle until closed
     */
    pub fn run(&mut self) {
        // Set is_running to true if its currently false.
        let _res = self
            .is_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire);

        if let Err(error) = self.agent.lock().expect("Mutex poisoned").on_start() {
            (self.exception_handler)(error);
            self.is_running.store(false, Ordering::SeqCst);
        }

        while self.is_running.load(Ordering::SeqCst) {
            match self.agent.lock().expect("Mutex poisoned").do_work() {
                Ok(work_cnt) => self.idle_strategy.idle_opt(work_cnt),
                Err(error) => (self.exception_handler)(error),
            }
        }

        if let Err(error) = self.agent.lock().expect("Mutex poisoned").on_close() {
            (self.exception_handler)(error);
        }
    }

    /**
     * Close the agent and stop the associated thread from running. This method waits for the thread to join.
     * Consumes self as join() consumes thread handle.
     */
    pub fn close(self) {
        // Atomically compare and set is_running value
        // If is_running currently true then it will be set to false and Ok(true) is returned
        // If is_running currently false then no change will be done and Err(false) is returned
        if let Ok(_prev_value) = self
            .is_running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Acquire)
        {
            if let Some(handle) = self.thread {
                let _ret = handle.join();
            }
        }
    }
}
