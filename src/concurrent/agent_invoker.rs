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
use std::sync::{Arc, Mutex};

use crate::concurrent::{agent_runner::Agent, logbuffer::term_reader::ErrorHandler};

pub struct AgentInvoker<T: Agent> {
    agent: Arc<Mutex<T>>,
    exception_handler: Box<dyn ErrorHandler>,
    is_started: bool,
    is_running: bool,
    is_closed: bool,
}

impl<T: Agent> AgentInvoker<T> {
    pub fn new(agent: Arc<Mutex<T>>, exception_handler: Box<dyn ErrorHandler>) -> Self {
        Self {
            agent,
            exception_handler,
            is_started: false,
            is_running: false,
            is_closed: false,
        }
    }

    /**
     * Has the Agent been started?
     *
     * @return has the Agent been started?
     */
    pub fn is_started(&self) -> bool {
        self.is_started
    }

    /**
     * Is the Agent running?
     *
     * @return is the Agent been started successfully and not closed?
     */
    pub fn is_running(&self) -> bool {
        self.is_running
    }

    /**
     * Has the Agent been closed?
     *
     * @return has the Agent been closed?
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /**
     * Mark the invoker as started and call the Agent::onStart() method.
     * <p>
     * Startup logic will only be performed once.
     */
    pub fn start(&mut self) {
        if !self.is_started {
            self.is_started = true;
            let on_start_result = self.agent.lock().expect("Mutex poisoned").on_start();
            if let Err(error) = on_start_result {
                self.exception_handler.call(error);
                self.close();
            } else {
                self.is_running = true;
            }
        }
    }

    /**
     * Invoke the Agent::doWork() method and return the work count.
     *
     * If not successfully started or after closed then this method will return without invoking the {@link Agent}.
     *
     * @return the work count for the Agent::doWork() method.
     */
    pub fn invoke(&self) -> i32 {
        let mut work_count = 0;

        if self.is_running {
            match self.agent.lock().expect("Mutex poisoned").do_work() {
                Err(error) => self.exception_handler.call(error),
                Ok(wrk_cnt) => work_count = wrk_cnt,
            }
        }

        work_count
    }

    /**
     * Mark the invoker as closed and call the Agent::onClose() logic for clean up.
     *
     * The clean up logic will only be performed once.
     */
    pub fn close(&mut self) {
        if !self.is_closed {
            self.is_running = false;
            self.is_closed = true;
            if let Err(error) = self.agent.lock().expect("Mutex poisoned").on_close() {
                self.exception_handler.call(error);
            }
        }
    }
}
