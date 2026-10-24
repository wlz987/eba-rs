use crate::prelude::*;

use crate::errors::{BoxError, WakeError};
use crate::job::core::HostState;
use crate::job::held::resolve_held;
use crate::job::pump::{advance_host, arm_closed_waits};
use crate::mail::inbox::close_count;
use crate::time::clock::pin_clock;

use super::{core_of, run_turn, Runtime};

impl Runtime {
    pub fn advance(&mut self) -> Result<bool, BoxError> {
        self.enter()?;
        run_turn(self, |runtime| runtime.advance_inner())
    }

    fn drain_cancelled(&self) -> Result<Vec<crate::envelope::EnvelopeId>, BoxError> {
        let mut cancelled = Vec::new();
        for host in self.hosts.values() {
            cancelled.extend(host.core().borrow_mut().take_cancelled());
        }
        if !cancelled.is_empty() {
            for host in self.hosts.values() {
                let core = host.core();
                let mut core = core.borrow_mut();
                core.cancel_queued(&cancelled);
                core.cancel_held(&cancelled);
                core.cancel_jobs(&cancelled)?;
            }
        }
        Ok(cancelled)
    }

    fn advance_inner(&mut self) -> Result<bool, BoxError> {
        let now = self.schedule_now();
        self.observed = Some(now);
        let noticed = close_count();
        if noticed != self.noticed_closes {
            self.noticed_closes = noticed;
            for host in self.hosts.values() {
                let core = host.core();
                let watched = core.borrow().sub.inbox.is_closed();
                if watched {
                    arm_closed_waits(&mut core.borrow_mut());
                    self.sync_core(&core);
                }
            }
        }
        let pinned = pin_clock(now);
        self.agenda.due(now);
        if !self.drain_cancelled()?.is_empty() {
            for host in self.hosts.values() {
                let core = host.core();
                self.sync_core(&core);
            }
        }
        let count = self.agenda.ready_size();
        let mut progressed = false;
        for _ in 0..count {
            let Some(actor) = self.agenda.pop() else {
                break;
            };
            let Some(host) = self.hosts.get(&actor).cloned() else {
                continue;
            };
            let core = core_of(&*host);
            let running = {
                let held = core.borrow();
                held.state == HostState::Running || held.state == HostState::Draining
            };
            let outcome = if running {
                host.on_step(&pinned);
                let mut held = core.borrow_mut();
                advance_host(&mut held, self.bus_ref(), &pinned, &self.id_gen)
            } else {
                Ok(false)
            };
            match outcome {
                Ok(was) => {
                    self.sync_core(&core);
                    progressed = progressed || was;
                }
                Err(error) => {
                    let answer = self.answer_spent(&error, &pinned);
                    self.sync_core(&core);
                    answer?;
                    return Err(error);
                }
            }
        }
        self.drain_cancelled()?;
        for host in self.hosts.values().cloned().collect::<Vec<_>>() {
            let core = core_of(&*host);
            let running = {
                let held = core.borrow();
                held.state == HostState::Running || held.state == HostState::Draining
            };
            if !running {
                continue;
            }
            let outcome = resolve_held(&mut core.borrow_mut(), self.bus_ref(), &self.id_gen);
            match outcome {
                Ok(was) => {
                    self.sync_core(&core);
                    progressed = progressed || was;
                }
                Err(error) => {
                    let answer = self.answer_spent(&error, &pinned);
                    self.sync_core(&core);
                    answer?;
                    return Err(error);
                }
            }
        }
        if let Some(fault) = self.agenda.take_fault() {
            return Err(Box::from(WakeError(fault.to_string())));
        }
        Ok(progressed)
    }
}
