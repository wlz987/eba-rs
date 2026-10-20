use crate::errors::{BoxError, WakeError};
use crate::job::core::HostState;
use crate::job::pump::{advance_host, arm_closed_waits};
use crate::mail::inbox::close_count;
use crate::time::clock::pin_clock;

use super::{core_of, Runtime};

impl Runtime {
    pub fn advance(&mut self) -> Result<bool, BoxError> {
        self.enter()?;
        let outcome = self.advance_inner();
        self.observed = None;
        self.busy = false;
        outcome
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
        if let Some(fault) = self.agenda.take_fault() {
            return Err(Box::from(WakeError(fault.to_string())));
        }
        Ok(progressed)
    }
}
