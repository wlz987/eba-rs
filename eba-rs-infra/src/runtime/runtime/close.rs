use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError, BusError};
use crate::job::close::close_host;
use crate::job::core::HostState;
use crate::job::host::JobHost;

use super::{core_of, Runtime, RuntimeState};

impl Runtime {
    pub fn shutdown(&mut self, actor_id: Option<&str>) -> Result<(), BoxError> {
        if self.state == RuntimeState::Closed || self.state == RuntimeState::Closing {
            return Ok(());
        }
        if self.reading_clock {
            return Err(Box::from(BusError("reentrant clock observation".into())));
        }
        let selected = self.select(actor_id)?;
        if self.busy {
            self.drain(&selected, actor_id);
            return Ok(());
        }
        self.busy = true;
        self.drain(&selected, actor_id);
        self.busy = false;
        Ok(())
    }

    fn drain(&mut self, selected: &[Rc<dyn JobHost>], actor_id: Option<&str>) {
        if actor_id.is_none() {
            self.state = RuntimeState::Draining;
        }
        for host in selected {
            let core = host.core();
            let mut held = core.borrow_mut();
            if held.state == HostState::Running {
                held.state = HostState::Draining;
            }
        }
    }

    pub fn close(&mut self, actor_id: Option<&str>) -> Result<Vec<Rc<Envelope>>, BoxError> {
        if self.state == RuntimeState::Closed {
            return Ok(Vec::new());
        }
        self.enter()?;
        let outcome = self.close_inner(actor_id);
        self.busy = false;
        self.agenda.wake_fn();
        outcome
    }

    fn close_inner(&mut self, actor_id: Option<&str>) -> Result<Vec<Rc<Envelope>>, BoxError> {
        let selected = self.select(actor_id)?;
        if actor_id.is_none() {
            self.state = RuntimeState::Closing;
            for host in &selected {
                core_of(&**host).borrow_mut().begin_close();
            }
        }
        let mut failed: Option<BoxError> = None;
        for host in &selected {
            let core = core_of(&**host);
            let outcome = {
                let mut held = core.borrow_mut();
                close_host(&mut held, self.bus_ref())
            };
            self.sync_core(&core);
            if let Err(error) = outcome {
                failed = Some(match failed {
                    None => error,
                    Some(held) => combine_failures(held, error),
                });
                let retired = core.borrow().state == HostState::Closed;
                if !retired {
                    return Err(failed.expect("just set"));
                }
            }
        }
        if let Some(error) = failed {
            return Err(error);
        }
        let mut abandoned_all: Vec<Rc<Envelope>> = Vec::new();
        for host in selected {
            let core = core_of(&*host);
            let actor = {
                let mut held = core.borrow_mut();
                abandoned_all.append(&mut held.abandoned.take().unwrap_or_default());
                held.signal.detach();
                held.sub.actor_id.0.clone()
            };
            self.agenda.remove(&actor);
            self.hosts.remove(&actor);
        }
        if actor_id.is_none() {
            self.state = RuntimeState::Closed;
        }
        Ok(abandoned_all)
    }
}
