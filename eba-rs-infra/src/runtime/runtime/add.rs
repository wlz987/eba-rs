use crate::prelude::*;
use alloc::rc::Rc;
use alloc::sync::Arc;
use core::cell::RefCell;

use crate::envelope::header::Address;
use crate::envelope::result::err_payload;
use crate::envelope::{make_envelope, Envelope, MakeEnvelope};
use crate::errors::{combine_failures, delivery_envelope, BoxError, BusError};
use crate::job::core::Core;
use crate::job::host::JobHost;
use crate::job::pump::attach;
use crate::notify::signal::AgendaHost;
use crate::time::clock::Clock;

use super::{run_turn, sync_state, Runtime, RuntimeState};

impl Runtime {
    pub fn add(&mut self, host: Rc<dyn JobHost>) -> Result<(), BoxError> {
        self.enter()?;
        run_turn(self, |runtime| runtime.add_inner(host))
    }

    fn add_inner(&mut self, host: Rc<dyn JobHost>) -> Result<(), BoxError> {
        if self.state != RuntimeState::Running {
            return Err(Box::from(BusError("runtime cannot accept hosts".into())));
        }
        let core = host.core();
        if core.borrow().signal.handed() {
            return Err(Box::from(BusError("host already admitted".into())));
        }
        let actor = core.borrow().sub.actor_id.0.clone();
        if self.hosts.contains_key(&actor) {
            return Err(Box::from(BusError("duplicate host actor".into())));
        }
        attach(&mut core.borrow_mut(), self.bus_ref())?;
        self.hosts.insert(actor.clone(), host);
        let agenda: Rc<dyn AgendaHost> = self.agenda.clone();
        core.borrow().signal.bind(&agenda, &actor);
        sync_state(&self.agenda, &core.borrow());
        if self.state != RuntimeState::Running {
            let refused = Box::from(BusError("runtime cannot accept hosts".into())) as BoxError;
            self.hosts.remove(&actor);
            self.agenda.remove(&actor);
            core.borrow().signal.detach();
            if let Err(stuck) = core.borrow().sub.stop() {
                return Err(combine_failures(refused, stuck));
            }
            return Err(refused);
        }
        Ok(())
    }

    pub(super) fn sync_core(&self, core: &Rc<RefCell<Core>>) {
        sync_state(&self.agenda, &core.borrow())
    }

    pub(super) fn answer_spent(&self, error: &BoxError, clock: &dyn Clock) -> Result<(), BoxError> {
        let Some(failed) = delivery_envelope(error) else {
            return Ok(());
        };
        let failed: Rc<Envelope> = Rc::new((*failed).clone());
        let Address::Result { correlation_id } = &failed.header.address else {
            return Ok(());
        };
        let request_id = correlation_id.clone();
        if !self.hosts.values().any(|host| host.delivery_spent(&failed)) {
            return Ok(());
        }
        let id = self.id_gen.borrow_mut().next_envelope_id();
        let failure = Rc::new(make_envelope(MakeEnvelope {
            topic: failed.header.topic.clone(),
            payload: Arc::new(err_payload("delivery_failed", None)?),
            from: failed.header.from.clone(),
            cause: Some(failed.header.cause.clone()),
            id: Some(id),
            kind: "result".into(),
            reply_to: None,
            correlation_id: Some(request_id.clone()),
        })?);
        let mut held = false;
        for host in self.hosts.values() {
            let core = host.core();
            let agenda = Rc::clone(&self.agenda);
            let after = || sync_state(&agenda, &core.borrow());
            if host.resume_held(
                &request_id,
                &failure,
                &self.bus,
                clock,
                &self.id_gen,
                &after,
            )? {
                held = true;
            }
        }
        if !held {
            let home: Rc<dyn AgendaHost> = self.agenda.clone();
            self.bus.deliver(&failure, &home);
        }
        Ok(())
    }
}
