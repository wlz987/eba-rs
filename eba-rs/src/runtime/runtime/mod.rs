use crate::collections::IndexedMap;
use std::rc::Rc;

use crate::errors::{BoxError, BusError};
use crate::job::core::{Core, HostState};
use crate::job::host::{core_of, JobHost};
use crate::job::job::SharedIdGen;
use crate::job::pump::{has_work, scheduled};
use crate::mail::bus::Bus;
use crate::mail::inbox::close_count;
use crate::notify::agenda::Agenda;
use crate::time::clock::Clock;
use crate::time::numbers::nonnegative_int;
use crate::time::schedule::Schedule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    Running,
    Draining,
    Closing,
    Closed,
}

mod add;
mod advance;
mod close;

pub struct Runtime {
    hosts: IndexedMap<String, Rc<dyn JobHost>>,
    agenda: Rc<Agenda>,
    busy: bool,
    reading_clock: bool,
    observed: Option<u64>,
    state: RuntimeState,
    noticed_closes: usize,
    bus: Bus,
    clock: Rc<dyn Clock>,
    id_gen: SharedIdGen,
}

impl Runtime {
    pub fn new(bus: Bus, clock: Rc<dyn Clock>, id_gen: SharedIdGen) -> Self {
        Runtime {
            hosts: IndexedMap::new(),
            agenda: Rc::new(Agenda::new()),
            busy: false,
            reading_clock: false,
            observed: None,
            state: RuntimeState::Running,
            noticed_closes: close_count(),
            bus,
            clock,
            id_gen,
        }
    }

    pub fn bus_ref(&self) -> &Bus {
        &self.bus
    }

    pub fn clock_ref(&self) -> &dyn Clock {
        &*self.clock
    }

    pub fn id_gen_ref(&self) -> &SharedIdGen {
        &self.id_gen
    }

    pub fn owns(&self, host: &dyn JobHost) -> bool {
        let core = host.core();
        let actor = core.borrow().sub.actor_id.0.clone();
        match self.hosts.get(&actor) {
            Some(registered) => Rc::ptr_eq(&registered.core(), &core),
            None => false,
        }
    }

    pub fn hosts(&self) -> Vec<Rc<dyn JobHost>> {
        self.hosts.values().cloned().collect()
    }

    pub fn wake_bound(&self) -> bool {
        self.agenda.wake_bound()
    }

    pub fn bind_wake(&self, wake: Option<crate::notify::agenda::WakeFn>) {
        self.agenda.bind_wake(wake);
    }

    pub fn schedule(&mut self) -> Schedule {
        let now = self.schedule_now();
        let snapshot = self.agenda.schedule(now);
        Schedule::new(
            !self.busy && snapshot.ready,
            snapshot.next_deadline_ms,
            !self.busy && snapshot.is_quiescent,
        )
        .observed(now)
    }

    fn schedule_now(&mut self) -> u64 {
        match self.observed {
            Some(now) => now,
            None => self.now(),
        }
    }

    fn now(&mut self) -> u64 {
        if self.reading_clock {
            panic!("reentrant clock observation");
        }
        self.reading_clock = true;
        let now = nonnegative_int(self.clock.now_ms(), "clock_ms");
        self.reading_clock = false;
        now
    }

    fn enter(&mut self) -> Result<(), BoxError> {
        if self.busy || self.reading_clock || self.state == RuntimeState::Closed {
            return Err(Box::from(BusError("closed or reentrant runtime".into())));
        }
        self.busy = true;
        Ok(())
    }

    fn select(&self, actor_id: Option<&str>) -> Result<Vec<Rc<dyn JobHost>>, BoxError> {
        match actor_id {
            None => Ok(self.hosts.values().cloned().collect()),
            Some(id) => match self.hosts.get(&id.to_string()) {
                Some(host) => Ok(vec![Rc::clone(host)]),
                None => Err(Box::from(BusError(format!("unknown host: {}", id)))),
            },
        }
    }
}

fn sync_state(agenda: &Agenda, core: &Core) {
    let closing = core.state == HostState::Closing;
    let retired = core.state == HostState::Closed;
    agenda.update(
        &core.sub.actor_id.0,
        if closing || retired {
            None
        } else {
            core.book.next_deadline_ms()
        },
        closing || (!retired && has_work(core)),
        !closing && !retired && scheduled(core),
    );
}
