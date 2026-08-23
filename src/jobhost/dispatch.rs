use crate::envelope::Envelope;
use crate::job::JobCtx;
use crate::{jobhost, Error};

pub(crate) fn dispatch(host: &mut jobhost::JobHost, env: &Envelope) -> crate::Result<()> {
    let gen = host.require_loan()?.gen.clone();
    if crate::result::looks_like_result_envelope(env, &gen) {
        return route_result(host, env);
    }
    if host.shutting_down {
        return Ok(());
    }
    if !matches_business(&host.accept, env) {
        return Ok(());
    }
    host.queue.offer(env.clone())
}

pub(crate) fn route_result(host: &mut jobhost::JobHost, env: &Envelope) -> crate::Result<()> {
    let outcome = host.registry.resolve_only(env)?;
    if !outcome.fresh {
        if outcome.state.is_some() {
            if let Some(request_id) = outcome.request_id {
                let loan_bus = host.require_loan()?.bus.clone();
                host.registry
                    .finish_safe(&loan_bus, &host.inbox, &request_id);
            }
        }
        return Ok(());
    }
    let Some(key) = host.slots.parent_key(&env.header.cause).cloned() else {
        if let Some(request_id) = outcome.request_id {
            let loan_bus = host.require_loan()?.bus.clone();
            host.registry
                .finish_safe(&loan_bus, &host.inbox, &request_id);
        }
        return Ok(());
    };
    let Some(mut parent) = host.slots.take(&key) else {
        if let Some(request_id) = outcome.request_id {
            let loan_bus = host.require_loan()?.bus.clone();
            host.registry
                .finish_safe(&loan_bus, &host.inbox, &request_id);
        }
        return Ok(());
    };
    {
        let mut ctx = JobCtx {
            job: &mut parent,
            host,
        };
        ctx.deliver_child_result(env)?;
    }
    host.slots.place(parent);
    Ok(())
}

pub(crate) fn matches_business(accept: &[crate::pattern::Pattern], env: &Envelope) -> bool {
    let Ok(parts) = crate::envelope::split_topic(&env.header.topic) else {
        return false;
    };
    accept.iter().any(|p| crate::pattern::matches(p, &parts))
}

pub(crate) fn watchdog(host: &mut jobhost::JobHost) -> crate::Result<()> {
    let now = host.require_loan()?.clock.now_ms();
    for key in host.slots.active_keys() {
        let Some(mut job) = host.slots.take(&key) else {
            continue;
        };
        {
            let mut ctx = JobCtx {
                job: &mut job,
                host,
            };
            ctx.expire_due(now)?;
        }
        host.slots.place(job);
    }
    Ok(())
}

pub(crate) fn flush_queue(host: &mut jobhost::JobHost) -> crate::Result<()> {
    while !host.queue.is_empty() {
        let e = host.queue.popleft();
        if host.shutting_down {
            continue;
        }
        adopt_and_begin(host, e)?;
    }
    Ok(())
}

pub(crate) fn adopt_and_begin(host: &mut jobhost::JobHost, root: Envelope) -> crate::Result<()> {
    let key = root.header.id.clone();
    let job = (host.make_job)(&root);
    host.slots.adopt(job)?;
    let mut job = host
        .slots
        .take(&key)
        .ok_or_else(|| Error::State("adopted job missing".into()))?;
    {
        let mut ctx = JobCtx {
            job: &mut job,
            host,
        };
        ctx.begin()?;
    }
    host.slots.place(job);
    Ok(())
}
