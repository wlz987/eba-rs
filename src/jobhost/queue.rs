use crate::envelope::Envelope;
use std::collections::VecDeque;

pub const DEFAULT_QUEUE_LIMIT: usize = 512;

#[derive(Debug)]
pub(crate) struct EnvelopeQueue {
    limit: usize,
    items: VecDeque<Envelope>,
}

impl EnvelopeQueue {
    pub fn new(limit: usize) -> EnvelopeQueue {
        assert!(limit >= 1, "jobhost: queue_limit must be >= 1");
        EnvelopeQueue {
            limit,
            items: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn at_limit(&self) -> bool {
        self.items.len() >= self.limit
    }

    pub fn offer(&mut self, e: Envelope) -> crate::Result<()> {
        if self.at_limit() {
            return Err(crate::Error::QueueFull);
        }
        self.items.push_back(e);
        Ok(())
    }

    pub fn popleft(&mut self) -> Envelope {
        self.items.pop_front().expect("queue nonempty")
    }
}

#[cfg(test)]
mod tests {
    use super::EnvelopeQueue;
    use crate::envelope::{make_envelope, ActorId, EnvelopeId, MakeOptions, Value};

    fn env_n(topic: &str, n: i64) -> crate::envelope::Envelope {
        make_envelope(
            topic,
            Value::Int(n),
            ActorId("a".into()),
            MakeOptions {
                id: Some(EnvelopeId(format!("{n:032x}"))),
                ..Default::default()
            },
        )
        .expect("env")
    }

    #[test]
    #[should_panic(expected = "queue_limit must be >= 1")]
    fn queue_limit_rejects_zero() {
        EnvelopeQueue::new(0);
    }

    #[test]
    fn offer_hits_limit() {
        let mut q = EnvelopeQueue::new(1);
        assert!(q.offer(env_n("job", 1)).is_ok());
        assert!(q.offer(env_n("job", 2)).is_err());
    }

    #[test]
    fn drain_order() {
        let mut q = EnvelopeQueue::new(2);
        let (a, b) = (env_n("job", 1), env_n("job", 2));
        q.offer(a.clone()).unwrap();
        q.offer(b.clone()).unwrap();
        assert!(q.at_limit());
        assert!(!q.is_empty());
        assert_eq!(q.popleft(), a);
        assert_eq!(q.popleft(), b);
        assert!(q.is_empty());
    }
}
