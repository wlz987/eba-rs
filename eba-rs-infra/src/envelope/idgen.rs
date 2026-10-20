use super::ids::EnvelopeId;

pub trait IdGen {
    fn next_envelope_id(&mut self) -> EnvelopeId;
}

pub struct SeqIdGen {
    n: u64,
}

impl SeqIdGen {
    pub fn new(start: u64) -> Self {
        SeqIdGen { n: start }
    }
}

impl Default for SeqIdGen {
    fn default() -> Self {
        SeqIdGen::new(1)
    }
}

impl IdGen for SeqIdGen {
    fn next_envelope_id(&mut self) -> EnvelopeId {
        let current = self.n;
        self.n += 1;
        EnvelopeId(format!("{:032x}", current))
    }
}
#[cfg(feature = "uuid")]
pub use uuid_gen::UuidIdGen;

#[cfg(feature = "uuid")]
mod uuid_gen {
    use super::{EnvelopeId, IdGen};
    use uuid::Uuid;

    pub struct UuidIdGen;

    impl Default for UuidIdGen {
        fn default() -> Self {
            UuidIdGen
        }
    }

    impl IdGen for UuidIdGen {
        fn next_envelope_id(&mut self) -> EnvelopeId {
            EnvelopeId(format!("{}", Uuid::new_v4().simple()))
        }
    }
}
