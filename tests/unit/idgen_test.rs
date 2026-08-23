use eba::{topic_segment, ActorId, EnvelopeId};
use std::cell::RefCell;
use std::rc::Rc;

#[test]
fn uuid_ids_differ() {
    let gen: eba::IdGenHandle = Rc::new(RefCell::new(eba::UuidIdGen::new()));
    let a = gen.borrow_mut().next_envelope_id();
    let b = gen.borrow_mut().next_envelope_id();
    assert_ne!(a, b);
    let mapped = gen.borrow().topic_segment(&a).unwrap();
    assert_eq!(mapped, topic_segment(&a).unwrap());
}

#[test]
fn bad_id_segment() {
    assert!(topic_segment(&EnvelopeId("not-hex".into())).is_err());
}

#[test]
fn seq_increments() {
    let mut gen = eba::SeqIdGen::new(3);
    assert_ne!(
        eba::IdGen::next_envelope_id(&mut gen),
        eba::IdGen::next_envelope_id(&mut gen)
    );
}

#[test]
fn seq_start_below_one_clamps() {
    let mut gen = eba::SeqIdGen::new(0);
    assert_eq!(
        eba::IdGen::next_envelope_id(&mut gen),
        EnvelopeId(format!("{:032x}", 1))
    );
}

#[test]
fn actor_id_is_plain_label() {
    assert_eq!(ActorId("h".into()).0, "h");
}
