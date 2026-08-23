use eba::{is_name, matches, parse_pattern, split_topic, InvalidTopic};

fn parts(topic: &str) -> Vec<&str> {
    split_topic(topic).unwrap()
}

fn p(text: &str) -> eba::Pattern {
    parse_pattern(text).unwrap()
}

#[test]
fn exact_star_glob() {
    assert!(matches(&p("echo"), &parts("echo")));
    assert!(matches(&p("echo.*"), &parts("echo.e01")));
    assert!(!matches(&p("echo.*"), &parts("echo.a.b")));
    assert!(matches(&p("echo.**"), &parts("echo.a.b")));
    assert!(!matches(&p("echo.**"), &parts("result.echo.e01")));
}

#[test]
fn wildcard_must_be_terminal() {
    let err = parse_pattern("echo.*.x").unwrap_err();
    assert_eq!(
        err,
        InvalidTopic("wildcard not terminal: \"echo.*.x\"".into())
    );
}

#[test]
fn empty_illegal() {
    assert!(parse_pattern("").is_err());
}

#[test]
fn empty_segment_and_illegal_name() {
    assert!(parse_pattern("echo..x").is_err());
    assert!(parse_pattern("Echo").is_err());
    assert!(parse_pattern("echo.**.x").is_err());
}

#[test]
fn bare_wildcards() {
    assert!(matches(&p("*"), &parts("echo")));
    assert!(!matches(&p("*"), &parts("echo.x")));
    assert!(matches(&p("**"), &parts("echo")));
    assert!(matches(&p("**"), &parts("a.b.c")));
}

#[test]
fn exact_miss() {
    assert!(!matches(&p("echo"), &parts("echo.x")));
    assert!(!matches(&p("echo.*"), &parts("echo")));
}

#[test]
fn segment_names() {
    assert!(is_name("e01"));
    assert!(is_name("read_x"));
    assert!(!is_name("Read"));
    assert!(!is_name("_x"));
    assert!(!is_name(""));
}
