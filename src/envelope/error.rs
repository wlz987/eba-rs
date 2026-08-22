
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeBuildError(pub String);

impl EnvelopeBuildError {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        EnvelopeBuildError(msg.into())
    }
}

impl From<EnvelopeBuildError> for crate::Error {
    fn from(e: EnvelopeBuildError) -> Self {
        crate::Error::EnvelopeBuild(e.0)
    }
}
