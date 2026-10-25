use std::fmt;

/// Recoverable failures at a database operation or configuration boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// A handle belongs to another database.
    WrongDatabase,
    /// The handle's database has been dropped.
    DatabaseDropped,
    /// An application panic invalidated further access to this database.
    Poisoned,
    /// A collection with this diagnostic name already exists.
    DuplicateCollection,
    /// Insert encountered an existing key in the staged or committed state.
    DuplicateKey,
    /// Replace or remove encountered an absent key.
    MissingKey,
    /// A batch method previously failed, even if its error was ignored.
    InvalidBatch,
    /// Application validation rejected the entire batch.
    Validation(String),
    /// A monotonically increasing revision exhausted its representation.
    ArithmeticOverflow,
    /// Journal capacities must both be nonzero.
    InvalidJournalLimits,
    /// The shared view already has different journal capacities.
    ViewOptionsConflict,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongDatabase => f.write_str("handle belongs to another database"),
            Self::DatabaseDropped => f.write_str("database has been dropped"),
            Self::Poisoned => f.write_str("database poisoned by an application panic"),
            Self::DuplicateCollection => f.write_str("duplicate collection name"),
            Self::DuplicateKey => f.write_str("key already exists"),
            Self::MissingKey => f.write_str("key does not exist"),
            Self::InvalidBatch => f.write_str("a batch operation previously failed"),
            Self::Validation(message) => write!(f, "validation failed: {message}"),
            Self::ArithmeticOverflow => f.write_str("revision overflow"),
            Self::InvalidJournalLimits => f.write_str("journal capacities must be nonzero"),
            Self::ViewOptionsConflict => {
                f.write_str("shared view has different journal capacities")
            }
        }
    }
}
impl std::error::Error for Error {}
