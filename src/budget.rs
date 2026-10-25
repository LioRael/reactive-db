use crate::Error;

/// Per-operation limits for staged mutations, emitted transitions and logical work.
///
/// Applies independently to each write or materialization. These are not byte or
/// time limits and cannot preempt application closures. Zero disallows that work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionBudget {
    /// Successful staged write operations, including repeated writes to one key.
    pub max_input_changes: usize,
    /// Source and derived transitions in total, including initial constructed rows.
    pub max_output_changes: usize,
    /// Logical row visits, candidates and graph visits, excluding tree comparisons.
    pub max_work: usize,
}
impl Default for ExecutionBudget {
    fn default() -> Self {
        Self {
            max_input_changes: 100_000,
            max_output_changes: 1_000_000,
            max_work: 10_000_000,
        }
    }
}
/// Work consumed by a successful write, suitable for application diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionStats {
    /// Number of staged write operations.
    pub input_changes: usize,
    /// Number of emitted transitions across all nodes.
    pub output_changes: usize,
    /// Number of logical work units.
    pub work: usize,
}
pub(crate) struct Meter {
    limits: ExecutionBudget,
    pub stats: ExecutionStats,
}
impl Meter {
    pub fn new(limits: ExecutionBudget) -> Self {
        Self {
            limits,
            stats: ExecutionStats::default(),
        }
    }
    fn charge(value: &mut usize, limit: usize, kind: &'static str) -> Result<(), Error> {
        if *value >= limit {
            return Err(Error::BudgetExceeded(kind));
        }
        *value += 1;
        Ok(())
    }
    pub fn input(&mut self) -> Result<(), Error> {
        Self::charge(
            &mut self.stats.input_changes,
            self.limits.max_input_changes,
            "input changes",
        )
    }
    pub fn output(&mut self) -> Result<(), Error> {
        Self::charge(
            &mut self.stats.output_changes,
            self.limits.max_output_changes,
            "output changes",
        )
    }
    pub fn work(&mut self) -> Result<(), Error> {
        Self::charge(&mut self.stats.work, self.limits.max_work, "work")
    }
}
