# Source provenance

M0–M3 is a new implementation of the accepted native Rust design. No source
files, vendor packages, bridge modules, or JS compatibility code were copied
from either original project. The new crate has no third-party dependencies.

The earlier audit of tanstack-db-rs informed delta propagation, typed consumer
scenarios and the decision to replace the original whole-row Ord bags, callback
scheduler and subscription delivery. Reference source fingerprints at the start
of implementation:

| Reference file | SHA256 |
|---|---|
| `src/lib.rs` | `c43917cd022940628e255b71cd27bb88afee71cf60175e947ca7044470144622` |
| `src/collection.rs` | `dee93d07cf09877fc627a3190a05351598afe48605ad78b650cf83d85df87bba` |
| `src/database.rs` | `b3721ea0f2eee5133c6fb6273c7468751b0172d1c20ca1b08ac58b0e52e23e21` |
| `src/scheduler.rs` | `58242460019f82641423afbfaac9cf53fb3675fdee2ba07c73f159e9d4f259de` |
| `src/window.rs` | `ae98f04718fa0aebf53088e13bacc541e7d66cfb90a25b65a9d7b8d30700137f` |
| `tests/consumer.rs` | `252180fe34500e43a8d42ef16dc0d528359cfa9aeb8df5b648746122f07968c7` |
| `THIRD_PARTY.md` | `477dcb74d7e23b4b8bdb281fd9ed96e9d52e21fcfde39a4544e814ddc6a892fc` |

The reference project attributes its incremental algorithm design to TanStack DB
commit `37b8fc2c5abd08e9128ec3606c6534aaec3ae67e` (MIT). Those records remain
in the untouched original project. Future code extraction must preserve the
licenses/copyright notices for the actual copied portions; this file does not
relicense or erase the provenance of future extracted implementations.

The livequery project remains separate. Its inspection API and JSON transport are
not dependencies or source inputs of this crate.
