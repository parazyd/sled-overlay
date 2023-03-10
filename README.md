sled-overlay
============

This Rust library serves as a minimal overlay mechanism for the
[sled](https://docs.rs/sled) embedded database.

This mechanism enables us to simulate changes in a sled database/tree
so that keys and values can be dynamically mutated, while avoiding
having to change the underlying database. With this, we can perform
changes to the sled trees and access the latest changes in-memory,
and then only when we're satisfied with the results, we can actually
atomically write it into the actual database.

This functionality can also serve as a rollback-like mechanism
for sled.

Usage examples are offered in the repository as test units, and docs
can be found on [docs.rs/sled-overlay](https://docs.rs/sled-overlay).

## License

GNU AGPLv3.
