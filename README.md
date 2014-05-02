Consistent provides eventually consistent atomic transactions, by delaying
updates until no threads is mutating a shared variable.

This comes at a cost of having a separate TVar for every thread, but has the
advantage that no thread will ever lock or retry except for the manager actor
responsible for performing the updates.
