# Declarative Dataflow

A reactive query engine built on [differential
dataflow](https://github.com/frankmcsherry/differential-dataflow).

Declarative provides:

**Interactive use:** Declarative accepts queries expressed in a
Datalog-inspired binding language and turns them into differential
dataflows dynamically and at runtime. This works equally well as a
library embedded into a specific application or as a standalone
service (e.g. via the included WebSocket server).

**Reactive relational queries:** Declarative provides a relational
query language, with full support for traditional binary joins,
worst-case optimal n-way joins, antijoins, various aggregates,
predicates, unions, and projections. Queries are made up of rules,
which can depend on each other (or recursively on themselves). Query
results are updated incrementally as inputs change.

**[WIP] Reactive GraphQL queries:** Declarative also comes with
built-in support for GraphQL-like queries, for a more
document-oriented usage model.

**Pluggable sinks and sources:** Declarative can be extended to read
data from and write results back to external systems, such as Kafka or
Datomic, as well as static sources such as csv files.

**Pluggable frontends:** Languages such as Datalog and SQL can be
easily implemented on top of Declarative. Well, maybe not *easily*,
but easier than without. A Datalog frontend is provided [in
Clojure(Script)](https://github.com/comnik/clj-3df).

Thanks to Differential Dataflow, all these capabilities are provided
within the dataflow model and can thus be scaled out to multiple
independent workers.

In order to provide all this in an arguably tasteful package,
Declarative is less efficient and much more opinionated than
hand-written Differential Dataflow. In particular, it enforces a
fully-normalized, RDF-like data model heavily inspired by systems like
[Datomic](https://docs.datomic.com/cloud/whatis/data-model.html) or
LogicBlox.

Other than that, Declarative is just Differential Dataflow under the
hood and can happily co-exist and interact with static, handwritten
dataflows.

Included in this repository is the library itself, as well as a server
binary, which wraps a Differential worker to accept commands and query
plans via WebSocket connections.

Declarative is in active development, with an alpha release scheduled
for Q1 2019.

## Build / Run

Assuming an up-to-date Rust environment, the server can be built and
run via

    cargo run --release --bin server -- <timely args> -- <server args>

The server executable accepts two sets of arguments separated by `--`,
one for [configuring timely
dataflow](https://github.com/frankmcsherry/timely-dataflow) and the
other for configuring the server itself.

## Configuration

    OPTION           | DESCRIPTION                | DEFAULT
    --port           | port to listen at          | 6262
    --enable-cli     | accept commands via stdin? | false

Logging at a specific level can be enabled by setting the `RUST_LOG`
environment variable to `RUST_LOG=server=info`.

Configuration options are still very much in flux and are found in
[the server module](src/server/mod.rs).

## Documentation

Important architectural decisions are documented in the
[docs/adr/](docs/adr/) sub-directory.

Documentation for this package can be built via `cargo doc --no-deps`
and viewed in a browser via `cargo doc --no-deps --open`. Please refer
to `declarative_dataflow::plan::Plan` for documentation on the
available operators. The [tests/](tests/) directory contains usage
examples.

## Clients

Query plans are rather cumbersome to write manually and do not map to
any interesting, higher-level semantics. Currently we provide a
[Datalog front end](https://github.com/comnik/clj-3df) written in
Clojure.

## Further Reading / Watching

[A post on the high-level motivation for this
project](https://www.nikolasgoebel.com/2018/09/13/incremental-datalog.html).

[[video] Reactive Datalog For
Datomic](https://www.youtube.com/watch?v=ZgqFlowyfTA), recorded at
Clojure/conj 2018.

The [Clockworks blog](https://www.clockworks.io/en/blog/) has a number
of posts on Declarative.
