//! Declarative dataflow infrastructure
//!
//! This crate contains types, traits, and logic for assembling
//! differential dataflow computations from declaratively specified
//! programs, without any additional compilation.

#![forbid(missing_docs)]

extern crate differential_dataflow;
extern crate timely;
extern crate timely_sort;
#[macro_use]
extern crate log;
extern crate abomonation;
#[macro_use]
extern crate serde_derive;
#[cfg(feature = "graphql")]
extern crate graphql_parser;
extern crate num_rational;

pub mod binding;
pub mod plan;
pub mod server;
pub mod sources;
pub mod timestamp;

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

use timely::dataflow::scopes::child::{Child, Iterative};
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::operators::group::Threshold;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use differential_dataflow::trace::wrappers::enter_at::TraceEnter as TraceEnterAt;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{Collection, Data};

pub use num_rational::Rational32;

pub use plan::{Hector, ImplContext, Implementable, Plan};

/// A unique entity identifier.
#[cfg(not(feature = "uuids"))]
pub type Eid = u64;

/// A unique entity identifier.
#[cfg(feature = "uuids")]
pub type Eid = u128;

/// A unique attribute identifier.
pub type Aid = String; // u32

/// Possible data values.
///
/// This enum captures the currently supported data types, and is the least common denominator
/// for the types of records moved around.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    /// An attribute identifier
    Aid(Aid),
    /// A string
    String(String),
    /// A boolean
    Bool(bool),
    /// A 64 bit signed integer
    Number(i64),
    /// A 32 bit rational
    Rational32(Rational32),
    /// An entity identifier
    Eid(Eid),
    /// Milliseconds since midnight, January 1, 1970 UTC
    Instant(u64),
    /// A 16 byte unique identifier.
    Uuid([u8; 16]),
}

/// A (tuple, time, diff) triple, as sent back to clients.
pub type Result = (Vec<Value>, u64, isize);

/// An entity, attribute, value triple.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Datom(pub Eid, pub Aid, pub Value);

/// A trace of values indexed by self.
pub type TraceKeyHandle<K, T, R> = TraceAgent<K, (), T, R, OrdKeySpine<K, T, R>>;

/// A trace of (K, V) pairs indexed by key.
pub type TraceValHandle<K, V, T, R> = TraceAgent<K, V, T, R, OrdValSpine<K, V, T, R>>;

// @TODO change this to TraceValHandle<Eid, Value> eventually
/// A handle to an arranged attribute.
pub type AttributeHandle = TraceValHandle<Value, Value, u64, isize>;

/// A handle to an arranged relation.
pub type RelationHandle = TraceKeyHandle<Vec<Value>, u64, isize>;

// A map for keeping track of collections that are being actively
// synthesized (i.e. that are not fully defined yet).
type VariableMap<G> = HashMap<String, Variable<G, Vec<Value>, isize>>;

/// Various indices over a collection of (K, V) pairs, required to
/// participate in delta-join pipelines.
pub struct CollectionIndex<K, V, T>
where
    K: Data,
    V: Data,
    T: Lattice + Data,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, isize>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, isize>,
}

impl<K, V, T> Clone for CollectionIndex<K, V, T>
where
    K: Data + Hash,
    V: Data + Hash,
    T: Lattice + Data + Timestamp,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T> CollectionIndex<K, V, T>
where
    K: Data + Hash,
    V: Data + Hash,
    T: Lattice + Data + Timestamp,
{
    /// Creates a named CollectionIndex from a (K, V) collection.
    pub fn index<G: Scope<Timestamp = T>>(
        name: &str,
        collection: &Collection<G, (K, V), isize>,
    ) -> Self {
        let counts = collection
            .map(|(k, _v)| (k, ()))
            .arrange_named(&format!("Counts({})", name))
            .trace;
        let propose = collection
            .arrange_named(&format!("Proposals({})", &name))
            .trace;
        let validate = collection
            .map(|t| (t, ()))
            .arrange_named(&format!("Validations({})", &name))
            .trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }

    /// Returns a LiveIndex that lives in the specified scope.
    pub fn import<G: Scope<Timestamp = T>>(
        &mut self,
        scope: &G,
    ) -> LiveIndex<
        G,
        K,
        V,
        TraceKeyHandle<K, T, isize>,
        TraceValHandle<K, V, T, isize>,
        TraceKeyHandle<(K, V), T, isize>,
    > {
        LiveIndex {
            count_trace: self.count_trace.import(scope),
            propose_trace: self.propose_trace.import(scope),
            validate_trace: self.validate_trace.import(scope),
        }
    }
}

/// CollectionIndex that was imported into a scope.
pub struct LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice + Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize> + Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize> + Clone,
    TrValidate: TraceReader<(K, V), (), G::Timestamp, isize> + Clone,
{
    count_trace: Arranged<G, K, (), isize, TrCount>,
    propose_trace: Arranged<G, K, V, isize, TrPropose>,
    validate_trace: Arranged<G, (K, V), (), isize, TrValidate>,
}

impl<G, K, V, TrCount, TrPropose, TrValidate> Clone
    for LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice + Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize> + Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize> + Clone,
    TrValidate: TraceReader<(K, V), (), G::Timestamp, isize> + Clone,
{
    fn clone(&self) -> Self {
        LiveIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<G, K, V, TrCount, TrPropose, TrValidate> LiveIndex<G, K, V, TrCount, TrPropose, TrValidate>
where
    G: Scope,
    G::Timestamp: Lattice + Data,
    K: Data,
    V: Data,
    TrCount: TraceReader<K, (), G::Timestamp, isize> + Clone,
    TrPropose: TraceReader<K, V, G::Timestamp, isize> + Clone,
    TrValidate: TraceReader<(K, V), (), G::Timestamp, isize> + Clone,
{
    /// Brings the index's traces into the specified scope.
    pub fn enter<'a, TInner>(
        &self,
        child: &Child<'a, G, TInner>,
    ) -> LiveIndex<
        Child<'a, G, TInner>,
        K,
        V,
        TraceEnter<K, (), G::Timestamp, isize, TrCount, TInner>,
        TraceEnter<K, V, G::Timestamp, isize, TrPropose, TInner>,
        TraceEnter<(K, V), (), G::Timestamp, isize, TrValidate, TInner>,
    >
    where
        TrCount::Batch: Clone,
        TrPropose::Batch: Clone,
        TrValidate::Batch: Clone,
        K: 'static,
        V: 'static,
        G::Timestamp: Clone + Default + 'static,
        TInner: Refines<G::Timestamp> + Lattice + Timestamp + Clone + Default + 'static,
    {
        LiveIndex {
            count_trace: self.count_trace.enter(child),
            propose_trace: self.propose_trace.enter(child),
            validate_trace: self.validate_trace.enter(child),
        }
    }

    /// Brings the index's traces into the specified scope.
    pub fn enter_at<'a, TInner, FCount, FPropose, FValidate>(
        &self,
        child: &Child<'a, G, TInner>,
        fcount: FCount,
        fpropose: FPropose,
        fvalidate: FValidate,
    ) -> LiveIndex<
        Child<'a, G, TInner>,
        K,
        V,
        TraceEnterAt<K, (), G::Timestamp, isize, TrCount, TInner, FCount>,
        TraceEnterAt<K, V, G::Timestamp, isize, TrPropose, TInner, FPropose>,
        TraceEnterAt<(K, V), (), G::Timestamp, isize, TrValidate, TInner, FValidate>,
    >
    where
        TrCount::Batch: Clone,
        TrPropose::Batch: Clone,
        TrValidate::Batch: Clone,
        K: 'static,
        V: 'static,
        G::Timestamp: Clone + Default + 'static,
        TInner: Refines<G::Timestamp> + Lattice + Timestamp + Clone + Default + 'static,
        FCount: Fn(&K, &(), &G::Timestamp) -> TInner + 'static,
        FPropose: Fn(&K, &V, &G::Timestamp) -> TInner + 'static,
        FValidate: Fn(&(K, V), &(), &G::Timestamp) -> TInner + 'static,
    {
        LiveIndex {
            count_trace: self.count_trace.enter_at(child, fcount),
            propose_trace: self.propose_trace.enter_at(child, fpropose),
            validate_trace: self.validate_trace.enter_at(child, fvalidate),
        }
    }
}

/// A symbol used in a query.
type Var = u32;

/// A named relation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Rule {
    /// The name identifying the relation.
    pub name: String,
    /// The plan describing contents of the relation.
    pub plan: Plan,
}

/// A relation between a set of symbols.
///
/// Relations can be backed by a collection of records of type
/// `Vec<Value>`, each of a common length (with offsets corresponding
/// to the symbol offsets), or by an existing arrangement.
trait Relation<'a, G: Scope>
where
    G::Timestamp: Lattice + Data,
{
    /// List the variable identifiers.
    fn symbols(&self) -> &[Var];

    /// A collection containing all tuples.
    fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize>;

    /// Returns the offset at which values for this symbol occur.
    fn offset(&self, sym: &Var) -> usize {
        self.symbols().iter().position(|&x| *sym == x).unwrap()
    }

    /// A collection with tuples partitioned by `syms`.
    ///
    /// Variables present in `syms` are collected in order and populate a first "key"
    /// `Vec<Value>`, followed by those variables not present in `syms`.
    fn tuples_by_symbols(
        self,
        syms: &[Var],
    ) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>;

    /// @TODO
    fn arrange_by_symbols(
        self,
        syms: &[Var],
    ) -> Arranged<
        Iterative<'a, G, u64>,
        Vec<Value>,
        Vec<Value>,
        isize,
        TraceValHandle<Vec<Value>, Vec<Value>, Product<G::Timestamp, u64>, isize>,
    >;
}

/// A collection and variable bindings.
pub struct CollectionRelation<'a, G: Scope> {
    symbols: Vec<Var>,
    tuples: Collection<Iterative<'a, G, u64>, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for CollectionRelation<'a, G>
where
    G::Timestamp: Lattice + Data,
{
    fn symbols(&self) -> &[Var] {
        &self.symbols
    }

    fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize> {
        self.tuples
    }

    /// Separates tuple fields by those in `syms` and those not.
    ///
    /// Each tuple is mapped to a pair `(Vec<Value>, Vec<Value>)`
    /// containing first exactly those symbols in `syms` in that
    /// order, followed by the remaining values in their original
    /// order.
    fn tuples_by_symbols(
        self,
        syms: &[Var],
    ) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize> {
        if syms == &self.symbols()[..] {
            self.tuples().map(|x| (x, Vec::new()))
        } else if syms.is_empty() {
            self.tuples().map(|x| (Vec::new(), x))
        } else {
            let key_length = syms.len();
            let values_length = self.symbols().len() - key_length;

            let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
            let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
            let sym_set: HashSet<Var> = syms.iter().cloned().collect();

            // It is important to preserve the key symbols in the order
            // they were specified.
            for sym in syms.iter() {
                key_offsets.push(self.symbols().iter().position(|&v| *sym == v).unwrap());
            }

            // Values we'll just take in the order they were.
            for (idx, sym) in self.symbols().iter().enumerate() {
                if sym_set.contains(sym) == false {
                    value_offsets.push(idx);
                }
            }

            // let debug_keys: Vec<String> = key_offsets.iter().map(|x| x.to_string()).collect();
            // let debug_values: Vec<String> = value_offsets.iter().map(|x| x.to_string()).collect();
            // println!("key offsets: {:?}", debug_keys);
            // println!("value offsets: {:?}", debug_values);

            self.tuples().map(move |tuple| {
                let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
                // @TODO second clone not really neccessary
                let values: Vec<Value> = value_offsets.iter().map(|i| tuple[*i].clone()).collect();

                (key, values)
            })
        }
    }

    fn arrange_by_symbols(
        self,
        syms: &[Var],
    ) -> Arranged<
        Iterative<'a, G, u64>,
        Vec<Value>,
        Vec<Value>,
        isize,
        TraceValHandle<Vec<Value>, Vec<Value>, Product<G::Timestamp, u64>, isize>,
    > {
        self.tuples_by_symbols(syms).arrange()
    }
}

// /// A arrangement and variable bindings.
// pub struct ArrangedRelation<'a, G: Scope>
// where
//     G::Timestamp: Lattice+Data
// {
//     symbols: Vec<Var>,
//     tuples: Arranged<Iterative<'a, G, u64>, Vec<Value>, Vec<Value>, isize,
//                      TraceValHandle<Vec<Value>, Vec<Value>, Product<G::Timestamp,u64>, isize>>,
// }

// impl<'a, G: Scope> Relation<'a, G> for ArrangedRelation<'a, G>
// where
//     G::Timestamp: Lattice+Data,
// {
//     fn symbols(&self) -> &[Var] { &self.symbols }

//     fn tuples(self) -> Collection<Iterative<'a, G, u64>, Vec<Value>, isize> {
//         unimplemented!()
//         self.tuples
//     }

//     /// Separates tuple fields by those in `syms` and those not.
//     ///
//     /// Each tuple is mapped to a pair `(Vec<Value>, Vec<Value>)`
//     /// containing first exactly those symbols in `syms` in that
//     /// order, followed by the remaining values in their original
//     /// order.
//     fn tuples_by_symbols
//         (self, syms: &[Var]) -> Collection<Iterative<'a, G, u64>, (Vec<Value>, Vec<Value>), isize>
//     {
//         self.arrange_by_symbols(syms).as_collection(|key,rest| (key,rest))
//     }

//     fn arrange_by_symbols
//         (self, syms: &[Var]) -> Arranged<Iterative<'a, G, u64>, Vec<Value>, Vec<Value>, isize,
//                                          TraceValHandle<Vec<Value>, Vec<Value>, Product<G::Timestamp,u64>, isize>>
//     {
//         if syms == &self.symbols()[..] {
//             self.tuples().map(|x| (x, Vec::new()))
//         } else if syms.is_empty() {
//             self.tuples().map(|x| (Vec::new(), x))
//         } else {
//             let key_length = syms.len();
//             let values_length = self.symbols().len() - key_length;

//             let mut key_offsets: Vec<usize> = Vec::with_capacity(key_length);
//             let mut value_offsets: Vec<usize> = Vec::with_capacity(values_length);
//             let sym_set: HashSet<Var> = syms.iter().cloned().collect();

//             // It is important to preserve the key symbols in the order
//             // they were specified.
//             for sym in syms.iter() {
//                 key_offsets.push(self.symbols().iter().position(|&v| *sym == v).unwrap());
//             }

//             // Values we'll just take in the order they were.
//             for (idx, sym) in self.symbols().iter().enumerate() {
//                 if sym_set.contains(sym) == false {
//                     value_offsets.push(idx);
//                 }
//             }

//             // let debug_keys: Vec<String> = key_offsets.iter().map(|x| x.to_string()).collect();
//             // let debug_values: Vec<String> = value_offsets.iter().map(|x| x.to_string()).collect();
//             // println!("key offsets: {:?}", debug_keys);
//             // println!("value offsets: {:?}", debug_values);

//             self.tuples().map(move |tuple| {
//                 let key: Vec<Value> = key_offsets.iter().map(|i| tuple[*i].clone()).collect();
//                 // @TODO second clone not really neccessary
//                 let values: Vec<Value> = value_offsets.iter().map(|i| tuple[*i].clone()).collect();

//                 (key, values)
//             })
//         }
//     }
// }

/// Returns a deduplicates list of all rules used in the definition of
/// the specified names. Includes the specified names.
pub fn collect_dependencies<I: ImplContext>(context: &I, names: &[&str]) -> Vec<Rule> {
    let mut seen = HashSet::new();
    let mut rules = Vec::new();
    let mut queue = VecDeque::new();

    for name in names {
        seen.insert(name.to_string());
        queue.push_back(context.rule(name).expect("unknown rule").clone());
    }

    while let Some(next) = queue.pop_front() {
        for dep_name in next.plan.dependencies().iter() {
            if !seen.contains(dep_name) {
                seen.insert(dep_name.to_string());
                queue.push_back(context.rule(dep_name).expect("unknown dependency").clone());
            }
        }

        rules.push(next);
    }

    rules
}

/// Takes a query plan and turns it into a differential dataflow.
pub fn implement<S: Scope<Timestamp = u64>, I: ImplContext>(
    name: &str,
    scope: &mut S,
    context: &mut I,
) -> HashMap<String, RelationHandle> {
    scope.iterative::<u64, _, _>(|nested| {
        let publish = vec![name];
        let mut rules = collect_dependencies(&*context, &publish[..]);

        let mut local_arrangements = VariableMap::new();
        let mut result_map = HashMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        if rules.is_empty() {
            panic!("Couldn't find any rules for that name.");
        }

        rules.sort_by(|x, y| x.name.cmp(&y.name));
        for index in 1..rules.len() - 1 {
            if rules[index].name == rules[index - 1].name {
                panic!("Duplicate rule definitions for rule {}", rules[index].name);
            }
        }

        // Step 1: Create new recursive variables for each rule.
        for rule in rules.iter() {
            local_arrangements.insert(rule.name.clone(), Variable::new(nested, Product::new(0, 1)));
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(name) {
                let trace = relation.leave().map(|t| (t, ())).arrange_named(name).trace;

                result_map.insert(name.to_string(), trace);
            } else {
                panic!("Attempted to publish undefined name {:?}", name);
            }
        }

        // Step 3: Define the executions for each rule.
        let mut executions = Vec::with_capacity(rules.len());
        for rule in rules.iter() {
            info!("planning {:?}", rule.name);
            executions.push(rule.plan.implement(nested, &local_arrangements, context));
        }

        // Step 4: Complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            local_arrangements
                .remove(&rule.name)
                .expect("Rule should be in local_arrangements, but isn't")
                .set(&execution.tuples().distinct());
        }

        result_map
    })
}

/// @TODO
pub fn implement_neu<S, I>(
    name: &str,
    scope: &mut S,
    context: &mut I,
) -> HashMap<String, RelationHandle>
where
    S: Scope<Timestamp = u64>,
    I: ImplContext,
{
    scope.iterative::<u64, _, _>(move |nested| {
        let publish = vec![name];
        let mut rules = collect_dependencies(&*context, &publish[..]);

        let mut local_arrangements = VariableMap::new();
        let mut result_map = HashMap::new();

        // Step 0: Canonicalize, check uniqueness of bindings.
        if rules.is_empty() {
            panic!("Couldn't find any rules for that name.");
        }

        rules.sort_by(|x, y| x.name.cmp(&y.name));
        for index in 1..rules.len() - 1 {
            if rules[index].name == rules[index - 1].name {
                panic!("Duplicate rule definitions for rule {}", rules[index].name);
            }
        }

        // @TODO at this point we need to know about...
        // @TODO ... which rules require recursion (and thus need wrapping in a Variable)
        // @TODO ... which rules are supposed to be re-used
        // @TODO ... which rules are supposed to be re-synthesized
        //
        // but based entirely on control data written to the server by something external
        // (for the old implement it could just be a decision based on whether the rule has a namespace)

        // Step 1: Create new recursive variables for each rule.
        for name in publish.iter() {
            local_arrangements.insert(name.to_string(), Variable::new(nested, Product::new(0, 1)));
        }

        // Step 2: Create public arrangements for published relations.
        for name in publish.into_iter() {
            if let Some(relation) = local_arrangements.get(name) {
                let trace = relation.leave().map(|t| (t, ())).arrange_named(name).trace;

                result_map.insert(name.to_string(), trace);
            } else {
                panic!("Attempted to publish undefined name {:?}", name);
            }
        }

        // Step 3: Define the executions for each rule.
        let mut executions = Vec::with_capacity(rules.len());
        for rule in rules.iter() {
            info!("neu_planning {:?}", rule.name);

            // @TODO here we need to split up the plan into multiple
            // Hector plans (one for each symbol)

            let plan = Plan::Hector(Hector {
                variables: rule.plan.variables(),
                bindings: rule.plan.into_bindings(),
            });

            executions.push(plan.implement(nested, &local_arrangements, context));
        }

        // Step 4: Complete named relations in a specific order (sorted by name).
        for (rule, execution) in rules.iter().zip(executions.drain(..)) {
            local_arrangements
                .remove(&rule.name)
                .expect("Rule should be in local_arrangements, but isn't")
                .set(&execution.tuples().distinct());
        }

        result_map
    })
}
