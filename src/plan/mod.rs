//! Types and traits for implementing query plans.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::scopes::child::Iterative;

use {Aid, Eid, Value, Var};
use {Rule, Binding};
use {CollectionIndex, RelationHandle, Relation, VariableMap, CollectionRelation};

pub mod project;
pub mod aggregate;
pub mod union;
pub mod join;
pub mod hector;
pub mod antijoin;
pub mod filter;
pub mod transform;
pub mod pull;

pub use self::project::Project;
pub use self::aggregate::{Aggregate, AggregationFn};
pub use self::union::Union;
pub use self::join::Join;
pub use self::hector::Hector;
pub use self::antijoin::Antijoin;
pub use self::filter::{Filter, Predicate};
pub use self::transform::{Function, Transform};
pub use self::pull::{Pull, PullLevel};

/// A thing that can provide global state required during the
/// implementation of plans.
pub trait ImplContext {
    /// Returns the set of constraints associated with a rule.
    fn rule
        (&self, name: &str) -> Option<&Rule>;
    
    /// Returns a mutable reference to a (non-base) relation, if one
    /// is registered under the given name.
    fn global_arrangement
        (&mut self, name: &str) -> Option<&mut RelationHandle>;

    /// Returns a mutable reference to an attribute (a base relation)
    /// arranged from eid -> value, if one is registered under the
    /// given name.
    fn forward_index
        (&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>>;

    /// Returns a mutable reference to an attribute (a base relation)
    /// arranged from value -> eid, if one is registered under the
    /// given name.
    fn reverse_index
        (&mut self, name: &str) -> Option<&mut CollectionIndex<Value, Value, u64>>;
}

/// A type that can be implemented as a simple relation.
pub trait Implementable {
    /// Returns names of any other implementable things that need to
    /// be available before implementing this one. Attributes are not
    /// mentioned explicitley as dependencies.
    fn dependencies(&self) -> Vec<String>;

    /// Transforms an implementable into an equivalent set of bindings
    /// that can be unified by Hector.
    fn into_bindings(&self) -> Vec<Binding> {
        panic!("This plan can't be implemented via Hector.");
    }
    
    /// Implements the type as a simple relation.
    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S>;
}

/// Possible query plan types.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Plan {
    /// Projection
    Project(Project<Plan>),
    /// Aggregation
    Aggregate(Aggregate<Plan>),
    /// Union
    Union(Union<Plan>),
    /// Equijoin
    Join(Join<Plan, Plan>),
    /// WCO
    Hector(Hector),
    /// Antijoin
    Antijoin(Antijoin<Plan, Plan>),
    /// Negation
    Negate(Box<Plan>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Plan>),
    /// Transforms a binding by a function expression
    Transform(Transform<Plan>),
    /// Data pattern of the form [?e a ?v]
    MatchA(Var, Aid, Var),
    /// Data pattern of the form [e a ?v]
    MatchEA(Eid, Aid, Var),
    /// Data pattern of the form [?e a v]
    MatchAV(Var, Aid, Value),
    /// Sources data from a query-local relation
    RuleExpr(Vec<Var>, String),
    /// Sources data from a published relation
    NameExpr(Vec<Var>, String),
    /// Pull expression
    Pull(Pull<Plan>),
    /// Single-level pull expression
    PullLevel(PullLevel<Plan>),
}

impl Implementable for Plan {

    fn dependencies(&self) -> Vec<String> {
        // @TODO provide a general fold for plans
        match self {
            &Plan::Project(ref projection) => projection.dependencies(),
            &Plan::Aggregate(ref aggregate) => aggregate.dependencies(),
            &Plan::Union(ref union) => union.dependencies(),
            &Plan::Join(ref join) => join.dependencies(),
            &Plan::Hector(ref hector) => hector.dependencies(),
            &Plan::Antijoin(ref antijoin) => antijoin.dependencies(),
            &Plan::Negate(ref plan) => plan.dependencies(),
            &Plan::Filter(ref filter) => filter.dependencies(),
            &Plan::Transform(ref transform) => transform.dependencies(),
            &Plan::MatchA(_, _, _) => Vec::new(),
            &Plan::MatchEA(_, _, _) => Vec::new(),
            &Plan::MatchAV(_, _, _) => Vec::new(),
            &Plan::RuleExpr(_, ref name) => vec![name.to_string()],
            &Plan::NameExpr(_, ref name) => vec![name.to_string()],
            &Plan::Pull(ref pull) => pull.dependencies(),
            &Plan::PullLevel(ref path) => path.dependencies(),
        }
    }

    fn into_bindings(&self) -> Vec<Binding> {
        // @TODO provide a general fold for plans
        match self {
            &Plan::Project(ref projection) => projection.into_bindings(),
            &Plan::Aggregate(ref aggregate) => aggregate.into_bindings(),
            &Plan::Union(ref union) => union.into_bindings(),
            &Plan::Join(ref join) => join.into_bindings(),
            &Plan::Hector(ref hector) => hector.into_bindings(),
            &Plan::Antijoin(ref antijoin) => antijoin.into_bindings(),
            &Plan::Negate(ref plan) => plan.into_bindings(),
            &Plan::Filter(ref filter) => filter.into_bindings(),
            &Plan::Transform(ref transform) => transform.into_bindings(),
            &Plan::MatchA(e, ref a, v) => vec![Binding { symbols: (e, v,), source_name: a.to_string() }],
            &Plan::MatchEA(_, _, _) => panic!("Only MatchA is supported in Hector."),
            &Plan::MatchAV(_, _, _) => panic!("Only MatchA is supported in Hector."),
            &Plan::RuleExpr(_, ref name) => unimplemented!(), // @TODO hmm...
            &Plan::NameExpr(_, ref name) => unimplemented!(), // @TODO hmm...
            &Plan::Pull(ref pull) => pull.into_bindings(),
            &Plan::PullLevel(ref path) => path.into_bindings(),
        }
    }

    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S>
    {
        match self {
            &Plan::Project(ref projection) => {
                projection.implement(nested, local_arrangements, context)
            }
            &Plan::Aggregate(ref aggregate) => {
                aggregate.implement(nested, local_arrangements, context)
            }
            &Plan::Union(ref union) => {
                union.implement(nested, local_arrangements, context)
            }
            &Plan::Join(ref join) => {
                join.implement(nested, local_arrangements, context)
            }
            &Plan::Hector(ref hector) => {
                hector.implement(nested, local_arrangements, context)
            }
            &Plan::Antijoin(ref antijoin) => {
                antijoin.implement(nested, local_arrangements, context)
            }
            &Plan::Negate(ref plan) => {
                let mut rel = plan.implement(nested, local_arrangements, context);
                CollectionRelation {
                    symbols: rel.symbols().to_vec(),
                    tuples: rel.tuples().negate(),
                }
            }
            &Plan::Filter(ref filter) => {
                filter.implement(nested, local_arrangements, context)
            }
            &Plan::Transform(ref transform) => {
                transform.implement(nested, local_arrangements, context)
            }
            &Plan::MatchA(sym1, ref a, sym2) => {
                let tuples = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => index
                        .validate_trace
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .as_collection(|(e,v), _| vec![e.clone(), v.clone()]),
                };

                CollectionRelation {
                    symbols: vec![sym1, sym2],
                    tuples,
                }
            }
            &Plan::MatchEA(match_e, ref a, sym1) => {
                let tuples = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => index
                        .propose_trace
                        .import_named(&nested.parent, a)
                        .enter(nested)
                        .filter(move |e, _v| *e == Value::Eid(match_e))
                        .as_collection(|_e, v| vec![v.clone()]),
                };

                CollectionRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            &Plan::MatchAV(sym1, ref a, ref match_v) => {
                let tuples = match context.reverse_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => {
                        let match_v = match_v.clone();
                        index
                            .propose_trace
                            .import_named(&nested.parent, a)
                            .enter(nested)
                            .filter(move |v, _e| *v == match_v)
                            .as_collection(|_v, e| vec![e.clone()])
                    }
                };

                CollectionRelation {
                    symbols: vec![sym1],
                    tuples,
                }
            }
            &Plan::RuleExpr(ref syms, ref name) => match local_arrangements.get(name) {
                None => panic!("{:?} not in relation map", name),
                Some(named) => CollectionRelation {
                    symbols: syms.clone(),
                    tuples: named.map(|tuple| tuple.clone()),
                },
            },
            &Plan::NameExpr(ref syms, ref name) => match context.global_arrangement(name) {
                None => panic!("{:?} not in query map", name),
                Some(named) => CollectionRelation {
                    symbols: syms.clone(),
                    tuples: named
                        .import_named(&nested.parent, name)
                        .enter(nested)
                        // @TODO this destroys all the arrangement re-use
                        .as_collection(|tuple, _| tuple.clone()),
                },
            },
            &Plan::Pull(ref pull) => {
                pull.implement(nested, local_arrangements, context)
            },
            &Plan::PullLevel(ref path) => {
                path.implement(nested, local_arrangements, context)
            },
        }
    }
}
