//! Pull expression plan, but without nesting.

use timely::dataflow::operators::Concatenate;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::Scope;
use timely::order::{Product, TotalOrder};
use timely::progress::Timestamp;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;

#[cfg(feature = "graphql")]
use crate::graphql_parser::parse_query;

#[cfg(feature = "graphql")]
use crate::graphql_parser::query::{
    Definition, Document, OperationDefinition, Selection, SelectionSet,
};

#[cfg(feature = "graphql")]
use crate::plan::Plan;

use crate::plan::{Dependencies, ImplContext, Implementable};
use crate::{Aid, CollectionRelation, Relation, ShutdownHandle, Value, Var, VariableMap};

/// A plan stage for extracting all matching [e a v] tuples for a
/// given set of attributes and an input relation specifying entities.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct PullLevel<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Plan for the input relation.
    pub plan: Box<P>,
    /// Attributes to pull for the input entities.
    pub pull_attributes: Vec<Aid>,
    /// Attribute names to distinguish plans of the same
    /// length. Useful to feed into a nested hash-map directly.
    pub path_attributes: Vec<Aid>,
}

/// A plan stage for pull queries split into individual paths. So
/// `[:parent/name {:parent/child [:child/name]}]` would be
/// represented as:
///
/// (?parent)                      <- [:parent/name] | no constraints
/// (?parent :parent/child ?child) <- [:child/name]  | [?parent :parent/child ?child]
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct Pull<P: Implementable> {
    /// TODO
    pub variables: Vec<Var>,
    /// Individual paths to pull.
    pub paths: Vec<PullLevel<P>>,
}

/// A plan for GraphQL queries, e.g. `{ Heroes { name age weight } }`
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GraphQl {
    /// String representation of GraphQL query
    pub query: String,
}

fn interleave(values: &[Value], constants: &[Aid]) -> Vec<Value> {
    if values.is_empty() || constants.is_empty() {
        values.to_owned()
    } else {
        let size: usize = values.len() + constants.len();
        // + 2, because we know there'll be a and v coming...
        let mut result: Vec<Value> = Vec::with_capacity(size + 2);

        let mut next_value = 0;
        let mut next_const = 0;

        for i in 0..size {
            if i % 2 == 0 {
                // on odd indices we interleave an attribute
                let a = constants[next_const].clone();
                result.push(Value::Aid(a));
                next_const += 1;
            } else {
                // on even indices we take from the result tuple
                result.push(values[next_value].clone());
                next_value += 1;
            }
        }

        result
    }
}

impl<P: Implementable> Implementable for PullLevel<P> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (CollectionRelation<'b, S>, ShutdownHandle<T>)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
        use differential_dataflow::operators::JoinCore;
        use differential_dataflow::trace::implementations::ord::OrdValSpine;
        use differential_dataflow::trace::TraceReader;

        let (input, shutdown_input) = self.plan.implement(nested, local_arrangements, context);

        if self.pull_attributes.is_empty() {
            if self.path_attributes.is_empty() {
                // nothing to pull
                (input, shutdown_input)
            } else {
                let path_attributes = self.path_attributes.clone();
                let tuples = input
                    .tuples()
                    .map(move |tuple| interleave(&tuple, &path_attributes));

                let relation = CollectionRelation {
                    variables: vec![],
                    tuples,
                };

                (relation, shutdown_input)
            }
        } else {
            // Arrange input entities by eid.
            let paths = input.tuples();
            let e_path: Arranged<
                Iterative<S, u64>,
                Value,
                Vec<Value>,
                isize,
                TraceAgent<
                    Value,
                    Vec<Value>,
                    Product<T, u64>,
                    isize,
                    OrdValSpine<Value, Vec<Value>, Product<T, u64>, isize>,
                >,
            > = paths.map(|t| (t.last().unwrap().clone(), t)).arrange();

            let mut shutdown_handle = shutdown_input;
            let streams = self.pull_attributes.iter().map(|a| {
                let e_v = match context.forward_index(a) {
                    None => panic!("attribute {:?} does not exist", a),
                    Some(index) => {
                        let frontier: Vec<T> = index.propose_trace.advance_frontier().to_vec();
                        let (arranged, shutdown_propose) =
                            index.propose_trace.import_core(&nested.parent, a);

                        let e_v = arranged.enter_at(nested, move |_, _, time| {
                            let mut forwarded = time.clone();
                            forwarded.advance_by(&frontier);
                            Product::new(forwarded, 0)
                        });

                        shutdown_handle.add_button(shutdown_propose);

                        e_v
                    }
                };

                let attribute = Value::Aid(a.clone());
                let path_attributes: Vec<Aid> = self.path_attributes.clone();

                e_path
                    .join_core(&e_v, move |_e, path: &Vec<Value>, v: &Value| {
                        // Each result tuple must hold the interleaved
                        // path, the attribute, and the value,
                        // i.e. [?p "parent/child" ?c ?a ?v]
                        let mut result = interleave(path, &path_attributes);
                        result.push(attribute.clone());
                        result.push(v.clone());

                        Some(result)
                    })
                    .inner
            });

            let tuples = nested.concatenate(streams).as_collection();

            let relation = CollectionRelation {
                variables: vec![], // @TODO
                tuples,
            };

            (relation, shutdown_handle)
        }
    }
}

impl<P: Implementable> Implementable for Pull<P> {
    fn dependencies(&self) -> Dependencies {
        Dependencies::none()
    }

    fn implement<'b, T, I, S>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> (CollectionRelation<'b, S>, ShutdownHandle<T>)
    where
        T: Timestamp + Lattice + TotalOrder,
        I: ImplContext<T>,
        S: Scope<Timestamp = T>,
    {
        let mut scope = nested.clone();
        let mut shutdown_handle = ShutdownHandle::empty();

        let streams = self.paths.iter().map(|path| {
            let (relation, shutdown) = path.implement(&mut scope, local_arrangements, context);

            shutdown_handle.merge_with(shutdown);

            relation.tuples().inner
        });

        let tuples = nested.concatenate(streams).as_collection();

        let relation = CollectionRelation {
            variables: vec![], // @TODO
            tuples,
        };

        (relation, shutdown_handle)
    }
}

#[cfg(feature = "graphql")]
fn selection_set_to_paths(
    selection_set: &SelectionSet,
    parent_path: &Vec<String>,
    at_root: bool,
) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    let mut pull_attributes = vec![];
    let variables = vec![];

    for item in &selection_set.items {
        match item {
            Selection::Field(field) => {
                if field.selection_set.items.is_empty() {
                    pull_attributes.push(field.name.to_string());
                }

                let mut new_parent_path = parent_path.to_vec();
                new_parent_path.push(field.name.to_string());

                result.extend(selection_set_to_paths(
                    &field.selection_set,
                    &new_parent_path,
                    parent_path.is_empty(),
                ));
            }
            _ => unimplemented!(),
        }
    }

    // parent_path handles root path case
    if !pull_attributes.is_empty() && !parent_path.is_empty() {
        // for root, we expect a NameExpr that puts the pulled IDs in the v position
        let plan;
        if at_root {
            plan = Box::new(Plan::NameExpr(
                vec![0, 1],
                parent_path.last().unwrap().to_string(),
            ));
        } else {
            plan = Box::new(Plan::MatchA(0, parent_path.last().unwrap().to_string(), 1));
        }

        let pull_level = PullLevel {
            pull_attributes,
            path_attributes: parent_path.to_vec(),
            variables,
            plan,
        };
        result.push(pull_level);
    }

    result
}

/// converts an ast to paths
/// The structure of a typical parsed ast looks like this:
/// ```
/// Document {
///   definitions: [
///     Operation(SelectionSet(SelectionSet {
///       items: [
///         Field(Field {
///           name: ...,
///           selection_set: SelectionSet(...}
///         }),
///         ...
///       ]
///     }))
///   ]
/// }
/// ```
#[cfg(feature = "graphql")]
fn ast_to_paths(ast: Document) -> Vec<PullLevel<Plan>> {
    let mut result = vec![];
    for definition in &ast.definitions {
        match definition {
            Definition::Operation(operation_definition) => match operation_definition {
                OperationDefinition::Query(query) => unimplemented!(),
                OperationDefinition::SelectionSet(selection_set) => {
                    result.extend(selection_set_to_paths(selection_set, &vec![], true))
                }
                _ => unimplemented!(),
            },
            Definition::Fragment(fragment_definition) => unimplemented!(),
        };
    }

    result
}

#[cfg(feature = "graphql")]
impl Implementable for GraphQl {
    fn dependencies(&self) -> Vec<String> {
        // @TODO cache this?
        let ast = parse_query(&self.query).expect("graphQL ast parsing failed");
        let parsed = Pull {
            variables: vec![],
            paths: ast_to_paths(ast),
        };

        parsed.dependencies()
    }

    fn implement<'b, S: Scope<Timestamp = u64>, I: ImplContext>(
        &self,
        nested: &mut Iterative<'b, S, u64>,
        local_arrangements: &VariableMap<Iterative<'b, S, u64>>,
        context: &mut I,
    ) -> CollectionRelation<'b, S> {
        let ast = parse_query(&self.query).expect("graphQL ast parsing failed");
        let parsed = Pull {
            variables: vec![],
            paths: ast_to_paths(ast),
        };

        parsed.implement(nested, local_arrangements, context)
    }
}
