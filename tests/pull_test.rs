extern crate declarative_dataflow;
extern crate timely;

use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::Configuration;

use declarative_dataflow::binding::Binding;
#[cfg(feature = "graphql")]
use declarative_dataflow::plan::GraphQl;
use declarative_dataflow::plan::{Implementable, Pull, PullLevel};
use declarative_dataflow::server::{Server, Transact, TxData};
use declarative_dataflow::{Aid, Plan, Rule, Value};
use Value::{Bool, Eid, Number, String};

struct Case {
    description: &'static str,
    plan: Plan,
    transactions: Vec<Vec<TxData>>,
    expectations: Vec<Vec<(Vec<Value>, u64, isize)>>,
}

fn plan_dependencies(plan: &Plan) -> HashSet<Aid> {
    let mut deps = HashSet::new();

    for binding in plan.into_bindings().iter() {
        match binding {
            Binding::Attribute(binding) => {
                deps.insert(binding.source_attribute.clone());
            }
            _ => {}
        }
    }

    deps
}

fn path_dependencies(path: &PullLevel<Plan>) -> HashSet<Aid> {
    let mut deps = plan_dependencies(&path.plan);

    for name in path.pull_attributes.iter() {
        deps.insert(name.clone());
    }

    for name in path.path_attributes.iter() {
        deps.insert(name.clone());
    }

    deps
}

fn pull_dependencies(pull: &Pull<Plan>) -> HashSet<Aid> {
    let mut deps = HashSet::new();

    for path in pull.paths.iter() {
        for dep in path_dependencies(path) {
            deps.insert(dep);
        }
    }

    deps
}

fn dependencies(case: &Case) -> HashSet<Aid> {
    match case.plan {
        Plan::PullLevel(ref path) => path_dependencies(path),
        Plan::Pull(ref pull) => pull_dependencies(pull),
        _ => unimplemented!(),
    }
}

#[cfg(feature = "graphql")]
#[test]
fn graph_ql() {
    timely::execute(Configuration::Thread, |worker| {
        let mut server = Server::<u64>::new(Default::default());
        let (send_results, results) = channel();

        let plan = Plan::GraphQl(GraphQl {
            query: "{hero {name height mass}}".to_string(),
        });

        worker.dataflow::<u64, _, _>(|scope| {
            server.create_attribute("hero", scope);
            server.create_attribute("name", scope);
            server.create_attribute("height", scope);
            server.create_attribute("mass", scope);

            server
                .test_single(
                    scope,
                    Rule {
                        name: "graphQl".to_string(),
                        plan,
                    },
                )
                .inner
                .sink(Pipeline, "Results", move |input| {
                    input.for_each(|_time, data| {
                        for datum in data.iter() {
                            send_results.send(datum.clone()).unwrap()
                        }
                    });
                });
        });

        server.transact(
            Transact {
                tx: Some(0),
                tx_data: vec![
                    TxData(1, 100, "hero".to_string(), Eid(200)),
                    TxData(1, 200, "name".to_string(), String("Batman".to_string())),
                    TxData(1, 200, "mass".to_string(), String("80kg".to_string())),
                ],
            },
            0,
            0,
        );

        worker.step_while(|| server.is_any_outdated());

        let mut expected = HashSet::new();

        expected.insert((
            vec![
                Eid(100),
                Value::Aid("hero".to_string()),
                Eid(200),
                Value::Aid("mass".to_string()),
                String("80kg".to_string()),
            ],
            0,
            1,
        ));

        expected.insert((
            vec![
                Eid(100),
                Value::Aid("hero".to_string()),
                Eid(200),
                Value::Aid("name".to_string()),
                String("Batman".to_string()),
            ],
            0,
            1,
        ));

        for _i in 0..expected.len() {
            let result = results.recv_timeout(Duration::from_millis(400)).unwrap();
            if !expected.remove(&result) {
                panic!("unknown result {:?}", result);
            }
        }

        assert!(results.recv_timeout(Duration::from_millis(400)).is_err());
    })
    .unwrap();
}

#[test]
fn run_pull_cases() {
    let mut cases = vec![
        Case {
            description: "[name age]",
            plan: Plan::PullLevel(PullLevel {
                variables: vec![],
                plan: Box::new(Plan::MatchAV(0, "admin?".to_string(), Bool(false))),
                pull_attributes: vec!["name".to_string(), "age".to_string()],
                path_attributes: vec!["root".to_string()],
            }),
            transactions: vec![vec![
                TxData(1, 100, "admin?".to_string(), Bool(true)),
                TxData(1, 200, "admin?".to_string(), Bool(false)),
                TxData(1, 300, "admin?".to_string(), Bool(false)),
                TxData(1, 100, "name".to_string(), String("Mabel".to_string())),
                TxData(1, 200, "name".to_string(), String("Dipper".to_string())),
                TxData(1, 300, "name".to_string(), String("Soos".to_string())),
                TxData(1, 100, "age".to_string(), Number(12)),
                TxData(1, 200, "age".to_string(), Number(13)),
            ]],
            expectations: vec![vec![
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(200),
                        Value::Aid("age".to_string()),
                        Number(13),
                    ],
                    0,
                    1,
                ),
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(200),
                        Value::Aid("name".to_string()),
                        String("Dipper".to_string()),
                    ],
                    0,
                    1,
                ),
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(300),
                        Value::Aid("name".to_string()),
                        String("Soos".to_string()),
                    ],
                    0,
                    1,
                ),
            ]],
        },
        Case {
            description: "[{parent/child [name age]}]",
            plan: Plan::PullLevel(PullLevel {
                variables: vec![],
                plan: Box::new(Plan::MatchA(0, "parent/child".to_string(), 1)),
                pull_attributes: vec!["name".to_string(), "age".to_string()],
                path_attributes: vec!["root".to_string(), "parent/child".to_string()],
            }),
            transactions: vec![vec![
                TxData(1, 100, "name".to_string(), String("Alice".to_string())),
                TxData(1, 100, "parent/child".to_string(), Eid(300)),
                TxData(1, 200, "name".to_string(), String("Bob".to_string())),
                TxData(1, 200, "parent/child".to_string(), Eid(400)),
                TxData(1, 300, "name".to_string(), String("Mabel".to_string())),
                TxData(1, 300, "age".to_string(), Number(13)),
                TxData(1, 400, "name".to_string(), String("Dipper".to_string())),
                TxData(1, 400, "age".to_string(), Number(12)),
            ]],
            expectations: vec![vec![
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(100),
                        Value::Aid("parent/child".to_string()),
                        Eid(300),
                        Value::Aid("age".to_string()),
                        Number(13),
                    ],
                    0,
                    1,
                ),
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(100),
                        Value::Aid("parent/child".to_string()),
                        Eid(300),
                        Value::Aid("name".to_string()),
                        String("Mabel".to_string()),
                    ],
                    0,
                    1,
                ),
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(200),
                        Value::Aid("parent/child".to_string()),
                        Eid(400),
                        Value::Aid("age".to_string()),
                        Number(12),
                    ],
                    0,
                    1,
                ),
                (
                    vec![
                        Value::Aid("root".to_string()),
                        Eid(200),
                        Value::Aid("parent/child".to_string()),
                        Eid(400),
                        Value::Aid("name".to_string()),
                        String("Dipper".to_string()),
                    ],
                    0,
                    1,
                ),
            ]],
        },
        {
            let (a, b, c) = (1, 2, 3);
            Case {
                description: "[name {join/binding [pattern/e pattern/a pattern/v]}]",
                plan: Plan::Pull(Pull {
                    variables: vec![],
                    paths: vec![
                        PullLevel {
                            variables: vec![],
                            plan: Box::new(Plan::MatchA(a, "join/binding".to_string(), b)),
                            pull_attributes: vec![
                                "pattern/e".to_string(),
                                "pattern/a".to_string(),
                                "pattern/v".to_string(),
                            ],
                            path_attributes: vec!["root".to_string(), "join/binding".to_string()],
                        },
                        PullLevel {
                            variables: vec![],
                            plan: Box::new(Plan::MatchA(a, "name".to_string(), c)),
                            pull_attributes: vec![],
                            path_attributes: vec!["root".to_string(), "name".to_string()],
                        },
                    ],
                }),
                transactions: vec![vec![
                    TxData(1, 100, "name".to_string(), String("rule".to_string())),
                    TxData(1, 100, "join/binding".to_string(), Eid(200)),
                    TxData(1, 100, "join/binding".to_string(), Eid(300)),
                    TxData(
                        1,
                        200,
                        "pattern/a".to_string(),
                        Value::Aid("xyz".to_string()),
                    ),
                    TxData(1, 300, "pattern/e".to_string(), Eid(12345)),
                    TxData(
                        1,
                        300,
                        "pattern/a".to_string(),
                        Value::Aid("asd".to_string()),
                    ),
                ]],
                expectations: vec![vec![
                    (
                        vec![
                            Value::Aid("root".to_string()),
                            Eid(100),
                            Value::Aid("name".to_string()),
                            String("rule".to_string()),
                        ],
                        0,
                        1,
                    ),
                    (
                        vec![
                            Value::Aid("root".to_string()),
                            Eid(100),
                            Value::Aid("join/binding".to_string()),
                            Eid(200),
                            Value::Aid("pattern/a".to_string()),
                            Value::Aid("xyz".to_string()),
                        ],
                        0,
                        1,
                    ),
                    (
                        vec![
                            Value::Aid("root".to_string()),
                            Eid(100),
                            Value::Aid("join/binding".to_string()),
                            Eid(300),
                            Value::Aid("pattern/e".to_string()),
                            Eid(12345),
                        ],
                        0,
                        1,
                    ),
                    (
                        vec![
                            Value::Aid("root".to_string()),
                            Eid(100),
                            Value::Aid("join/binding".to_string()),
                            Eid(300),
                            Value::Aid("pattern/a".to_string()),
                            Value::Aid("asd".to_string()),
                        ],
                        0,
                        1,
                    ),
                ]],
            }
        },
    ];

    for case in cases.drain(..) {
        timely::execute(Configuration::Thread, move |worker| {
            let mut server = Server::<u64>::new(Default::default());
            let (send_results, results) = channel();

            dbg!(case.description);

            let deps = dependencies(&case);
            let plan = case.plan.clone();

            worker.dataflow::<u64, _, _>(|scope| {
                for dep in deps.iter() {
                    server.create_attribute(dep, scope);
                }

                server
                    .test_single(
                        scope,
                        Rule {
                            name: "hector".to_string(),
                            plan,
                        },
                    )
                    .inner
                    .sink(Pipeline, "Results", move |input| {
                        input.for_each(|_time, data| {
                            for datum in data.iter() {
                                send_results.send(datum.clone()).unwrap()
                            }
                        });
                    });
            });

            let mut transactions = case.transactions.clone();

            for (tx_id, tx_data) in transactions.drain(..).enumerate() {
                let tx = Some(tx_id as u64);
                server.transact(Transact { tx, tx_data }, 0, 0);

                worker.step_while(|| server.is_any_outdated());

                let mut expected: HashSet<(Vec<Value>, u64, isize)> =
                    HashSet::from_iter(case.expectations[tx_id].iter().cloned());

                for _i in 0..expected.len() {
                    match results.recv_timeout(Duration::from_millis(400)) {
                        Err(_err) => {
                            panic!("No result.");
                        }
                        Ok(result) => {
                            if !expected.remove(&result) {
                                panic!("Unknown result {:?}.", result);
                            }
                        }
                    }
                }

                match results.recv_timeout(Duration::from_millis(400)) {
                    Err(_err) => {}
                    Ok(result) => {
                        panic!("Extraneous result {:?}", result);
                    }
                }
            }
        })
        .unwrap();
    }
}
