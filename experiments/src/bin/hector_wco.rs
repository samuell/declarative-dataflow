use graph_map::GraphMMap;

use declarative_dataflow::server::Server;
use declarative_dataflow::{q, AttributeSemantics, Binding, Rule, TxData, Value};
use Value::Eid;

fn main() {
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        let timer = std::time::Instant::now();
        let graph = GraphMMap::new(&filename);
        let mut server = Server::<u64, u64>::new(Default::default());

        // [?a :edge ?b] [?b :edge ?c] [?a :edge ?c]
        let (a, b, c) = (1, 2, 3);
        let plan = q(
            vec![a, b, c],
            vec![
                Binding::attribute(a, "edge", b),
                Binding::attribute(b, "edge", c),
                Binding::attribute(a, "edge", c),
            ],
        );

        let peers = worker.peers();
        let index = worker.index();

        worker.dataflow::<u64, _, _>(|scope| {
            server
                .context
                .internal
                .create_attribute("edge", AttributeSemantics::Raw, scope)
                .unwrap();

            server
                .test_single(
                    scope,
                    Rule {
                        name: "triangles".to_string(),
                        plan,
                    },
                )
                .filter(move |_| inspect)
                .inspect(|x| println!("\tTriangle: {:?}", x));

            server.advance_domain(None, 1).unwrap();
        });

        let mut index = index;
        let mut next_tx = 1;

        while index < graph.nodes() {
            server
                .transact(
                    graph
                        .edges(index)
                        .iter()
                        .map(|y| TxData(1, index as u64, "edge".to_string(), Eid(*y as u64)))
                        .collect(),
                    0,
                    0,
                )
                .unwrap();

            server.advance_domain(None, next_tx).unwrap();
            next_tx += 1;

            index += peers;
            if (index / peers) % batching == 0 {
                worker.step_while(|| server.is_any_outdated());
                println!("{:?}\tRound {} complete", timer.elapsed(), index);
            }
        }
    })
    .unwrap();
}
