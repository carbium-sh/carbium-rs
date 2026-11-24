//! Every served method has a typed call, and nothing else has one. The method table and the typed calls are compared
//! with the vendored spec/served.json both ways, and each typed call is made through the client's own path against a
//! recording server, so a call that exists but sends another name, or other params, is caught too. The params are
//! the spec's own examples (spec/examples.json), read into the call's typed arguments and written back by the client.

#[path = "../tests/common/mod.rs"]
mod common;

use std::collections::BTreeSet;
use std::path::Path;

use crate::generated::typed_calls;
use carbium::{Carbium, ClientOptions, METHODS, ParamStructure, Value, method_spec};
use common::{Planted, Reply, key, rpc_ok};

fn spec(file: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("spec")
        .join(file);
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn served(group: &str) -> Vec<String> {
    spec("served.json")[group]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

/// Names served but missing from `have`, then names in `have` that are not served.
fn both_ways(what: &str, have: &[&str], served: &[String]) -> Vec<String> {
    let have: BTreeSet<&str> = have.iter().copied().collect();
    let served: BTreeSet<&str> = served.iter().map(String::as_str).collect();
    let mut out: Vec<String> = served
        .difference(&have)
        .map(|n| format!("served but has no {what}: {n}"))
        .collect();
    out.extend(
        have.difference(&served)
            .map(|n| format!("has a {what} but is not served: {n}")),
    );
    out
}

#[test]
fn the_method_table_and_the_typed_calls_are_exactly_the_served_methods() {
    let rpc = served("rpc");
    let table: Vec<&str> = METHODS.iter().map(|m| m.name).collect();
    let calls: Vec<&str> = typed_calls::ALL.iter().map(|(name, _)| *name).collect();
    assert_eq!(both_ways("table entry", &table, &rpc), Vec::<String>::new());
    assert_eq!(both_ways("typed call", &calls, &rpc), Vec::<String>::new());
    assert_eq!((table.len(), calls.len(), rpc.len()), (68, 68, 68));
    assert_eq!(
        table.iter().collect::<BTreeSet<_>>().len(),
        68,
        "a name is listed twice"
    );

    // The check itself catches a missing name and an extra one.
    let less: Vec<&str> = table.iter().copied().filter(|n| *n != "getSlot").collect();
    assert_eq!(
        both_ways("table entry", &less, &rpc),
        ["served but has no table entry: getSlot"]
    );
    let more: Vec<&str> = table.iter().copied().chain(["notAMethod"]).collect();
    assert_eq!(
        both_ways("table entry", &more, &rpc),
        ["has a table entry but is not served: notAMethod"]
    );
}

#[cfg(feature = "ws")]
#[test]
fn the_subscription_names_are_the_served_ones() {
    assert_eq!(
        both_ways("subscription", carbium::SUBSCRIPTIONS, &served("ws")),
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_typed_call_sends_its_own_name_with_the_example_params() {
    let server = Planted::start(vec![rpc_ok(Value::Null).into()]).await;
    let client = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: server.url.clone(),
        retry: None,
        ..Default::default()
    })
    .unwrap();
    let examples = spec("examples.json");
    let mut wrong = Vec::new();
    for (wire, call) in typed_calls::ALL {
        // examples.json writes an object-params call as a list holding that one object (the spec's build script)
        let example = match (&examples[*wire], method_spec(wire).map(|m| m.params)) {
            (Value::Array(one), Some(ParamStructure::ByName)) if one.len() <= 1 => one
                .first()
                .cloned()
                .unwrap_or(Value::Object(Default::default())),
            (other, _) => other.clone(),
        };
        let before = server.count();
        let outcome = call(&client, example.clone()).await; // a null result need not read as the type; the wire is what counts
        if server.count() != before + 1 {
            wrong.push(format!(
                "{wire}: sent {} requests ({outcome:?})",
                server.count() - before
            ));
            continue;
        }
        let sent = server.seen()[before].json();
        if sent["method"] != *wire {
            wrong.push(format!("{wire}: its call sends {}", sent["method"]));
        }
        let empty = if example.is_array() {
            Value::Array(vec![])
        } else {
            Value::Object(Default::default())
        };
        let params = sent.get("params").cloned().unwrap_or(empty);
        if params != example {
            wrong.push(format!(
                "{wire}: sends params {params}, the example is {example}"
            ));
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
    assert_eq!(server.count(), 68);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rest_routes_are_the_served_ones_and_each_call_takes_its_route() {
    assert_eq!(
        both_ways("REST call", carbium::REST_ROUTES, &served("rest")),
        Vec::<String>::new()
    );
    let server = Planted::start(vec![Reply::json(200, serde_json::json!({})).into()]).await;
    let client = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: server.url.clone(),
        retry: None,
        ..Default::default()
    })
    .unwrap();
    #[allow(deprecated)]
    let _ = client.cq1_quote("a", "b", 1, 50).await; // an empty answer need not read as a quote; the route is what counts
    let seen = &server.seen()[0];
    let route = format!("{} {}", seen.method, seen.path.split('?').next().unwrap());
    assert_eq!(vec![route], served("rest"));
}
