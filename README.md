# carbium

Typed async Rust client for Carbium's Solana services on [services.carbium.sh](https://carbium.sh): JSON-RPC (every
served method, DAS included), the quote plane (quote, swap, prices, health), the account API, webhook signature checks,
the DCA API on the dex, the token API, WebSocket subscriptions and Carbon Stream (Yellowstone gRPC). Built on tokio and reqwest, with addresses and signatures as strings, so the
default features pull in no Solana crate.

The crate never holds a private key, signs or sends on its own: `swap` returns one unsigned transaction for your own
wallet to sign, and `send_transaction` takes bytes you signed. Every method the crate offers is served; what the
services do not serve is not here.

```sh
cargo add carbium
cargo add tokio --features macros,rt-multi-thread
```
Carbon Stream, WebSocket subscriptions and webhook checks are features: `cargo add carbium --features stream,ws,webhooks`.
To update: `cargo update -p carbium`; each version's notes are at https://github.com/carbium-sh/carbium-rs/releases.

## Quick start

A service key (`c_serv_…`) comes from the dashboard at carbium.sh. Without one the call reaches the door and is refused
with 401, which the quick start prints.

```rust,no_run
use carbium::{Carbium, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), carbium::Error> {
    let client = Carbium::new(ClientOptions {
        api_key: std::env::var("CARBIUM_KEY").ok(),
        ..Default::default()
    })?;
    match client.get_latest_blockhash(None).await {
        Ok(latest) => println!(
            "blockhash {} valid until block height {} at slot {}",
            latest.value.blockhash, latest.value.last_valid_block_height, latest.context.slot
        ),
        Err(e) => {
            let code = e.code().map(ToString::to_string).unwrap_or_default();
            println!("{} {} {} {} {}", e.kind(), e.http_status(), code, e.reason().unwrap_or(""), e.message());
        }
    }
    Ok(())
}
```

## RPC, DAS and the quote plane

```rust,no_run
# async fn run(client: carbium::Carbium, a: String, b: String, mint: String, owner: String) -> carbium::Result<()> {
use carbium::{AccountInfoConfig, Encoding, GetAssetParams, GetAssetsByOwnerParams, JsonRpcCall, Value};
use serde_json::json; // serde_json = "1" in your Cargo.toml

// Standard Solana methods and the fee extras take positional params, as on the wire.
let balance = client.get_balance("So11111111111111111111111111111111111111112", None).await?;
// Params and config structs are built with new(), taking only the required fields; set the optional ones after.
let mut config = AccountInfoConfig::new();
config.encoding = Some(Encoding::Base64);
let accounts = client.get_multiple_accounts(&[a, b], Some(&config)).await?.value;

// DAS methods take one params struct in Helius field names.
let asset = client.get_asset(&GetAssetParams::new(mint)).await?;
let mut by_owner = GetAssetsByOwnerParams::new(owner);
by_owner.limit = Some(1000);
let page = client.get_assets_by_owner(&by_owner).await?;

// Any served method by name, read as any serde type.
let slot: u64 = client.call("getSlot", Some(json!([{ "commitment": "confirmed" }]))).await?;

// A JSON-RPC batch (1 to 100 calls, at most two permit methods, DAS only with DAS; scans, getSupply,
// getLargestAccounts, the fee extras, getTransactionsForAddress and the quote plane never batch).
let answers = client.batch(&[JsonRpcCall::new("getSlot", None), JsonRpcCall::new("getHealth", None)]).await?;
# Ok(())
# }
```

A result number is a `serde_json::Number`: an integer stays exact (read it with `as_u64`), a fraction stays a
fraction. Token amounts and quote-plane amounts are decimal strings. Token scans (`getTokenAccountsByOwner`,
`getTokenAccountsByDelegate`), `getProgramAccounts` and `getTransactionsForAddress` walks are billed by what they read:
name a mint, keep `limit` small, and read the method notes at https://carbium.sh/skill.md before running one in a loop.

### Swap: quote, build, sign with your own wallet, send

The quote plane needs the account's quote module (free plan) and a key with the quote scope.

```rust,no_run
# async fn run(client: carbium::Carbium, sol: String, usdc: String, wallet: String) -> carbium::Result<()> {
use carbium::{QuoteParams, QuoteParamsAmountIn, SwapParams};

let quote = client.quote(&QuoteParams::new(sol, usdc, QuoteParamsAmountIn::Text("100000000".into()), 50)).await?;
let swap = client.swap(&SwapParams::new(quote.quote_id, wallet)).await?;
// swap.transaction is one unsigned v0 transaction, base64; your wallet is its only signer.
# let signed_base64 = swap.transaction;
let mut send = carbium::SendTransactionConfig::new();
send.encoding = Some(carbium::BinaryEncoding::Base64);
send.skip_preflight = Some(true);
let signature = client.send_transaction(&signed_base64, Some(&send)).await?;
# Ok(())
# }
```

A quote also takes a cap on the accounts the swap instruction may carry and your own integrator fee:

```rust,no_run
# async fn run(client: carbium::Carbium, sol: String, usdc: String, treasury: String) -> carbium::Result<()> {
use carbium::{QuoteParams, QuoteParamsAmountIn, QuoteParamsIntegratorFeeBps};

let mut ask = QuoteParams::new(sol, usdc, QuoteParamsAmountIn::Text("100000000".into()), 50);
ask.max_accounts = Some(32);
ask.integrator_fee_bps = Some(QuoteParamsIntegratorFeeBps::Number(25));
ask.integrator_fee_account = Some(treasury);
let capped = client.quote(&ask).await?;
// capped.integrator_fee is { amount, fee_bps, mint, account }; dest_amount_out and min_amount_out are what the
// trader receives after it.
# Ok(())
# }
```

The fee is taken in the token `integrator_fee.mint` names. A token fee goes to that wallet's associated token account,
which must already exist; a fee that cannot be paid is skipped and the swap still lands. A quote with no route under
`max_accounts` answers `-32055` (`ErrorKind::Route`): change the ask, a retry gives the same answer.

`GET /cq1/quote` is the old quote path, kept for callers already on it (`client.cq1_quote`, deprecated); new code uses
`quote`.

Decode `swap.transaction` from base64, sign it with your own wallet (for example as a `VersionedTransaction` from
solana-sdk), encode it as base64 again and pass it to `send_transaction` with `encoding: base64` and
`skipPreflight: true`, as above. Then poll
`get_signature_statuses` and resend the same bytes until the block height passes `swap.last_valid_block_height`. The
crate never retries `send_transaction`. Quote again (never re-sign the old message) when the blockhash expires or when
`swap` answers a route error that asks for a new quote (`-32095`, `-32074`, `-32081` to `-32083`, `-32096`, `-32093`).

## Errors and retries

Every failure is a `carbium::Error`:

| Method | Meaning |
|---|---|
| `kind()` | `Auth`, `Credits`, `RateLimit`, `Request`, `Upstream`, `Node`, `Route`, `Transport` (and `Socket`, `Stream`, `Account`, `Dca`, `Tokens`, `Webhook` for the other clients) |
| `http_status()` | the HTTP status, 0 when nothing answered |
| `code()` | the JSON-RPC code, or an HTTP API's error string |
| `reason()` | the door's `error.data.reason` |
| `is_retryable()` | whether waiting and sending the same request can change the answer |
| `retry_after_ms()` | the server's wait hint |
| `request_id()` | `x-request-id`, to quote to support |
| `data()` | `error.data` as served |

The retry policy is the one published in skill.md and applies to every HTTP call on services.carbium.sh: wait the larger of the server's hint
(`retry-after-ms`, else `retryAfterMs` in the error's data, else `Retry-After`) and 250 ms doubling to 4 s, plus up to 250 ms of jitter; a hint is never cut, and a hint over `max_hint_ms` (30 s by default) is not waited for: the error comes back at once with it in `retry_after_ms()`;
at most 4 attempts unless the published row says otherwise (a scan index behind: about 6 from 100 ms).
`sendTransaction`, 401, 402 (credits exhausted), 403, bad params (apart from `getTransactionsForAddress`'s `-32602` "the walk read no history", which is retried), unknown methods and every answer marked
`retryable: false` are never retried. `ClientOptions { retry: None, .. }` turns retries off, and
`RetryOptions { max_attempts, .. }` lowers the attempt count. Keys never appear in error messages, data, Debug output
or URLs: an endpoint that carries a key is refused.

## WebSocket subscriptions

Turn on the `ws` feature. A tokio runtime must be running.

```rust,no_run
# #[cfg(feature = "ws")]
# async fn run(client: carbium::Carbium, pubkey: String) -> carbium::Result<()> {
use carbium::ws::{Carrier, SocketEvent};
use serde_json::json; // serde_json = "1" in your Cargo.toml

let (socket, mut events) = client.socket(Carrier::default())?;
let mut sub = socket.account_subscribe(&pubkey, Some(json!({ "commitment": "confirmed" })))?;
sub.ready().await?;
while let Some(notification) = sub.next().await {
    let n = notification?;
    println!("{} {}", n.context.slot, n.value.lamports);
}
// events yields SocketEvent::Reseed { gap, .. } after every reconnect: re-read what you may have missed.
# Ok(())
# }
```

All eight served subscriptions have a method (`account_subscribe`, `program_subscribe`, `logs_subscribe`,
`signature_subscribe`, `slot_subscribe`, `root_subscribe`, `slots_updates_subscribe`, `block_subscribe`). The client
follows the server's close codes: it reconnects and binds every live subscription again after 1013, 1012, 1001 and
network drops, reconnects once after 1008 `credits_exhausted`, retries a 429 handshake after its Retry-After, and stops
with `SocketEvent::Fatal` on a key refusal. Nothing is replayed by the server after a disconnect: `SocketEvent::Reseed`
comes after every reconnect, with `gap: true` after `stream_gap_reseed_required`. `signature_subscribe` fires once;
after a reconnect, check `get_signature_statuses`. Dropping a `Subscription` ends it, and ending the last one closes
the socket. `unsubscribe` returns before its frame is necessarily written, so a program that exits at once may not
have told the server; closing the socket ends its subscriptions either way.
The key travels in the `x-api-key` handshake header by default (`Carrier::Header`); `Carrier::Subprotocol` sends it in
`Sec-WebSocket-Protocol` instead, the browsers' form. It never goes in the URL.

## Carbon Stream (Yellowstone gRPC)

Turn on the `stream` feature. The key needs the grpc scope and a plan with streams (DEV and above).

```rust,no_run
# #[cfg(feature = "stream")]
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
use carbium::stream::{CarbiumStream, ReconnectOptions, StreamEvent, StreamFilters, StreamOptions};
use carbium::stream::proto::SubscribeRequestFilterSlots;

let stream = CarbiumStream::connect(StreamOptions::new(std::env::var("CARBIUM_KEY")?)).await?;
println!("slot {}", stream.get_slot(None).await?.slot);
let mut filters = StreamFilters::default();
filters.slots.insert("s".into(), SubscribeRequestFilterSlots::default());
let mut live = stream.subscribe_with_reconnect(&filters, ReconnectOptions::default())?;
while let Some(event) = live.next().await {
    match event {
        StreamEvent::Update(update) => println!("{:?}", update.update_oneof),
        StreamEvent::Reseed { .. } => { /* re-read over RPC what you may have missed */ }
        StreamEvent::Stopped(error) => break, // a key, plan or request refusal: fix it, do not retry
        _ => {}
    }
}
# Ok(())
# }
```

The key travels as x-token metadata, never in the URL. `subscribe_with_reconnect` sends the whole filter set in the
first message, pings every 10 s with never more than one pong outstanding, and follows the published rules: it stops on
UNAUTHENTICATED, PERMISSION_DENIED, INVALID_ARGUMENT, UNIMPLEMENTED and on RESOURCE_EXHAUSTED for exhausted credits;
it re-seeds on OUT_OF_RANGE and goes on live; any other end is retried after the larger of the table wait (2 s, 1 s
while the service is still loading, or the server's retry-after) and a backoff from 2 s doubling to 30 s, plus up to
1 s of jitter, reset after any data. At processed commitment it resumes from the last slot seen when the retained floor
(`SubscribeReplayInfo`) still holds it, and asks you to re-seed when it does not; a resume can replay one slot you
already saw, so dedupe on slot and signature. The seven filter kinds the service serves (accounts, slots, transactions,
transactions_status, blocks, blocks_meta, entry) are the only ones offered, and shapes it refuses are refused before
any call: cuckoo filters, `token_accounts`, `include_update_parent`, a bare transaction filter with nothing named, and
`from_slot` above processed commitment. The unary calls are `get_slot`, `get_block_height`, `get_latest_blockhash`,
`is_blockhash_valid`, `get_version`, `ping` and `subscribe_replay_info`; calls on one client run side by side.
`subscribe` opens one plain stream with no reconnect, which you can `ping`. Updates wait in a small queue (32 frames): a
slow reader slows the stream, and the service cuts a reader that takes nothing for 10 s. `close()` ends the events with
`Stopped(None)`.

What the feature pulls in: the stock `yellowstone-grpc-client` 17.0.0 and `yellowstone-grpc-proto` 14.0.1, and through
them tonic 0.14 (with native TLS roots), prost and `solana-pubkey`; the message types are re-exported as
`carbium::stream::proto`. To build it needs nothing installed: the proto crate compiles its `.proto` files with a
vendored `protoc` (`protoc-bin-vendored`) at build time.

## Webhooks

Turn on the `webhooks` feature: `cargo add carbium --features webhooks`.

```rust,no_run
# #[cfg(feature = "webhooks")]
# fn run(raw_body: Vec<u8>, headers: std::collections::HashMap<String, String>) -> Result<(), Box<dyn std::error::Error>> {
use carbium::webhooks::VerifyOptions;

// in your HTTPS handler, with the RAW body bytes (never a re-encoded JSON string)
let secret = std::env::var("CARBIUM_WEBHOOK_SECRET")?;
let events = carbium::verify_webhook(&raw_body, &headers, &secret, &VerifyOptions::default())?;
# Ok(())
# }
```

Standard Webhooks: HMAC-SHA256 over `webhook-id.webhook-timestamp.body` with the secret's base64 after `whsec_`,
`v1,` entries in `webhook-signature` (two during a rotation), 300 s of clock tolerance, compared in constant time.
Header names match in any case; `headers` can be a `reqwest::header::HeaderMap`, a `HashMap`, a `BTreeMap`, or a list
of name and value pairs. A timestamp or clock that cannot be compared is refused as stale. A failure is an error of
kind `Webhook` with reason `missing_header`, `bad_timestamp`, `stale`, `bad_secret`, `no_match` or `bad_json`. Dedupe on
`webhook-id`: it stays the same across retries.

## Account API

An account API key (`c_acct_…`, made on the dashboard) runs the account by API: service keys, webhooks, usage and
records. Calls cost no credits; 600 per 10 minutes per key. Nothing here is retried: a 429 comes back with
`retry_after_ms`.

```rust,no_run
# async fn run(wallet: String) -> Result<(), Box<dyn std::error::Error>> {
use carbium::CarbiumAccount;
use carbium::account::{AccountOptions, CreateWebhookRequest};

let account = CarbiumAccount::new(AccountOptions::new(std::env::var("CARBIUM_ACCOUNT_KEY")?))?;
let keys = account.keys().list().await?;
let request = CreateWebhookRequest { key_id: keys[0].key_id, url: "https://hooks.example.com/carbium".into(), addresses: vec![wallet], ..Default::default() };
let created = account.webhooks().create(&request).await?; // created.secret is shown once: store it
let result = account.webhooks().test(created.webhook.webhook_id).await?; // read result.outcome
# Ok(())
# }
```

Groups: `keys()` (list, create, rotate, revoke, delete, rename, set_scope, set_caps, set_allowlist, set_limits),
`webhooks()` (list, get, deliveries, create, update, delete, rotate_secret, test), `usage()` (daily, per_key, load,
hourly, latency), `account()` (get, balance, invoices, payments, modules, crypto_intent), `modules().enable_free`,
`self_record` and `activity`. A raw key or webhook secret in an answer is a `carbium::Secret`: its Debug shows only the
prefix, and `expose()` gives the value. Payments, plan changes and making account keys stay on the dashboard.

## DCA API

```rust,no_run
# async fn run(owner: String, usdc: String, sol: String) -> carbium::Result<()> {
use carbium::CarbiumDca;
use carbium::dca::{DcaAmount, DcaFrequency, DcaInterval, DcaOpenRequest, DcaOptions, DcaQuoteRequest};

let dca = CarbiumDca::new(DcaOptions::default())?;
let quote = dca.quote(&DcaQuoteRequest { input_mint: usdc.clone(), output_mint: sol.clone(), amount: 1_000_000, ..Default::default() }).await?;
let build = dca
    .open(&DcaOpenRequest {
        owner,
        input_mint: usdc,
        output_mint: sol,
        cycles: 10,
        amount: DcaAmount::PerCycle(1_000_000),
        interval: DcaInterval::Frequency(DcaFrequency::Daily),
        max_slippage_bps: None,
        start_at: None,
        index: None,
        min_out_e6: None,
        max_price: None,
    })
    .await?;
// build.build.txn_base64 is unsigned: sign it with the owner's wallet, then send it yourself or relay it with dca.send
# Ok(())
# }
```

Reads: `quote`, `positions`, `history`, `position`, `health`. Builders: `open`, `update`, `withdraw`, `close`. `send`
relays bytes the owner's wallet signed, once. Amounts travel as decimal strings. Server-side only: a browser page is
refused with `forbidden_origin`. Errors are kind `Dca` with the API's `err` string as `code`.

## Token API

```rust,no_run
# async fn run(mints: Vec<String>, mint: String) -> carbium::Result<()> {
use carbium::CarbiumTokens;
use carbium::tokens::{TokenSearchOptions, TokensOptions};

let tokens = CarbiumTokens::new(TokensOptions::default())?;
let sol = tokens.lookup("So11111111111111111111111111111111111111112").await?; // None when unknown
let top = tokens.search("", &TokenSearchOptions { limit: Some(20), ..Default::default() }).await?;
let batch = tokens.batch(&mints).await?; // up to 500; batch.unknown is queued for enrichment
let src = tokens.image_url(&mint);
# Ok(())
# }
```

Keyless, 300 requests a minute per client. These four are the token API's public routes; a lookup of a mint not indexed
yet is `None`. A token's `symbol` and `name` are `''` until the mint is resolved.

## Features and what the default build pulls in

The default build is the JSON-RPC client, the REST quote and the account, DCA and token clients on reqwest (rustls with
the platform's certificate roots, HTTP/2, the system proxy), serde, serde_json, serde_path_to_error, thiserror and
tokio's timer. `webhooks` adds hmac, sha2 and base64; `ws` adds tokio-tungstenite and futures-util; `stream` adds the Yellowstone gRPC client (see Carbon Stream). `native-tls` uses
the system's TLS library instead of rustls: `default-features = false, features = ["native-tls"]`.

## How answers are read

Results are typed from the published spec, and reading them is tolerant where that costs nothing. An extra field never
fails a call: open objects keep it in `extra`. An enum value this version does not know is kept as `Other(String)`.
A field of the wrong type fails the call with an `Upstream` error whose message names the field path (for example
``at `value.blockhash` ``) and whose `data()` holds the raw answer; `call::<serde_json::Value>()` reads any answer
untyped. Generated structs and enums are `#[non_exhaustive]` so new fields and values can arrive without breaking a
build: build params and config structs with `new()`, which takes only the required fields, then set the optional ones.

## Limits the crate checks before sending

Body 51,200 bytes; batch 1 to 100 calls with at most two permit methods; DAS only with DAS; scans, `getSupply`,
`getLargestAccounts`, the fee extras, `getTransactionsForAddress` and the quote plane never batch; a method the services
do not serve is refused before it is sent; token `batch` takes 1 to 500 mints.

## Working on the crate

`src/gen.rs` is generated from the spec repo's `openrpc.json`: `python3 scripts/generate.py <openrpc.json>`, then
`cargo fmt`. Once per clone, point git at the hooks folder with `git config core.hooksPath hooks`: the leak check in
`scripts/leak-check.sh` then guards every upload, as it does in CI.

MIT. Issues: https://github.com/carbium-sh/carbium-rs/issues. Docs: https://carbium.sh/docs and https://carbium.sh/skill.md.
