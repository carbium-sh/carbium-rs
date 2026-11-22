//! The REST form of the quote, GET /cq1/quote on the services endpoint. It is admitted as the quote method (same
//! scope, plan and rate), so it follows the quote's retry rules. The answer is one JSON object of strings.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{ErrorKind, Result, decode};
use crate::http::url;
use crate::transport::{Carbium, Wire, error_of, rpc_error};

/// One venue hop of a REST quote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cq1SwapInfo {
    pub amm_key: String,
    pub label: String,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    pub fee_amount: String,
    pub fee_mint: String,
}

/// One leg of a REST quote's route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cq1RouteLeg {
    pub swap_info: Cq1SwapInfo,
    pub percent: String,
    pub bps: String,
}

/// The REST quote. Every value is a string, amounts in base units. Fields the route adds later land in `extra`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cq1Quote {
    pub src_amount_in: String,
    pub dest_amount_out: String,
    pub dest_amount_out_min: String,
    pub swap_mode: String,
    pub slippage: String,
    pub price_impact_pct: String,
    pub route_plan: Vec<Cq1RouteLeg>,
    pub fee_mint: String,
    pub fee_bps: String,
    pub signature_fee_lamports: String,
    pub prioritization_fee_lamports: String,
    pub rent_fee_lamports: String,
    pub input_mint: String,
    pub output_mint: String,
    pub swap_type: String,
    pub router: String,
    pub request_id: String,
    pub price_impact: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The REST routes the client offers, as served.json names them.
pub const REST_ROUTES: &[&str] = &["GET /cq1/quote"];

const LABEL: &str = "GET /cq1/quote";

impl Carbium {
    /// `GET /cq1/quote`: a quote for `amount_in` base units of `src_mint` into `dst_mint`. A parameter fault is a
    /// Request error with the service's text; a key or plan refusal maps as for `quote`.
    pub async fn cq1_quote(
        &self,
        src_mint: &str,
        dst_mint: &str,
        amount_in: u64,
        slippage_bps: u64,
    ) -> Result<Cq1Quote> {
        let pairs = [
            ("src_mint", Some(src_mint.to_string())),
            ("dst_mint", Some(dst_mint.to_string())),
            ("amount_in", Some(amount_in.to_string())),
            ("slippage_bps", Some(slippage_bps.to_string())),
        ];
        let target = url(self.endpoint().trim_end_matches('/'), "/cq1/quote", &pairs)?;
        let a = self
            .send(Wire::Get(target), LABEL, None, false)
            .await
            .map_err(|e| {
                if e.http_status() == 422 {
                    e.with_kind(ErrorKind::Route)
                } else {
                    e
                }
            })?; // 422: no route for the pair
        let body = a.body.unwrap_or(Value::Null);
        if let Some(err) = error_of(&body) {
            return Err(rpc_error(LABEL, a.status, err, &a.headers, None).0);
        }
        decode(body, ErrorKind::Upstream, LABEL, "the answer").map_err(|e| e.with_status(a.status))
    }
}
