// shared scenarios: written once against `gate::Gate`, run on every provider via `for_all_gates!`
mod ping;

// transport-specific behavior
mod api_only;
mod ws_only;
