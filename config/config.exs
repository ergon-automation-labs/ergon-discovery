import Config

# 2026-09-05: releases previously ran at Logger's default (:debug) because the
# app had no config dir at all — the debug flood ([NATS] Published message,
# per-file walk errors) produced a ~79MB log per 2h on air. Prod logs at info.
if config_env() == :prod do
  config :logger, level: :info
end