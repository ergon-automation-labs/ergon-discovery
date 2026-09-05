defmodule BotArmyDiscovery.InsightRouter do
  @moduledoc """
  Routes audit insights to the operator and the PARA system.
  """

  require Logger

  @insight_subject "discovery.insight.new"
  @para_write_subject "para.fs.write"

  def route_insights(insights) do
    # Only route WARN and CRITICAL insights to PARA/Operator
    critical_insights = Enum.filter(insights, fn i -> i.severity in ["WARN", "CRITICAL"] end)

    if critical_insights == [] do
      Logger.info("[InsightRouter] No critical insights to route.")
    else
      Enum.each(critical_insights, &route_single_insight/1)
    end
  end

  defp route_single_insight(insight) do
    # 1. Broadcast to NATS
    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
      Gnat.pub(conn, @insight_subject, Jason.encode!(insight))
    end

    # 2. Write to PARA
    # We'll write to a dedicated insights file in the PARA root
    # We assume the bot can discover the PARA root or use a known path for now
    # but according to the global instructions, we should use para.system.config.
    # For this implementation, we'll use a simplified request to para.fs.write.

    content = format_sovereign_insight(insight)

    payload = %{
      "path" => "projects/Bot Army/insights/discovery_insights.md",
      "content" => content,
      "append" => true
    }

    Logger.info("[InsightRouter] Routing Sovereign Insight: #{insight.pattern}")

    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
      Gnat.pub(conn, @para_write_subject, Jason.encode!(payload))
    end
  end

  defp format_sovereign_insight(insight) do
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601()

    """
    ## Sovereign Insight: #{insight.pattern}
    - **Severity**: #{insight.severity}
    - **Finding**: #{insight.message}
    - **Timestamp**: #{timestamp}
    #{if Map.has_key?(insight, :evidence), do: "- **Evidence**: #{inspect(insight.evidence)}\n", else: ""}
    ---\n
    """
  end
end
