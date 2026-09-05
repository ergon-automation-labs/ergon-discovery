defmodule BotArmyDiscovery.Scheduler do
  @moduledoc """
  Schedules periodic audits and handles on-demand audit requests.
  """

  use GenServer
  require Logger

  # 24 hours
  @audit_interval_ms 86_400_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    schedule_audit()
    {:ok, %{last_run: nil, last_result: nil, active_audit: nil}, {:continue, :run_audit}}
  end

  @impl true
  def handle_continue(:run_audit, state) do
    Logger.info("[Scheduler] Triggering autonomous audit...")
    {:noreply, start_audit(state)}
  end

  @impl true
  def handle_info(:tick, state) do
    {:noreply, state, {:continue, :run_audit}}
  end

  @impl true
  def handle_info({:audit_completed, insights}, state) do
    Logger.info("[Scheduler] Audit completed successfully")
    BotArmyDiscovery.InsightRouter.route_insights(insights)
    {:noreply, %{state | last_run: DateTime.utc_now(), last_result: insights, active_audit: nil}}
  end

  @impl true
  def handle_info({:audit_failed, reason}, state) do
    Logger.error("[Scheduler] Audit failed: #{inspect(reason)}")
    {:noreply, %{state | active_audit: nil}}
  end

  @impl true
  def handle_cast({:trigger_audit, reply_to}, state) do
    if state.active_audit do
      Logger.info("[Scheduler] Audit already in progress, queuing/skipping on-demand request")

      with {:ok, conn} <-
             GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
        Gnat.pub(
          conn,
          reply_to,
          Jason.encode!(%{status: "busy", reason: "Audit already running"})
        )
      end

      {:noreply, state}
    else
      Logger.info("[Scheduler] Triggering on-demand audit...")
      {:noreply, start_audit(state, reply_to)}
    end
  end

  @impl true
  def handle_cast({:get_status, reply_to}, state) do
    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
      Logger.info("[Scheduler] Replying to requester on #{reply_to}")

      Gnat.pub(
        conn,
        reply_to,
        Jason.encode!(%{
          last_run: state.last_run,
          last_result_count: length(state.last_result || []),
          status: "ok",
          active_audit: not is_nil(state.active_audit)
        })
      )
    end

    {:noreply, state}
  end

  defp start_audit(state, reply_to \\ nil) do
    pid =
      Task.start(fn ->
        case BotArmyDiscovery.Audit.run_audit() do
          {:ok, insights} ->
            if reply_to, do: reply_to_requester(reply_to, :audit_completed, insights)
            send(BotArmyDiscovery.Scheduler, {:audit_completed, insights})

          {:error, reason} ->
            if reply_to, do: reply_to_requester(reply_to, :audit_failed, reason)
            send(BotArmyDiscovery.Scheduler, {:audit_failed, reason})
        end
      end)

    %{state | active_audit: pid}
  end

  defp reply_to_requester(reply_to, status, result) do
    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
      payload =
        case status do
          :audit_completed ->
            Jason.encode!(%{
              status: "ok",
              insights: result,
              timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
            })

          :audit_failed ->
            Jason.encode!(%{status: "error", reason: result})
        end

      Gnat.pub(conn, reply_to, payload)
    end
  end

  defp schedule_audit do
    Process.send_after(self(), :tick, @audit_interval_ms)
  end
end
