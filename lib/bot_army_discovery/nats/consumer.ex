defmodule BotArmyDiscovery.NATS.Consumer do
  @moduledoc """
  NATS message consumer for discovery.

  Subscribes to NATS subjects and routes messages to handlers.
  Uses standardized Reply format for request/reply patterns.

  All request/reply handlers should return responses using Reply helpers:
  - BotArmyLibraryRuntime.NATS.Reply.ok(data) for success
  - BotArmyLibraryRuntime.NATS.Reply.error(message, code) for errors
  """

  use GenServer
  require Logger

  @reconnect_delay_ms 5000
  @version Mix.Project.config()[:version]

  # Register subjects with their metadata for runtime discovery
  @subjects [
    %{
      subject: "discovery.audit.run",
      type: :request_reply,
      description: "Trigger an on-demand fleet audit"
    },
    %{
      subject: "discovery.status",
      type: :request_reply,
      description: "Get the status of the last audit run"
    }
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Logger.info("Starting NATS consumer")

    state = %{
      subscriptions: [],
      conn: nil,
      opts: opts
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        do_connect(conn, state)

      {:error, _reason} ->
        Logger.warning("NATS connection not ready, will retry")
        Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
        {:noreply, state}
    end
  end

  defp do_connect(conn, state) do
    BotArmyLibraryRuntime.NATS.Connection.subscribe_to_status()
    Logger.info("Connected to NATS, subscribing to topics")

    subscriptions =
      [
        "discovery.audit.run",
        "discovery.status"
      ]
      |> Enum.map(fn subject ->
        case Gnat.sub(conn, self(), subject) do
          {:ok, sub} ->
            Logger.info("Subscribed to #{subject}")
            sub

          {:error, reason} ->
            Logger.error("Failed to subscribe to #{subject}: #{inspect(reason)}")
            nil
        end
      end)
      |> Enum.filter(&(not is_nil(&1)))

    # Register subjects for runtime discovery
    BotArmyLibraryRuntime.Registry.register("discovery", @subjects, @version)

    {:noreply, %{state | subscriptions: subscriptions, conn: conn}}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    BotArmyLibraryRuntime.Tracing.with_consumer_span(msg.topic, Map.get(msg, :headers), fn ->
      process_msg(msg)
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("Disconnected from NATS, will reconnect")
    Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
    {:noreply, %{state | subscriptions: [], conn: nil}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("Reconnected to NATS, re-subscribing")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:connect_retry, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    {:noreply, state, {:continue, :connect}}
  end

  # Message routing
  defp process_msg(msg) do
    Logger.debug("Received NATS message on subject: #{msg.topic}")
    Logger.info("Processing message on #{msg.topic}...")

    if msg.reply_to do
      handle_request(msg)
    else
      handle_pubsub(msg)
    end

    Logger.info("Finished processing message on #{msg.topic}")
  end

  defp handle_request(msg) do
    case msg.topic do
      "discovery.audit.run" ->
        GenServer.cast(BotArmyDiscovery.Scheduler, {:trigger_audit, msg.reply_to})

      "discovery.status" ->
        GenServer.cast(BotArmyDiscovery.Scheduler, {:get_status, msg.reply_to})

      _ ->
        Logger.debug("Unknown request/reply subject: #{msg.topic}")
    end
  end

  defp handle_pubsub(msg) do
    case BotArmyLibraryCore.NATS.Decoder.decode(msg.body) do
      {:ok, decoded_message} ->
        route_message(decoded_message, msg.topic)

      {:error, reason} ->
        Logger.warning("Failed to decode message from #{msg.topic}: #{inspect(reason)}")
    end
  end

  defp route_message(_message, topic) do
    # Route decoded messages to appropriate handlers
    Logger.debug("Routing message from #{topic}")
  end

  # Request/reply handlers
  # defp handle_task_list(msg, state) do
  #   response =
  #     case get_tasks() do
  #       {:ok, tasks} ->
  #         BotArmyLibraryRuntime.NATS.Reply.ok(%{"tasks" => tasks})
  #
  #       {:error, reason} ->
  #         BotArmyLibraryRuntime.NATS.Reply.error(inspect(reason), :list_failed)
  #     end
  #
  #   if state.conn do
  #     Gnat.pub(state.conn, msg.reply_to, response)
  #   end
  # end
end
