defmodule BotArmyDiscovery.Audit do
  @moduledoc """
  Implements the architectural discovery lenses for the Bot Army ecosystem.
  Detects architectural drift, orphaned bots, and redundant logic.
  """

  require Logger

  defp root_dir do
    Application.get_env(:bot_army_discovery, :root_dir, "/Users/abby/code/elixir_bots")
  end

  defp graph_path do
    Path.join(root_dir(), "graphify-out/graph.json")
  end

  defp services_path do
    Path.join(root_dir(), "config/services.toml")
  end

  defp ops_catalog_path do
    Path.join(root_dir(), "config/ops_catalog.toml")
  end

  defp surfaces_dir do
    Path.join(root_dir(), "surfaces")
  end

  def run_audit do
    Logger.info("[Discovery] 🚀 Starting Bot Army Fleet Audit...")

    graph = fetch_graph()
    services = load_toml(services_path())
    ops_catalog = load_toml(ops_catalog_path())

    if graph == nil do
      Logger.error(
        "[Discovery] ❌ Error: Graph data unavailable. Run 'make graphify-refresh' first."
      )

      {:error, :missing_graph}
    else
      insights =
        if(ops_catalog, do: detect_silent_screams(graph, ops_catalog), else: []) ++
          if(services, do: detect_orphaned_bots(services), else: []) ++
          detect_redundancy(graph)

      report_insights(insights)
      {:ok, insights}
    end
  end

  defp fetch_graph do
    # Try graphify_cache bot via NATS first
    case request_graph_from_cache() do
      {:ok, graph} ->
        Logger.debug("[Discovery] Successfully fetched graph from graphify_cache")
        graph

      {:error, reason} ->
        Logger.debug(
          "[Discovery] NATS graph fetch failed (#{inspect(reason)}), falling back to local file"
        )

        load_json(graph_path())
    end
  end

  defp request_graph_from_cache do
    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, 5_000) do
      reply_subject = "discovery.graph.reply.#{:crypto.strong_rand_bytes(8) |> Base.encode16()}"
      payload = Jason.encode!(%{repo_path: root_dir()})

      case Gnat.sub(conn, self(), reply_subject) do
        {:ok, _sub} ->
          # We use Gnat.pub instead of Gnat.request to have full control over the reply process
          Gnat.pub(conn, "bot_army.graph.query", payload, reply_to: reply_subject)

          # Wait for the response
          receive do
            {:msg, msg} ->
              if msg.topic == reply_subject do
                case Jason.decode(msg.body) do
                  {:ok, %{"graph" => graph}} -> {:ok, graph}
                  {:ok, %{"error" => err}} -> {:error, "Cache error: #{err}"}
                  {:ok, _} -> {:error, "Unexpected graph response format"}
                  {:error, reason} -> {:error, "JSON decode failed: #{reason}"}
                end
              else
                {:error, "Received message on wrong subject: #{msg.topic}"}
              end

            _ ->
              {:error, "Unexpected message received"}
          end

        {:error, reason} ->
          {:error, "Failed to subscribe to reply subject: #{inspect(reason)}"}
      end
    end
  end

  defp report_insights(insights) do
    if insights == [] do
      Logger.info("[Discovery] ✅ No major architectural patterns detected. Fleet is lean!")
    else
      insights
      |> Enum.group_by(fn i -> i.pattern end)
      |> Enum.each(&log_pattern/1)
    end
  end

  defp log_pattern({pattern, items}) do
    Logger.info("[Discovery] 🔍 Pattern: #{pattern}")
    Enum.each(items, &log_insight/1)
  end

  defp log_insight(item) do
    Logger.info("[Discovery]   - #{item.message}")

    if Map.has_key?(item, :evidence),
      do: Logger.info("[Discovery]     Evidence: #{inspect(item.evidence)}")
  end

  # --- Lenses ---

  defp detect_silent_screams(graph, ops_catalog) do
    Logger.debug("[Discovery] Scanning for Silent Screams...")

    subjects =
      (ops_catalog["operations"] || [])
      |> Enum.flat_map(fn op -> op["nats_subjects"] || [] end)
      |> Enum.uniq()

    Logger.debug("[Discovery] Subjects to check: #{inspect(subjects)}")

    graph_text = Jason.encode!(graph)

    # 1. Fast check: remove subjects already found in the graph
    subjects_missing_from_graph =
      Enum.filter(subjects, fn subject -> not String.contains?(graph_text, subject) end)

    # 2. Optimized check: scan codebase once for all remaining subjects
    found_map = find_subjects_in_code(root_dir(), subjects_missing_from_graph)

    subjects_missing_from_graph
    |> Enum.filter(fn subject -> !Map.has_key?(found_map, subject) end)
    |> Enum.map(fn subject ->
      %{
        pattern: "Silent Scream",
        message: "Subject '#{subject}' is defined in ops_catalog but not found in code or graph.",
        severity: "WARN"
      }
    end)
  end

  defp detect_orphaned_bots(services) do
    bot_services =
      services["services"]
      |> Map.filter(fn {_, v} -> v["type"] == "bot" end)

    surface_folders =
      try do
        File.ls!(surfaces_dir())
      rescue
        _ -> []
      end

    bot_services
    |> Enum.flat_map(fn {bot_id, info} ->
      bot_name_stem = String.replace(bot_id, "_bot", "")

      # Check if any folder in surfaces/ contains the stem
      found =
        Enum.any?(surface_folders, fn folder ->
          String.contains?(folder, bot_name_stem)
        end)

      if found do
        []
      else
        [
          %{
            pattern: "Orphaned Capability",
            message:
              "Bot '#{info["name"]}' (#{bot_id}) has no corresponding surface in surfaces/.",
            severity: "INFO"
          }
        ]
      end
    end)
  end

  defp detect_redundancy(graph) do
    nodes = graph["nodes"] || []

    labels =
      nodes
      |> Enum.filter(fn n -> Map.has_key?(n, "norm_label") end)
      |> Enum.group_by(fn n -> n["norm_label"] end)

    labels
    |> Enum.filter(fn {_, occurrences} -> length(occurrences) > 1 end)
    |> Enum.flat_map(fn {label, occurrences} ->
      files =
        occurrences
        |> Enum.map(fn n -> n["source_file"] end)
        |> Enum.uniq()

      if length(files) > 1 do
        [
          %{
            pattern: "Redundant Logic",
            message: "Symbol '#{label}' implemented in #{length(files)} different files.",
            evidence: files,
            severity: "INFO"
          }
        ]
      else
        []
      end
    end)
  end

  # --- Helpers ---

  defp find_subjects_in_code(dir, subjects) do
    # We want to return a map: %{"subject" => ["file1", "file2"]}
    walk_and_find(dir, subjects, %{})
  end

  defp walk_and_find(dir, subjects, acc) do
    try do
      dir
      |> File.ls!()
      |> Enum.reduce(acc, fn entry, current_acc ->
        path = Path.join(dir, entry)

        if File.dir?(path) do
          if entry in [
               ".git",
               "node_modules",
               ".cache",
               "_build",
               "deps",
               ".claude",
               ".elixir_ls",
               ".hex",
               ".venv",
               "venv",
               "target",
               ".pytest_cache",
               ".mypy_cache"
             ] do
            current_acc
          else
            walk_and_find(path, subjects, current_acc)
          end
        else
          if not String.ends_with?(path, "ops_catalog.toml") and file_matches_extension?(entry) do
            case File.read(path) do
              {:ok, content} ->
                # For this file, check which of the target subjects are present
                matches =
                  Enum.filter(subjects, fn subject -> String.contains?(content, subject) end)

                # Update the accumulator map
                Enum.reduce(matches, current_acc, fn subject, map ->
                  Map.update(map, subject, [path], fn files -> [path | files] end)
                end)

              _ ->
                current_acc
            end
          else
            current_acc
          end
        end
      end)
    rescue
      e ->
        # 2026-09-05: eperm/enoent are routine on a dev tree (sandboxed dirs,
        # racing worktrees) — logging each one produced a ~70 lines/sec debug
        # flood and a 79MB log per 2h. Skip silently; only unusual errors log.
        reason = if is_struct(e, File.Error), do: Map.get(e, :reason, :unknown), else: :unknown

        unless reason in [:eperm, :eacces, :enoent] do
          Logger.debug("[Discovery] Error walking #{dir}: #{inspect(e)}")
        end

        acc
    end
  end

  defp file_matches_extension?(filename) do
    ext = Path.extname(filename)
    ext in [".ex", ".exs", ".toml", ".sh", ".py"]
  end

  defp file_contains_string?(path, string) do
    case File.read(path) do
      {:ok, content} -> String.contains?(content, string)
      _ -> false
    end
  end

  defp load_json(path) do
    case File.read(path) do
      {:ok, content} -> Jason.decode!(content)
      _ -> nil
    end
  end

  defp load_toml(path) do
    case File.read(path) do
      {:ok, content} -> Toml.decode!(content)
      _ -> nil
    end
  end
end
