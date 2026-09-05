defmodule BotArmyDiscovery.AuditTest do
  use ExUnit.Case
  require Logger

  setup do
    # Create a temporary root directory for the audit
    root = Path.join(System.tmp_dir!(), "discovery_audit_test_#{:erlang.unique_integer()}")
    File.mkdir_p!(Path.join(root, "graphify-out"))
    File.mkdir_p!(Path.join(root, "config"))
    File.mkdir_p!(Path.join(root, "surfaces"))

    # Setup mock data
    # 1. graph.json - contains some nodes, but missing one subject
    graph = %{
      "nodes" => [
        %{"norm_label" => "get_tasks", "source_file" => "bot_army_gtd/lib/handlers.ex"},
        # Redundant
        %{"norm_label" => "get_tasks", "source_file" => "bot_army_llm/lib/handlers.ex"}
      ]
    }

    File.write!(Path.join(root, "graphify-out/graph.json"), Jason.encode!(graph))

    # 2. services.toml - contains an orphaned bot
    services = %{
      "services" => %{
        "bot_army_gtd" => %{"name" => "GTD Bot", "type" => "bot"},
        "bot_army_orphaned" => %{"name" => "Orphaned Bot", "type" => "bot"}
      }
    }

    # Simple TOML string
    File.write!(
      Path.join(root, "config/services.toml"),
      "[services]\n[services.bot_army_gtd]\nname = \"GTD Bot\"\ntype = \"bot\"\n[services.bot_army_orphaned]\nname = \"Orphaned Bot\"\ntype = \"bot\""
    )

    # 3. ops_catalog.toml - contains a silent scream
    # Note: "bridge.test.query" is NOT in graph or code
    catalog = %{
      "operations" => [
        %{"id" => "test_op", "nats_subjects" => ["bridge.test.query"]}
      ]
    }

    File.write!(
      Path.join(root, "config/ops_catalog.toml"),
      "[[operations]]\nid = \"test_op\"\nnats_subjects = [\"bridge.test.query\"]"
    )

    # Create a surface for GTD bot
    File.mkdir_p!(Path.join(root, "surfaces/gtd-tui"))

    # Mock a file for the code search check
    # We'll put "bridge.test.query" in some file to verify it DOESN'T scream if found
    # But for the first test, we want it to scream.

    Application.put_env(:bot_army_discovery, :root_dir, root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    %{root: root}
  end

  test "detects silent screams, orphaned bots, and redundant logic", %{root: _root} do
    {:ok, insights} = BotArmyDiscovery.Audit.run_audit()

    # Check Silent Scream
    assert Enum.any?(insights, fn i ->
             i.pattern == "Silent Scream" and String.contains?(i.message, "bridge.test.query")
           end)

    # Check Orphaned Bot
    assert Enum.any?(insights, fn i ->
             i.pattern == "Orphaned Capability" and String.contains?(i.message, "Orphaned Bot")
           end)

    # Check Redundancy
    assert Enum.any?(insights, fn i ->
             i.pattern == "Redundant Logic" and String.contains?(i.message, "get_tasks")
           end)
  end

  test "does not scream if subject is found in code", %{root: root} do
    # Add the subject to a file
    File.write!(
      Path.join(root, "some_file.ex"),
      "def handle_msg, do: NATS.pub(\"bridge.test.query\", %{})"
    )

    {:ok, insights} = BotArmyDiscovery.Audit.run_audit()

    refute Enum.any?(insights, fn i ->
             i.pattern == "Silent Scream" and String.contains?(i.message, "bridge.test.query")
           end)
  end
end
