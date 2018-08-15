defmodule KafkaEx.ConsumerGroup.Manager do
  @moduledoc false

  # actual consumer group management implementation

  use GenServer

  alias KafkaEx.ConsumerGroup
  alias KafkaEx.ConsumerGroup.Heartbeat
  alias KafkaEx.ConsumerGroup.PartitionAssignment
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.JoinGroup.Response, as: JoinGroupResponse
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :supervisor_pid,
      :worker_name,
      :heartbeat_interval,
      :session_timeout,
      :consumer_module,
      :consumer_opts,
      :partition_assignment_callback,
      :group_name,
      :topics,
      :member_id,
      :leader_id,
      :consumer_supervisor_pid,
      :members,
      :generation_id,
      :assignments,
      :heartbeat_timer,
      :worker_opts,
    ]
    @type t :: %__MODULE__{}
  end

  @heartbeat_interval 5_000
  @session_timeout 35_000
  @session_timeout_padding 5_000
  @startup_delay 100

  @type assignments :: [{binary(), integer()}]

  # Client API

  @doc false
  # use `KafkaEx.ConsumerGroup.start_link/4` instead
  @spec start_link(module, binary, [binary], KafkaEx.GenConsumer.options) ::
    GenServer.on_start
  def start_link(consumer_module, group_name, topics, opts \\ []) do
    self() |> IO.inspect(label: "040 #{__MODULE__}.start_link consumer_module:#{inspect consumer_module}, self")

    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    consumer_opts = Keyword.drop(opts, [:gen_server_opts])

    GenServer.start_link(
      __MODULE__,
      {consumer_module, group_name, topics, consumer_opts},
      gen_server_opts
      # name: __MODULE__
    )
    |> IO.inspect(label: "050 #{__MODULE__}.start_link res")
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topics, opts}) do
    self() |> IO.inspect(label: "041 #{__MODULE__}.init")

    heartbeat_interval = Keyword.get(
      opts,
      :heartbeat_interval,
      Application.get_env(:kafka_ex, :heartbeat_interval, @heartbeat_interval)
    )
    session_timeout = Keyword.get(
      opts,
      :session_timeout,
      Application.get_env(:kafka_ex, :session_timeout, @session_timeout)
    )
    partition_assignment_callback = Keyword.get(
      opts,
      :partition_assignment_callback,
      &PartitionAssignment.round_robin/2
    )

    supervisor_pid = Keyword.fetch!(opts, :supervisor_pid)
    consumer_opts = Keyword.drop(
      opts,
      [
        :supervisor_pid,
        :heartbeat_interval,
        :session_timeout,
        :partition_assignment_callback
      ]
    )

    worker_opts = Keyword.take(opts, [:uris])

    state = %State{
      supervisor_pid: supervisor_pid,
      heartbeat_interval: heartbeat_interval,
      session_timeout: session_timeout,
      consumer_module: consumer_module,
      partition_assignment_callback: partition_assignment_callback,
      consumer_opts: consumer_opts,
      group_name: group_name,
      topics: topics,
      worker_opts: worker_opts
    }

    IO.puts "042 #{__MODULE__}.init sending init_start_worker"
    Process.send_after(self(), :init_start_worker, @startup_delay)

    {:ok, state}
  end

  ######################################################################
  # handle_call clauses - mostly for ops queries
  def handle_call(:generation_id, _from, state) do
    {:reply, state.generation_id, state}
  end

  def handle_call(:member_id, _from, state) do
    {:reply, state.member_id, state}
  end

  def handle_call(:leader_id, _from, state) do
    {:reply, state.leader_id, state}
  end

  def handle_call(:am_leader, _from, state) do
    {:reply, state.leader_id && state.member_id == state.leader_id, state}
  end

  def handle_call(:assignments, _from, state) do
    {:reply, state.assignments, state}
  end

  def handle_call(:consumer_supervisor_pid, _from, state) do
    {:reply, state.consumer_supervisor_pid, state}
  end

  def handle_call(:group_name, _from, state) do
    {:reply, state.group_name, state}
  end
  ######################################################################

  # `init_start_worker` - stage 1/4 of connecting, during the `init/1`
  # callback, it fires a message to do this, which means if this fails, it can be re-tried by
  # sending the same message
  def handle_info(
    :init_start_worker, %State{group_name: group_name, worker_opts: worker_opts} = state
  ) do
    Logger.debug(fn -> "Phase 1/4 - broker connection" end)
    IO.ANSI.Docs.print_heading("Phase 1/4")
    self() |> IO.inspect(label: "#{__MODULE__}.handle_info :init_start_worker")

    res = KafkaEx.create_worker(
      # KafkaEx.ConsumerGroup.Manager, # process name instead of pid
      :no_name,
      [consumer_group: group_name] ++ worker_opts
    )

    case res do
      {:ok, worker_name} ->
        worker_name |> IO.inspect(label: "#{__MODULE__}.handle_info :init_start_worker success")
        state = %State{state | worker_name: worker_name}

        Process.flag(:trap_exit, true)

        # Move onto second stage of init, which can fail separately from this
        # connection, see below for more details
        # Process.send_after(self(), :init_join_group, @startup_delay)

        Process.send_after(self(), :init_wait_for_server, @startup_delay)

        {:noreply, state}

      {:error, {:already_started, pid}} ->
        self() |> IO.inspect(label: "#{__MODULE__}.handle_info :init_start_worker error already_started pid:#{inspect(pid)}, self")

        Process.flag(:trap_exit, true)

        # Move onto second stage of init, which can fail seperatly from this
        # connection, see below for more details
        # Process.send_after(self(), :init_join_group, @startup_delay)

        Process.send_after(self(), :init_wait_for_server, @startup_delay)

        {:noreply, state}

      {:error, reason} ->
        self() |> IO.inspect(label: "#{__MODULE__}.handle_info :init_start_worker error reason:#{inspect reason}")

        # We dont need to stop_worker here, as it exists itself
        #   12:54:00.356 [error] CRASH REPORT Process <0.745.0> with 0 neighbours exited with reason:
        # As the call to KafkaEx.create_worker/2 starts a Server* as a child process via Supervisor.start_child
        # but if it fails within the init/1, it returns {:stop, reason} which exits that process for us

        # Re-attempt this stage
        Process.send_after(self(), :init_start_worker, @startup_delay)
        {:noreply, state}
    end
  end

  # phase 2/4
  def handle_info(:init_wait_for_server, %State{worker_name: worker_name} = state) do
    Logger.debug(fn -> "Phase 2/4 - broker connection" end)
    IO.ANSI.Docs.print_heading("Phase 3/4")

    if KafkaEx.ready?(worker_name) do
      Process.send_after(self(), :init_join_group, @startup_delay)
    else
      Process.send_after(self(), :init_wait_for_server, @startup_delay)
    end

     {:noreply, state}
  end

  # `init_join_group` - phase 3/4
  # If `member_id` and `generation_id` aren't set, we haven't yet joined the
  # group. `member_id` and `generation_id` are initialized by
  # `JoinGroupResponse`.
  # `join/1`
  def handle_info(
    :init_join_group, %State{generation_id: nil, member_id: nil} = state
  ) do
    Logger.debug(fn -> "Phase 3/4 - partition assignments" end)
    IO.ANSI.Docs.print_heading("Phase 2/4")
    self() |> IO.inspect(label: "#{__MODULE__}.handle_info :init_join_group")
    case join(state) do
      {:ok, new_state} ->
        new_state |> IO.inspect(label: "#{__MODULE__}.handle_info join new_state")
        # move onto next stage, if `join/1` has set `assignments` then we are done
        Process.send_after(self(), :init_assign_partitions, @startup_delay)
        {:noreply, new_state}

      {:error, reason} ->
        reason |> IO.inspect(label: "#{__MODULE__}.handle_info join error reason")
        # retry join
        Process.send_after(self(), :init_join_group, @startup_delay)
        {:noreply, state}
    end
  end

  def handle_info(:init_join_group, %State{} = state) do
    state |> IO.inspect(label: "#{__MODULE__}.handle_info :init_join_group catch error")
    {:noreply, state}
  end

  # `init_assign_partitions` - phase 3/3
  # If `assignments` is nil, it means we've not done partitions assignment
  # Leader is responsible for assigning partitions to all group members.
  def handle_info(
    :init_assign_partitions, %State{assignments: nil, members: members} = state
  ) when not is_nil(members) do
    Logger.debug(fn -> "Phase 4/4 - partition assignments" end)
    IO.ANSI.Docs.print_heading("Phase 3/3")
    self() |> IO.inspect(label: "#{__MODULE__}.handle_info :init_assign_partitions assignments")

    res = assignable_partitions(state)
    |> IO.inspect(label: "#{__MODULE__}.handle_info :init_assign_partitions assignable_partitions res")

    case res do
      [] ->
        IO.puts "#{__MODULE__}.handle_info :init_assign_partitions sending init_assign_partitions"
        Process.send_after(self(), :init_assign_partitions, @startup_delay)
        {:noreply, state}

      partitions ->
        partitions |> IO.inspect(label: "#{__MODULE__}.handle_info :init_assign_partitions partitions")
        assignments = assign_partitions(state, members, partitions)
        {:ok, new_state} = sync(state, assignments)
        Logger.debug(fn -> "Connection complete" end)
        IO.puts "#{__MODULE__}.handle_info :init_assign_partitions complete"
        IO.ANSI.Docs.print_heading("COMPLETE")
        {:noreply, new_state}
    end
  end

  def handle_info(:init_assign_partitions, %State{assignments: []} = state) do
    state |> IO.inspect(label: "#{__MODULE__}.handle_info :init_assignments none")
    {:noreply, state}
  end

  def handle_info(:init_assign_partitions, %State{} = state) do
    state |> IO.inspect(label: "#{__MODULE__}.handle_info :init_assignments catch error")
    {:noreply, state}
  end

  # If the heartbeat gets an error, we need to rebalance.
  # {:stop, {:shutdown, :rebalance}, state}
  def handle_info({:EXIT, heartbeat_timer, {:shutdown, :rebalance}}, %State{heartbeat_timer: heartbeat_timer} = state) do
    self() |> IO.inspect(label: "#{__MODULE__}.handle_info1 EXIT from heartbeat_timer:#{inspect heartbeat_timer}, self")
    {:ok, state} = rebalance(state)
    {:noreply, state}
  end

  # [{:EXIT, #PID<0.813.0>, {:shutdown, {:error, :timeout}}},
  def handle_info({:EXIT, pid, {:shutdown, {:error, error_code}}}, %State{} = state) do
    self() |> IO.inspect(label: "#{__MODULE__}.handle_info2 EXIT from pid:#{inspect pid}, error_code:#{inspect error_code}, self")
    # {:stop, error_code, state}
    {:stop, :normal, state}
  end

  # When terminating, inform the group coordinator that this member is leaving
  # the group so that the group can rebalance without waiting for a session
  # timeout.
  def terminate(reason, %State{generation_id: nil, member_id: nil}) do
    self() |> IO.inspect(label: "#{__MODULE__}.terminate1, reason:#{inspect reason}, self")
    :ok
  end

  def terminate(reason, %State{} = state) do
    self() |> IO.inspect(label: "#{__MODULE__}.terminate2, reason:#{inspect reason}, self")

    stop_consumer(state)
    {:ok, _state} = leave(state)
    Process.unlink(state.worker_name)
    KafkaEx.stop_worker(state.worker_name)
    :ok
  end

  def stop(_) do
    self() |> IO.inspect(label: "#{__MODULE__}.stop !!!")
  end

  ### Helpers

  # `JoinGroupRequest` is used to set the active members of a group. The
  # response blocks until the broker has decided that it has a full list of
  # group members. This requires that all active members send a
  # `JoinGroupRequest`. For active members, this is triggered by the broker
  # responding to a heartbeat with a `:rebalance_in_progress` error code. If
  # any group members fail to send a `JoinGroupRequest` before the session
  # timeout expires, then those group members are removed from the group and
  # synchronization continues without them.
  #
  # `JoinGroupResponse` tells each member its unique member ID as well as the
  # group's current generation ID. The broker will pick one group member to be
  # the leader, which is reponsible for assigning partitions to all of the
  # group members. Once a `JoinGroupResponse` is received, all group members
  # must send a `SyncGroupRequest` (see sync/2).
  defp join(
    %State{
      worker_name: worker_name,
      session_timeout: session_timeout,
      group_name: group_name,
      topics: topics,
      member_id: member_id
    } = state
  ) do
    self() |> IO.inspect(label: "#{__MODULE__}.join")
    join_request = %JoinGroupRequest{
      group_name: group_name,
      member_id: member_id || "",
      topics: topics,
      session_timeout: session_timeout,
    }

    # @session_timeout 35_000
    # @session_timeout_padding 5_000
    Logger.debug("Joining can take a while")

    join_response = KafkaEx.join_group(
      join_request,
      worker_name: worker_name,
      timeout: session_timeout + @session_timeout_padding
    )

    if join_response.error_code != :no_error do
      # self() |> IO.inspect(label: "#{__MODULE__}.join join_response:#{inspect join_response}")
      # TODO: Dont raise but let timeout retry
      message = "Error joining consumer group #{group_name}: " <> "#{inspect join_response.error_code}"
      Logger.error(message)
      # raise message
      {:error, join_response.error_code}
    else
      Logger.debug(fn -> "Joined consumer group #{group_name}" end)

      new_state = %State{
        state |
        leader_id: join_response.leader_id,
        member_id: join_response.member_id,
        generation_id: join_response.generation_id,
        members: join_response.members
      }

      if JoinGroupResponse.leader?(join_response) do
        # Leader is responsible for assigning partitions to all group members.
        # Process.send_after(self(), :init_assign_partitions, 1000)

        {:ok, new_state}
      else
        # Follower does not assign partitions; must be empty.
        sync(new_state, [])
      end
    end
  end

  # `SyncGroupRequest` is used to distribute partition assignments to all group
  # members. All group members must send this request after receiving a
  # response to a `JoinGroupRequest`. The request blocks until assignments are
  # provided by the leader. The leader sends partition assignments (given by
  # the `assignments` parameter) as part of its `SyncGroupRequest`. For all
  # other members, `assignments` must be empty.
  #
  # `SyncGroupResponse` contains the individual member's partition assignments.
  # Upon receiving a successful `SyncGroupResponse`, a group member is free to
  # start consuming from its assigned partitions, but must send periodic
  # heartbeats to the coordinating broker.
  defp sync(
         %State{
           group_name: group_name,
           member_id: member_id,
           generation_id: generation_id,
           worker_name: worker_name,
           session_timeout: session_timeout
         } = state,
         assignments
       ) do
    sync_request = %SyncGroupRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments
    }

    %SyncGroupResponse{error_code: error_code, assignments: assignments} =
      KafkaEx.sync_group(
        sync_request,
        worker_name: worker_name,
        timeout: session_timeout + @session_timeout_padding
      )

    case error_code do
      :no_error ->
        # On a high-latency connection, the join/sync process takes a long
        # time. Send a heartbeat as soon as possible to avoid hitting the
        # session timeout.
        {:ok, state} = start_heartbeat_timer(state)
        {:ok, state} = stop_consumer(state)
        start_consumer(state, unpack_assignments(assignments))

      :rebalance_in_progress ->
        rebalance(state)
    end
  end

  # `LeaveGroupRequest` is used to voluntarily leave a group. This tells the
  # broker that the member is leaving the group without having to wait for the
  # session timeout to expire. Leaving a group triggers a rebalance for the
  # remaining group members.
  defp leave(
    %State{
      worker_name: worker_name,
      group_name: group_name,
      member_id: member_id
    } = state
  ) do
    self() |> IO.inspect(label: "#{__MODULE__}.leave self")
    stop_heartbeat_timer(state)

    leave_request = %LeaveGroupRequest{
      group_name: group_name,
      member_id: member_id,
    }

    leave_group_response =
      KafkaEx.leave_group(leave_request, worker_name: worker_name)

    case leave_group_response do
      {:error, :timeout} ->
        Logger.warn(fn ->
            "Received error #{inspect leave_group_response}, " <>
            "consumer group manager will exit regardless."
          end)

      {:stop, leave_group_response} ->
        Logger.warn(fn ->
            "Received error #{inspect leave_group_response}, " <>
            "consumer group manager will exit regardless."
          end)

      _ ->
        if leave_group_response.error_code == :no_error do
          Logger.debug(fn -> "Left consumer group #{group_name}" end)
        else
          Logger.warn(fn ->
            "Received error #{inspect leave_group_response.error_code}, " <>
            "consumer group manager will exit regardless."
          end)
        end
    end

    {:ok, state}
  end

  # When instructed that a rebalance is in progress, a group member must rejoin
  # the group with `JoinGroupRequest` (see join/1). To keep the state
  # synchronized during the join/sync phase, each member pauses its consumers
  # and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state) do
    {:ok, state} = stop_heartbeat_timer(state)
    {:ok, state} = stop_consumer(state)
    join(state)
  end

  ### Timer Management

  # Starts a heartbeat process to send heartbeats in the background to keep the
  # consumers active even if it takes a long time to process a batch of
  # messages.
  defp start_heartbeat_timer(%State{} = state) do
    {:ok, timer} = Heartbeat.start_link(state)

    {:ok, %State{state | heartbeat_timer: timer}}
  end

  # Stops the heartbeat process.
  defp stop_heartbeat_timer(%State{heartbeat_timer: nil} = state), do: {:ok, state}
  defp stop_heartbeat_timer(
    %State{heartbeat_timer: heartbeat_timer} = state
  ) do
    if Process.alive?(heartbeat_timer) do
      :gen_server.stop(heartbeat_timer)
    end
    new_state = %State{state | heartbeat_timer: nil}
    {:ok, new_state}
  end

  ### Consumer Management

  # Starts consuming from the member's assigned partitions.
  defp start_consumer(
    %State{
      consumer_module: consumer_module,
      consumer_opts: consumer_opts,
      group_name: group_name,
      supervisor_pid: pid
    } = state,
    assignments
  ) do
    self() |> IO.inspect(label: "#{__MODULE__}.start_consumer")
    {:ok, consumer_supervisor_pid} = ConsumerGroup.start_consumer(
      pid,
      consumer_module,
      group_name,
      assignments,
      consumer_opts
    )

    state = %{
      state |
      assignments: assignments,
      consumer_supervisor_pid: consumer_supervisor_pid
    }
    {:ok, state}
  end

  # Stops consuming from the member's assigned partitions and commits offsets.
  defp stop_consumer(%State{supervisor_pid: pid} = state) do
    :ok = ConsumerGroup.stop_consumer(pid)

    {:ok, state}
  end

  ### Partition Assignment

  # Queries the Kafka brokers for a list of partitions for the topics of
  # interest to this consumer group. This function returns a list of
  # topic/partition tuples that can be passed to a GenConsumer's
  # `assign_partitions` method.
  defp assignable_partitions(
    %State{worker_name: worker_name, topics: topics, group_name: group_name}
  ) do
    metadata = KafkaEx.metadata(worker_name: worker_name)
    self() |> IO.inspect(label: "#{__MODULE__}.assignable_partitions self")

    Enum.flat_map(topics, fn (topic) ->
      self() |> IO.inspect(label: "#{__MODULE__}.assignable_partitions topic:#{inspect topic}, self")
      partitions = MetadataResponse.partitions_for_topic(metadata, topic)

      warn_if_no_partitions(partitions, group_name, topic)

      Enum.map(partitions, fn (partition) ->
        {topic, partition}
      end)
    end)
  end

  defp warn_if_no_partitions([], group_name, topic) do
    Logger.warn(fn ->
      "Consumer group #{group_name} encountered nonexistent topic #{topic}"
    end)
    # send message to retry
    :nonexistent_topic
  end
  defp warn_if_no_partitions(_partitions, _group_name, _topic), do: :ok

  # This function is used by the group leader to determine partition
  # assignments during the join/sync phase. `members` is provided to the leader
  # by the coordinating broker in `JoinGroupResponse`. `partitions` is a list
  # of topic/partition tuples, obtained from `assignable_partitions/1`. The
  # return value is a complete list of member assignments in the format needed
  # by `SyncGroupResponse`.
  defp assign_partitions(
    %State{partition_assignment_callback: partition_assignment_callback},
    members,
    partitions
  ) do
    # Delegate partition assignment to GenConsumer module.
    assignments = partition_assignment_callback.(members, partitions)

    # Convert assignments to format expected by Kafka protocol.
    packed_assignments =
      Enum.map(assignments, fn ({member, topic_partitions}) ->
        {member, pack_assignments(topic_partitions)}
      end)
    assignments_map = Enum.into(packed_assignments, %{})

    # Fill in empty assignments for missing member IDs.
    Enum.map(members, fn (member) ->
      {member, Map.get(assignments_map, member, [])}
    end)
  end

  # Converts assignments from Kafka's protocol format to topic/partition tuples.
  #
  # Example:
  #
  #   unpack_assignments([{"foo", [0, 1]}]) #=> [{"foo", 0}, {"foo", 1}]
  defp unpack_assignments(assignments) do
    Enum.flat_map(assignments, fn ({topic, partition_ids}) ->
      Enum.map(partition_ids, &({topic, &1}))
    end)
  end

  # Converts assignments from topic/partition tuples to Kafka's protocol format.
  #
  # Example:
  #
  #   pack_assignments([{"foo", 0}, {"foo", 1}]) #=> [{"foo", [0, 1]}]
  defp pack_assignments(assignments) do
    assignments
    |> Enum.reduce(%{}, fn({topic, partition}, assignments) ->
      Map.update(assignments, topic, [partition], &(&1 ++ [partition]))
    end)
    |> Map.to_list
  end
end
