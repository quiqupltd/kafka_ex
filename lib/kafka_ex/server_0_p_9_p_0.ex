defmodule KafkaEx.Server0P9P0 do
  @moduledoc """
  Implements kafkaEx.Server behaviors for kafka 0.9.0 API.
  """
  use KafkaEx.Server
  alias KafkaEx.ConsumerGroupRequiredError
  alias KafkaEx.InvalidConsumerGroupError
  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Heartbeat
  alias KafkaEx.Protocol.JoinGroup
  alias KafkaEx.Protocol.LeaveGroup
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.SyncGroup
  alias KafkaEx.Server.State
  alias KafkaEx.NetworkClient
  alias KafkaEx.Server0P8P2

  require Logger

  @consumer_group_update_interval 30_000

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    self() |> IO.inspect(label: "#{__MODULE__}.start_link :no_name")
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    # Process.info(self(), :current_stacktrace)
    # |> IO.inspect(label: "CALLSTACK")
    self() |> IO.inspect(label: "#{__MODULE__}.start_link name:#{inspect name}, self")
    GenServer.start_link(__MODULE__, [args, name], [name: name])
  end

  # The functions below are all defined in KafkaEx.Server0P8P2 and their
  # implementation is exactly same across both versions of kafka.

  defdelegate kafka_server_consumer_group(state), to: Server0P8P2
  defdelegate kafka_server_fetch(fetch_request, state), to: Server0P8P2
  defdelegate kafka_server_offset_fetch(offset_fetch, state), to: Server0P8P2
  defdelegate kafka_server_offset_commit(offset_commit_request, state), to: Server0P8P2
  defdelegate kafka_server_consumer_group_metadata(state), to: Server0P8P2
  defdelegate kafka_server_update_consumer_metadata(state), to: Server0P8P2

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    self() |> IO.inspect(label: "#{__MODULE__}.kafka_server_init self")

    uris = Keyword.get(args, :uris, [])
    metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
    consumer_group_update_interval = Keyword.get(args, :consumer_group_update_interval, @consumer_group_update_interval)

    # this should have already been validated, but it's possible someone could
    # try to short-circuit the start call
    consumer_group = Keyword.get(args, :consumer_group)
    unless KafkaEx.valid_consumer_group?(consumer_group) do
      raise InvalidConsumerGroupError, consumer_group
    end

    use_ssl = Keyword.get(args, :use_ssl, false)
    ssl_options = Keyword.get(args, :ssl_options, [])

    brokers = Enum.map(uris,
      fn({host, port}) -> %Broker{host: host, port: port, socket: NetworkClient.create_socket(host, port, ssl_options, use_ssl)} end
    )

    state = %State{
      brokers: brokers,
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: consumer_group_update_interval,
      worker_name: name,
      ssl_options: ssl_options,
      use_ssl: use_ssl
    }

    Process.send_after(self(), :init_server_connect, 1000)

    {:ok, state}
  end

  def kafka_server_connect(%State{brokers: brokers} = state) do
    IO.puts("#{__MODULE__}.kafka_server_connect -> retrieve_metadata")
    case retrieve_metadata(brokers, 0, config_sync_timeout()) do
      {:error, reason} ->
        self() |> IO.inspect(label: "#{__MODULE__}.kafka_server_init retrieve_metadata :error reason:#{inspect reason}")

        # this is supervised by the main OTP app, so if this returns stop too many times
        # it will stop the app!
        # {:stop, reason}

        # :ignore # returns `{:ok, ?}` which means start_worker cannot pattern match!

        Process.send_after(self(), :init_server_connect, 1000)
        {:noreply, state}

      {correlation_id, metadata} ->
        IO.puts "#{__MODULE__}.kafka_server_connect retrieve_metadata ok"
        state = %State{state | metadata: metadata, correlation_id: correlation_id}
        # Get the initial "real" broker list and start a regular refresh cycle.
        state = update_metadata(state)
        {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

        if consumer_group?(state) do
          # If we are using consumer groups then initialize the state and start the update cycle
          {_, updated_state} = update_consumer_metadata(state)
          {:ok, _} = :timer.send_interval(state.consumer_group_update_interval, :update_consumer_metadata)

          # {:ok, updated_state}
          {:noreply, %{updated_state | ready: true}}
        else
          # {:ok, state}
          {:noreply, state}
        end
    end
  end

  # def kafka_server_ready_check(%State{brokers: brokers} = state) do
  #   value
  #   {:reply, value, state}
  # end

  def kafka_server_join_group(request, network_timeout, state_in) do
    state_in.brokers |> IO.inspect(label: "#{__MODULE__}.kafka_server_join_group brokers")

    {response, state_out} = consumer_group_sync_request(
      request,
      JoinGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_sync_group(request, network_timeout, state_in) do
    state_in.brokers |> IO.inspect(label: "#{__MODULE__}.kafka_server_sync_group brokers")

    {response, state_out} = consumer_group_sync_request(
      request,
      SyncGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_leave_group(request, network_timeout, state_in) do
    state_in.brokers |> IO.inspect(label: "#{__MODULE__}.kafka_server_leave_group brokers")
    {response, state_out} = consumer_group_sync_request(
      request,
      LeaveGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_heartbeat(request, network_timeout, state_in) do
    IO.puts("#{__MODULE__}.kafka_server_heartbeat")
    {response, state_out} = consumer_group_sync_request(
      request,
      Heartbeat,
      network_timeout,
      state_in
    )

    response |> IO.inspect(label: "#{__MODULE__}.kafka_server_heartbeat response")

    {:reply, response, state_out}
  end

  defp consumer_group_sync_request(
    request,
    protocol_module,
    network_timeout,
    state
  ) do
    IO.puts "#{__MODULE__}.consumer_group_sync_request"
    unless consumer_group?(state) do
      raise ConsumerGroupRequiredError, request
    end

    {broker, state} = broker_for_consumer_group_with_update(state, true)

    state_out = %{state | correlation_id: state.correlation_id + 1}

    sync_timeout = config_sync_timeout(network_timeout)

    wire_request = protocol_module.create_request(
      state.correlation_id,
      @client_id,
      request
    )
   broker |> IO.inspect(label: "#{__MODULE__}.consumer_group_sync_request calling send_sync_request broker")
    wire_response = NetworkClient.send_sync_request(
      broker,
      wire_request,
      sync_timeout
    )
    |> IO.inspect(label: "#{__MODULE__}.consumer_group_sync_request send_sync_request res")

    case wire_response do
      {:error, reason} ->
        reason |> IO.inspect(label: "#{__MODULE__}.consumer_group_sync_request send_sync_request error")
        {{:error, reason}, state_out}
      _ ->
        response = protocol_module.parse_response(wire_response)

        if response.error_code == :not_coordinator_for_consumer do
          {_, updated_state_out} = update_consumer_metadata(state_out)
          consumer_group_sync_request(
            request,
            protocol_module,
            network_timeout,
            updated_state_out
          )
        else
          {response, state_out}
        end
    end
  end

  defp update_consumer_metadata(state), do: update_consumer_metadata(state, @retry_count, 0)

  defp update_consumer_metadata(%State{consumer_group: consumer_group} = state, 0, error_code) do
    Logger.log(:error, "Fetching consumer_group #{consumer_group} metadata failed with error_code #{inspect error_code}")
    {%ConsumerMetadataResponse{error_code: error_code}, state}
  end

  defp update_consumer_metadata(%State{consumer_group: consumer_group, correlation_id: correlation_id} = state, retry, _error_code) do
    IO.puts "#{__MODULE__}.update_consumer_metadata"
    response = correlation_id
      |> ConsumerMetadata.create_request(@client_id, consumer_group)
      |> first_broker_response(state)
      |> ConsumerMetadata.parse_response

    case response.error_code do
      :no_error ->
        {
          response,
          %{
            state |
            consumer_metadata: response,
            correlation_id: state.correlation_id + 1
          }
        }
      _ -> :timer.sleep(400)
        update_consumer_metadata(
          %{state | correlation_id: state.correlation_id + 1},
          retry - 1,
          response.error_code
        )
    end
  end

  defp broker_for_consumer_group(state) do
    ConsumerMetadataResponse.broker_for_consumer_group(state.brokers, state.consumer_metadata)
  end

  # refactored from two versions, one that used the first broker as valid answer, hence
  # the optional extra flag to do that. Wraps broker_for_consumer_group with an update
  # call if no broker was found.
  defp broker_for_consumer_group_with_update(state, use_first_as_default \\ false) do
    case broker_for_consumer_group(state) do
      nil ->
        IO.puts("#{__MODULE__}.broker_for_consumer_group_with_update 1")
        {_, updated_state} = update_consumer_metadata(state)
        IO.puts("#{__MODULE__}.broker_for_consumer_group_with_update 2")
        default_broker = if use_first_as_default, do: hd(state.brokers), else: nil
        state.brokers |> IO.inspect(label: "#{__MODULE__}.broker_for_consumer_group_with_update brokers")
        {broker_for_consumer_group(updated_state) || default_broker, updated_state}
        |> IO.inspect(label: "#{__MODULE__}.broker_for_consumer_group_with_update ret")
      broker ->
        broker |> IO.inspect(label: "#{__MODULE__}.broker_for_consumer_group_with_update broker")
        {broker, state}
    end
  end

  # note within the genserver state, we've already validated the
  # consumer group, so it can only be either :no_consumer_group or a
  # valid binary consumer group name
  def consumer_group?(%State{consumer_group: :no_consumer_group}), do: false
  def consumer_group?(_), do: true

  defp first_broker_response(request, state) do
    first_broker_response(request, state.brokers, config_sync_timeout())
  end
end
