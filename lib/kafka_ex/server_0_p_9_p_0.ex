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
    GenServer.start_link(__MODULE__, [args])
  end
  def start_link(args, name) do
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


    state = %State{
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: consumer_group_update_interval,
      worker_name: name,
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      uris: uris,
    }

    Process.send_after(self(), :init_server_connect, 1000)

    {:ok, state}
  end

  def kafka_server_connect(%State{uris: uris, ssl_options: ssl_options, use_ssl: use_ssl} = state) do
    brokers = Enum.map(uris,
      fn({host, port}) -> %Broker{host: host, port: port, socket: NetworkClient.create_socket(host, port, ssl_options, use_ssl)} end
    )

    case retrieve_metadata(brokers, 0, config_sync_timeout()) do
      {:error, _reason} ->
        Process.send_after(self(), :init_server_connect, 1000)
        {:noreply, state}

      {correlation_id, metadata} ->
        state = %State{state | metadata: metadata, correlation_id: correlation_id, brokers: brokers}
        # Get the initial "real" broker list and start a regular refresh cycle.
        state = update_metadata(state)
        {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

        if consumer_group?(state) do
          # If we are using consumer groups then initialize the state and start the update cycle
          {_, updated_state} = update_consumer_metadata(state)
          {:ok, _} = :timer.send_interval(state.consumer_group_update_interval, :update_consumer_metadata)

          {:noreply, %{updated_state | ready: true}}
        else
          {:noreply, state}
        end
    end
  end

  def kafka_server_join_group(request, network_timeout, state_in) do
    {response, state_out} = consumer_group_sync_request(
      request,
      JoinGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_sync_group(request, network_timeout, state_in) do
    {response, state_out} = consumer_group_sync_request(
      request,
      SyncGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_leave_group(request, network_timeout, state_in) do
    {response, state_out} = consumer_group_sync_request(
      request,
      LeaveGroup,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  def kafka_server_heartbeat(request, network_timeout, state_in) do
    {response, state_out} = consumer_group_sync_request(
      request,
      Heartbeat,
      network_timeout,
      state_in
    )

    {:reply, response, state_out}
  end

  defp consumer_group_sync_request(
    request,
    protocol_module,
    network_timeout,
    state
  ) do
    unless consumer_group?(state) do
      raise ConsumerGroupRequiredError, request
    end

    {broker, state} = broker_for_consumer_group_with_update(state)

    state_out = %{state | correlation_id: state.correlation_id + 1}

    sync_timeout = config_sync_timeout(network_timeout)

    wire_request = protocol_module.create_request(
      state.correlation_id,
      @client_id,
      request
    )
    wire_response = NetworkClient.send_sync_request(
      broker,
      wire_request,
      sync_timeout
    )

    case wire_response do
      {:error, reason} -> {{:error, reason}, state_out}
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
        {_, updated_state} = update_consumer_metadata(state)
        default_broker = if use_first_as_default, do: hd(state.brokers), else: nil
        {broker_for_consumer_group(updated_state) || default_broker, updated_state}
      broker ->
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
