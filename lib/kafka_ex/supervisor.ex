defmodule KafkaEx.Supervisor do
  # KafkaEx.ServerSupervisor

  @moduledoc false

  use Supervisor

  def start_link(server, max_restarts, max_seconds) do
    self() |> IO.inspect(label: "001 #{__MODULE__}.start_link server:#{inspect(server)}")
    {:ok, pid} = Supervisor.start_link(
      __MODULE__,
      [server, max_restarts, max_seconds],
      name: __MODULE__)
    {:ok, pid}
  end

  def start_child(opts) do
    Supervisor.start_child(__MODULE__, opts)
  end

  def stop_child(child) do
    Supervisor.terminate_child(__MODULE__, child)
  end

  def init([server, max_restarts, max_seconds]) do
    self() |> IO.inspect(label: "002 #{__MODULE__}.init")
    children = [
      worker(server, [])
    ]
    opts =
      [strategy: :simple_one_for_one,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    ]
    supervise(children, opts)
  end

  def stop(_) do
    self() |> IO.inspect(label: "#{__MODULE__}.stop !!!!!!!!!!!!!!!!!!!")
  end

  def terminate(reason, _state) do
    self() |> IO.inspect(label: "#{__MODULE__}.terminate #{inspect reason}")
  end
end
