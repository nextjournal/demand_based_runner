alias Experimental.GenStage
alias GenStage.PartitionDispatcher
require IEx

defmodule RH do
  defmodule LanguageWorker do
    use GenStage
    require Logger

    def start_link(lang, partition) when lang in [:julia, :python, :elixir] do
      GenStage.start_link(__MODULE__, {lang, partition}, name: {:global, {__MODULE__, lang, partition}})
    end

    def init({lang, partition}) do
      Logger.info("\n#{__MODULE__} #{inspect {lang, partition}} starting")
      {
        :consumer,
        %{lang: lang, partition: partition},
        subscribe_to: [
          {RH.HandlerProducer, [max_demand: 1, partition: {lang, partition}]}
        ]
      }
    end

    def handle_events([message], _from, state) do
      Logger.debug "\n#{__MODULE__} - #{state.lang} - #{state.partition} receives:\n#{inspect message}\n"
      :timer.sleep 1_000
      {:noreply, [], state}
    end
  end

  defmodule LanguageSupervisor do
    use Supervisor

    def start_link(lang, width) do
      Supervisor.start_link(__MODULE__, {lang, width})
    end

    def init({lang, width}) do
      children = for i <- 1..width do
        worker(LanguageWorker, [lang, i], id: {lang, i}, restart: :temporary)
      end
      supervise(children, strategy: :one_for_one)
    end
  end

  defmodule HandlerProducer do
    use GenStage
    require Logger
    def start_link(langs, width) do
      GenStage.start_link(__MODULE__, {langs, width}, name: __MODULE__)
    end

    @keyspace for _i <- 0..10, do: Enum.random(~w(A B)) <> "#{Enum.random(1..3)}"

    defstruct langs: [], width: 0, queue: :queue.new, nodes: %{}, pending: 0, keyspace: []

    def init({langs, width}) do
      partitions =
        langs
        |> Enum.flat_map(fn lang -> for i <- 1..width, do: {lang, i} end)
      hash = fn %{id: id, lang: lang} = event -> {event, {lang, :erlang.phash2(id, width)}} end
      Process.send_after self, :produce, 3_000
      Logger.debug "Starting #{__MODULE__} with partition: #{inspect partitions}"
      {
        :producer,
        %__MODULE__{langs: langs, width: width, keyspace: @keyspace},
        dispatcher: {PartitionDispatcher, [partitions: partitions, hash: hash]}
      }
    end

    def handle_demand(1, state) do
      {events, state} = out(state)
      {:noreply, events, state}
    end

    def out(%__MODULE__{queue: queue, nodes: nodes, pending: p} = state) do
      Logger.warn("outing pending: #{p}")
      case :queue.out(queue) do
        {{:value, key}, rest} ->
          node = Map.fetch(nodes, key)
          {[node], %__MODULE__{state | queue: rest}}
        {:empty, _} ->
          {[], state}
      end
    end

    def handle_info(:produce, %__MODULE__{langs: langs, keyspace: keyspace, nodes: nodes, queue: queue, pending: pending} = state) do
      :rand.seed(:exsplus, :os.timestamp)
      node = %{id: Enum.random(keyspace), lang: Enum.random(langs)}
      Process.send_after(self, :produce, 500)
      {events, state} = if pending > 0 do
        Logger.debug("sending out immediately: #{inspect node}")
        {
          [node],
          %__MODULE__{state | nodes: nodes, queue: queue}
        }
      else
        {
          [],
          %__MODULE__{state | nodes: Map.put(nodes, node.id, node), queue: maybe_queue(queue, node.id)}
        }
      end
      {:noreply, [], state}
    end

    defp maybe_queue(queue, key) do
      case :queue.member(key, queue) do
        true -> queue
        false -> :queue.in(key, queue)
      end
    end
  end

  use Application
  def start(_type, _args) do
    import Supervisor.Spec, warn: false
    children = [
      worker(HandlerProducer, [[:julia, :python, :elixir], 3], restart: :temporary),
      supervisor(LanguageSupervisor, [:julia,  3], id: :julia),
      supervisor(LanguageSupervisor, [:python, 3], id: :python),
      supervisor(LanguageSupervisor, [:elixir, 3], id: :elixir)
    ]
    opts = [strategy: :one_for_one, name: RH.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
