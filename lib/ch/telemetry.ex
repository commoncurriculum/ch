defmodule Ch.Telemetry do
  @moduledoc false

  # A query can be ended from two processes: the caller, and the pool's connection process when
  # it disconnects with the state the caller last saved. `ended` makes sure only one of them
  # emits the stop or exception event.
  @type t :: %{
          start_time: integer,
          metadata: map,
          num_rows: non_neg_integer | nil,
          ended: :atomics.atomics_ref()
        }

  @spec start(map) :: t
  def start(metadata) do
    metadata = Map.put(metadata, :telemetry_span_context, make_ref())
    start_time = System.monotonic_time()
    measurements = %{system_time: System.system_time(), monotonic_time: start_time}
    :telemetry.execute([:ch, :query, :start], measurements, metadata)
    %{start_time: start_time, metadata: metadata, num_rows: nil, ended: :atomics.new(1, [])}
  end

  @spec stop(t, term) :: :ok
  def stop(%{start_time: start_time, metadata: metadata, num_rows: num_rows} = telemetry, error) do
    if end_once(telemetry) do
      stop_time = System.monotonic_time()
      measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
      metadata = Map.merge(metadata, %{error: error, num_rows: num_rows})
      :telemetry.execute([:ch, :query, :stop], measurements, metadata)
    end

    :ok
  end

  @spec add_rows(t, non_neg_integer | nil) :: t
  def add_rows(telemetry, nil), do: telemetry
  def add_rows(%{num_rows: nil} = telemetry, rows), do: %{telemetry | num_rows: rows}

  def add_rows(%{num_rows: num_rows} = telemetry, rows),
    do: %{telemetry | num_rows: num_rows + rows}

  @spec run(t | nil, (-> result)) :: result when result: var
  def run(nil, fun), do: fun.()

  def run(%{start_time: start_time, metadata: metadata} = telemetry, fun) do
    fun.()
  catch
    kind, reason ->
      if end_once(telemetry) do
        stop_time = System.monotonic_time()
        measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
        metadata = Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        :telemetry.execute([:ch, :query, :exception], measurements, metadata)
      end

      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp end_once(%{ended: ended}), do: :atomics.compare_exchange(ended, 1, 0, 1) == :ok
end
