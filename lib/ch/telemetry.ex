defmodule Ch.Telemetry do
  @moduledoc false

  # A query can be ended from two processes: the caller, and the pool's connection process when
  # it disconnects with the state the caller last saved. `ended` makes sure only one of them
  # emits the stop or exception event.
  @enforce_keys [:start_time, :metadata, :ended]
  defstruct [:start_time, :metadata, :ended, num_rows: nil]

  @type t :: %__MODULE__{
          start_time: integer,
          metadata: map,
          num_rows: non_neg_integer | nil,
          ended: :atomics.atomics_ref()
        }

  defmodule Reply do
    @moduledoc false
    # The responses of a query whose span stays open until they are decoded.
    @enforce_keys [:telemetry, :responses]
    defstruct [:telemetry, :responses]
    @type t :: %__MODULE__{telemetry: Ch.Telemetry.t(), responses: list}
  end

  @spec start(map) :: t
  def start(metadata) do
    metadata = Map.put(metadata, :telemetry_span_context, make_ref())
    start_time = System.monotonic_time()
    measurements = %{system_time: System.system_time(), monotonic_time: start_time}
    :telemetry.execute([:ch, :query, :start], measurements, metadata)
    %__MODULE__{start_time: start_time, metadata: metadata, ended: :atomics.new(1, [])}
  end

  @spec stop(t, term) :: :ok
  def stop(%__MODULE__{num_rows: num_rows} = telemetry, error) do
    emit_end(telemetry, :stop, %{error: error, num_rows: num_rows})
  end

  @spec add_rows(t, non_neg_integer | nil) :: t
  def add_rows(telemetry, nil), do: telemetry
  def add_rows(%__MODULE__{num_rows: nil} = telemetry, rows), do: %{telemetry | num_rows: rows}

  def add_rows(%__MODULE__{num_rows: num_rows} = telemetry, rows),
    do: %{telemetry | num_rows: num_rows + rows}

  @spec run(t, (-> result)) :: result when result: var
  def run(telemetry, fun) do
    fun.()
  catch
    kind, reason ->
      emit_end(telemetry, :exception, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  @spec decode(Reply.t(), (list -> result)) :: result when result: %{num_rows: term}
  def decode(%Reply{telemetry: telemetry, responses: responses}, fun) do
    result = run(telemetry, fn -> fun.(responses) end)
    stop(add_rows(telemetry, result.num_rows), nil)
    result
  end

  defp emit_end(%__MODULE__{} = telemetry, event, extra) do
    %{start_time: start_time, metadata: metadata, ended: ended} = telemetry

    if :atomics.compare_exchange(ended, 1, 0, 1) == :ok do
      stop_time = System.monotonic_time()
      measurements = %{duration: stop_time - start_time, monotonic_time: stop_time}
      :telemetry.execute([:ch, :query, event], measurements, Map.merge(metadata, extra))
    end

    :ok
  end
end
