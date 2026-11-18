defmodule Ch.TelemetryTest do
  use ExUnit.Case

  @events [[:ch, :query, :start], [:ch, :query, :stop], [:ch, :query, :exception]]

  setup do
    handler_id = make_ref()
    :telemetry.attach_many(handler_id, @events, &__MODULE__.send_to_test/4, self())
    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  def send_to_test(event, measurements, metadata, test) do
    if self() == test, do: send(test, {:telemetry, event, measurements, metadata})
  end

  test "connecting emits no query events" do
    test = self()
    handler_id = make_ref()

    :telemetry.attach(
      handler_id,
      [:ch, :query, :start],
      fn
        _event, _measurements, %{hostname: "127.0.0.1"}, _config -> send(test, {:start, self()})
        _event, _measurements, _metadata, _config -> :ok
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    conn =
      start_supervised!({Ch, database: Ch.Test.database(), hostname: "127.0.0.1", pool_size: 1},
        id: :other
      )

    assert {:ok, _} = Ch.query(conn, "select 1")
    assert_received {:start, ^test}
    refute_received {:start, _connection}
  end

  test "a query emits start and stop in the calling process", %{conn: conn} do
    assert {:ok, %{rows: [[1]]}} = Ch.query(conn, "select {a:UInt8}", %{"a" => 1})

    assert_receive {:telemetry, [:ch, :query, :start], %{system_time: _, monotonic_time: _},
                    start_metadata}

    assert_receive {:telemetry, [:ch, :query, :stop], %{duration: duration}, stop_metadata}
    assert duration > 0

    assert %{
             query: %Ch.Query{statement: "select {a:UInt8}", command: :select},
             database: database,
             hostname: "localhost",
             port: 8123,
             source: nil
           } = start_metadata

    assert database == Ch.Test.database()
    assert stop_metadata == Map.merge(start_metadata, %{error: nil, num_rows: 1})
  end

  test "the query's source option is in the metadata", %{conn: conn} do
    assert {:ok, _} = Ch.query(conn, "select 1", [], source: "events")
    assert_receive {:telemetry, [:ch, :query, :stop], _, %{source: "events"}}
  end

  test "the database passed to the query wins over the connection's", %{conn: conn} do
    assert {:ok, _} = Ch.query(conn, "select 1", [], database: "system")
    assert_receive {:telemetry, [:ch, :query, :stop], _, %{database: "system"}}
  end

  test "a failed query reports the ClickHouse error on stop", %{conn: conn} do
    assert {:error, %Ch.Error{code: 62}} = Ch.query(conn, "select ")

    assert_receive {:telemetry, [:ch, :query, :stop], _,
                    %{query: %Ch.Query{statement: "select "}, error: %Ch.Error{code: 62}}}
  end

  test "an insert emits start and stop", %{conn: conn} do
    Ch.query!(conn, "create table telemetry_insert(a UInt8) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table telemetry_insert") end)
    assert_receive {:telemetry, [:ch, :query, :stop], _, _}

    assert {:ok, %{num_rows: 2}} =
             Ch.query(conn, "insert into telemetry_insert(a) format RowBinary", [[1], [2]],
               types: ["UInt8"]
             )

    assert_receive {:telemetry, [:ch, :query, :start], _, %{query: %Ch.Query{command: :insert}}}

    assert_receive {:telemetry, [:ch, :query, :stop], _,
                    %{query: %Ch.Query{command: :insert}, error: nil, num_rows: 2}}
  end

  test "a failed query reports no rows", %{conn: conn} do
    assert {:error, _} = Ch.query(conn, "select ")
    assert_receive {:telemetry, [:ch, :query, :stop], _, %{num_rows: nil}}
  end

  test "an undecoded query reports no rows", %{conn: conn} do
    assert {:ok, _} = Ch.query(conn, "select 1", [], decode: false)
    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: nil, num_rows: nil}}
  end

  test "a query that fails to decode emits exception instead of stop", %{conn: conn} do
    assert_raise ArgumentError, fn -> Ch.query(conn, "select toIntervalDay(1)") end

    assert_receive {:telemetry, [:ch, :query, :exception], _,
                    %{kind: :error, reason: %ArgumentError{}}}

    refute_received {:telemetry, [:ch, :query, :stop], _, _}
  end

  test "a stream stops only after its last row is fetched", %{conn: conn} do
    statement = "select number from numbers({count:UInt64}) settings max_block_size = 1"

    chunks =
      DBConnection.run(conn, fn conn ->
        conn
        |> Ch.stream(statement, %{"count" => 5})
        |> Enum.map(fn result ->
          refute_received {:telemetry, [:ch, :query, :stop], _, _}
          result
        end)
      end)

    assert length(chunks) > 1

    assert_received {:telemetry, [:ch, :query, :start], _,
                     %{query: %Ch.Query{statement: ^statement}}}

    assert_received {:telemetry, [:ch, :query, :stop], _, %{error: nil, num_rows: 5}}
    refute_received {:telemetry, [:ch, :query, _], _, _}
  end

  test "a streamed insert is one query that stops after its last chunk", %{conn: conn} do
    Ch.query!(conn, "create table telemetry_stream_insert(a UInt8) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table telemetry_stream_insert") end)
    assert_receive {:telemetry, [:ch, :query, :start], _, _}
    assert_receive {:telemetry, [:ch, :query, :stop], _, _}

    statement = "insert into telemetry_stream_insert(a) format RowBinary"

    DBConnection.run(conn, fn conn ->
      stream = Ch.stream(conn, statement, [], source: "telemetry_stream_insert", encode: false)

      [[1], [2], [3]]
      |> Stream.map(fn row ->
        refute_received {:telemetry, [:ch, :query, :stop], _, _}
        Ch.RowBinary.encode_row(row, ["UInt8"])
      end)
      |> Enum.into(stream)
    end)

    assert_received {:telemetry, [:ch, :query, :start], _,
                     %{query: %Ch.Query{statement: ^statement}, source: "telemetry_stream_insert"}}

    assert_received {:telemetry, [:ch, :query, :stop], _,
                     %{query: %Ch.Query{statement: ^statement}, error: nil, num_rows: 3}}

    refute_received {:telemetry, [:ch, :query, _], _, _}
    assert %{rows: [[3]]} = Ch.query!(conn, "select count() from telemetry_stream_insert")
  end

  test "a failed streamed insert reports the ClickHouse error on stop", %{conn: conn} do
    assert_raise Ch.Error, fn ->
      DBConnection.run(conn, fn conn ->
        stream =
          Ch.stream(conn, "insert into telemetry_missing_table(a) format RowBinary", [],
            encode: false
          )

        Enum.into([Ch.RowBinary.encode_row([1], ["UInt8"])], stream)
      end)
    end

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: %Ch.Error{code: 60}}}
  end

  test "a query that raises emits exception instead of stop", %{conn: conn} do
    assert_raise ArgumentError, fn ->
      DBConnection.run(conn, fn conn ->
        conn
        |> Ch.stream("select toIntervalDay(1)")
        |> Enum.into([])
      end)
    end

    assert_receive {:telemetry, [:ch, :query, :exception], %{duration: _}, %{kind: :error}}
    refute_received {:telemetry, [:ch, :query, :stop], _, _}
  end

  test "a failed stream reports the ClickHouse error on stop", %{conn: conn} do
    assert_raise Ch.Error, fn ->
      DBConnection.run(conn, fn conn -> conn |> Ch.stream("select ") |> Enum.into([]) end)
    end

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: %Ch.Error{code: 62}}}
  end

  test "a stream stopped early stops without an error", %{conn: conn} do
    DBConnection.run(conn, fn conn ->
      conn
      |> Ch.stream("select number from numbers(5000000) settings max_block_size=1000")
      |> Enum.take(1)
    end)

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: nil}}
  end

  test "a stream that is not decoded leaves its rows out", %{conn: conn} do
    DBConnection.run(conn, fn conn ->
      conn
      |> Ch.stream("select number from numbers(10) format RowBinary", [], decode: false)
      |> Stream.run()
    end)

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: nil, num_rows: nil}}
  end

  test "a streamed insert whose source raises ends its query", %{conn: conn} do
    Ch.query!(conn, "create table telemetry_halted_insert(a UInt8) engine Memory")
    on_exit(fn -> Ch.Test.query("drop table telemetry_halted_insert") end)
    assert_receive {:telemetry, [:ch, :query, :stop], _, _}

    assert_raise RuntimeError, "source failed", fn ->
      DBConnection.run(conn, fn conn ->
        stream =
          Ch.stream(conn, "insert into telemetry_halted_insert(a) format RowBinary", [],
            encode: false
          )

        [[1], [2], :fail]
        |> Stream.map(fn
          :fail -> raise "source failed"
          row -> Ch.RowBinary.encode_row(row, ["UInt8"])
        end)
        |> Enum.into(stream)
      end)
    end

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: nil}}
  end

  def send_from_any_process(event, _measurements, metadata, test) do
    send(test, {:ended, List.last(event), metadata.telemetry_span_context, metadata})
  end

  defp attach_from_any_process(test) do
    handler_id = make_ref()

    :telemetry.attach_many(
      handler_id,
      [[:ch, :query, :stop], [:ch, :query, :exception]],
      &__MODULE__.send_from_any_process/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    test
  end

  test "a stream whose checkout times out is stopped once, by the pool, with the error" do
    attach_from_any_process(self())
    conn = start_supervised!({Ch, database: Ch.Test.database(), pool_size: 1}, id: :timeout)

    assert_raise DBConnection.ConnectionError, fn ->
      DBConnection.run(
        conn,
        fn conn ->
          conn
          |> Ch.stream("select number from numbers(5000000) settings max_block_size=1000")
          |> Stream.each(fn _ -> Process.sleep(50) end)
          |> Stream.run()
        end,
        timeout: 300
      )
    end

    assert_receive {:telemetry, [:ch, :query, :start], _, %{telemetry_span_context: ref}}

    assert_receive {:ended, :stop, ^ref, %{error: %DBConnection.ConnectionError{}}}, 1_000
    refute_receive {:ended, _, ^ref, _}, 200
  end

  test "a stream that raises ends once, with the exception", %{conn: conn} do
    attach_from_any_process(self())

    assert_raise ArgumentError, fn ->
      DBConnection.run(conn, fn conn ->
        conn
        |> Ch.stream("select toIntervalDay(1)")
        |> Enum.into([])
      end)
    end

    assert_receive {:telemetry, [:ch, :query, :start], _, %{telemetry_span_context: ref}}
    assert_receive {:ended, :exception, ^ref, _}
    refute_receive {:ended, _, ^ref, _}, 200
  end

  test "a connection error is reported on stop", %{conn: conn} do
    assert {:error, %Mint.TransportError{}} = Ch.query(conn, "select sleep(1)", [], timeout: 100)

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: %Mint.TransportError{}}}
  end
end
