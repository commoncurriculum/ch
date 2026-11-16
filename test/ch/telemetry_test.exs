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
             port: 8123
           } = start_metadata

    assert database == Ch.Test.database()
    assert stop_metadata == Map.put(start_metadata, :error, nil)
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
                    %{query: %Ch.Query{command: :insert}, error: nil}}
  end

  test "a stream emits start and stop when it declares", %{conn: conn} do
    DBConnection.run(conn, fn conn ->
      conn
      |> Ch.stream("select * from numbers({count:UInt64})", %{"count" => 10})
      |> Enum.into([])
    end)

    assert_receive {:telemetry, [:ch, :query, :start], _,
                    %{query: %Ch.Query{statement: "select * from numbers({count:UInt64})"}}}

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: nil}}
  end

  test "a failed stream reports the ClickHouse error on stop", %{conn: conn} do
    assert_raise Ch.Error, fn ->
      DBConnection.run(conn, fn conn -> conn |> Ch.stream("select ") |> Enum.into([]) end)
    end

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: %Ch.Error{code: 62}}}
  end

  test "a connection error is reported on stop", %{conn: conn} do
    assert {:error, %Mint.TransportError{}} = Ch.query(conn, "select sleep(1)", [], timeout: 100)

    assert_receive {:telemetry, [:ch, :query, :stop], _, %{error: %Mint.TransportError{}}}
  end
end
