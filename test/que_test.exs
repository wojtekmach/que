defmodule QueTest do
  use ExUnit.Case, async: true
  import Que

  test "ERTS" do
    q =
      q(
        select: [:pid, :registered_name],
        from: :processes,
        where: :heap_size < 1000,
        limit: 3
      )

    procs = Que.ERTS.run(q)
    assert length(procs) == 3

    q =
      q(
        select: :*,
        from: :processes,
        where: :registered_name == :logger
      )

    [proc] = Que.ERTS.run(q)
    assert proc.pid == Process.whereis(:logger)
  end

  test "FS" do
    File.rm_rf!("tmp")
    File.mkdir_p!("tmp")
    File.write!("tmp/b.txt", "")
    sleep_until_next_second()
    File.write!("tmp/a.txt", "")

    q = q(select: [:path], from: "tmp", order_by: :path)
    assert Que.FS.run(q) == [%{path: "a.txt"}, %{path: "b.txt"}]

    q = q(select: [:path], from: "tmp", order_by: :mtime)
    assert Que.FS.run(q) == [%{path: "b.txt"}, %{path: "a.txt"}]
  end

  defp sleep_until_next_second() do
    time = System.system_time(:millisecond)
    seconds = div(time, 1000)
    delta = (seconds + 1) * 1000 - time
    Process.sleep(delta)
  end
end
