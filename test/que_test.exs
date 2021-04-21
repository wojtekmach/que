defmodule QueTest do
  use ExUnit.Case, async: true
  import Que

  test "it works" do
    q(
      select: [:pid, :registered_name],
      from: :processes,
      where: :heap_size < 1000,
      limit: 3
    )
    |> Que.ERTS.run()
    |> IO.inspect()

    q(
      select: :*,
      from: :processes,
      where: :registered_name == :logger
    )
    |> Que.ERTS.run()
    |> IO.inspect()
  end
end
