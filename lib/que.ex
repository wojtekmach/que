defmodule Que do
  defmacro q(query) do
    query
    |> Macro.prewalk(fn
      {name, _meta, args} ->
        {name, args}

      other ->
        other
    end)
    |> Map.new()
    |> Macro.escape()
  end
end

defmodule Que.ERTS do
  def run(%{from: :processes} = query) do
    Process.list()
    |> Stream.map(&(&1 |> Process.info() |> Map.new() |> Map.put(:pid, &1)))
    |> Stream.filter(&filter(&1, query[:where]))
    |> Stream.map(&select(&1, query[:select] || :*))
    |> limit(query[:limit])
  end

  defp select(item, :*) do
    item
  end

  defp select(item, fields) when is_list(fields) do
    Map.new(fields, &{&1, Map.get(item, &1)})
  end

  defp filter(_item, nil) do
    true
  end

  defp filter(item, {:<, [field, value]}) do
    v = Map.get(item, field)
    v && v < value
  end

  defp filter(item, {:==, [field, value]}) do
    v = Map.get(item, field)
    v && v == value
  end

  defp limit(stream, nil) do
    Enum.to_list(stream)
  end

  defp limit(stream, limit) do
    Enum.take(stream, limit)
  end
end
