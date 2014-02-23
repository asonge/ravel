
defmodule Ravel.GCounter do
  @moduledoc "A grow-only counter"
  
  @type actor :: term
  @type value :: pos_integer
  
  defrecordp :s, __MODULE__, dict: nil
  
  @opaque gcounter :: { __MODULE__, dict: Dict.t | nil }
  
  @doc "Create a new gcounter"
  @spec new() :: gcounter
  def new() do
    s(dict: HashDict.new())
  end
  
  @doc "Get a list of actors"
  @spec actors(counter) :: [actor] when counter: gcounter
  def actors(s(dict: t)) do
    Enum.sort(HashDict.keys(t))
  end

  # This is broken if v1 is a subset of v2, try both ways because import error in some elixir versions <0.12.4
  @doc "See if CRDT's are equal, not if their values are equal."
  @spec equal?(counter, counter) :: boolean when counter: gcounter
  def equal?(s(dict: v1),s(dict: v2)) do
    Dict.size(v1)==Dict.size(v2) and Dict.equal?(v1, v2)
  end
  
  @doc "Increment the counter for actor by 1"
  @spec increment(counter, actor) :: gcounter when counter: gcounter
  def increment(counter, actor) do
    increment(counter, actor, 1)
  end
  
  @doc "Increment the counter for actor by n"
  @spec increment(counter, actor, pos_integer) :: gcounter when counter: gcounter
  def increment(s(dict: t), actor, n) when n > 0 do
    s(dict: HashDict.update(t, actor, n, &(&1+n)))
  end
  
  @doc "Merge 2 gcounters"
  @spec merge(counter1, counter2) :: gcounter when counter1: gcounter, counter2: gcounter
  def merge(s(dict: v1), s(dict: v2)) do
    s(dict: HashDict.merge(v1, v2, fn(_, v1, v2) -> max(v1, v2) end))
  end
  
  @doc "The value of the grow-only counter"
  @spec value(counter) :: pos_integer() when counter: gcounter
  def value(s(dict: t)) do
    Enum.reduce(t, 0, fn({_, v}, acc) -> acc + v end)
  end
    
end

defimpl Ravel.CRDT, for: Ravel.GCounter do
  alias Ravel.GCounter
  
  def actors(crdt),        do: GCounter.actors(crdt)
  def equal?(crdt1, crdt2),       do: crdt1 === crdt2
  def equal_value?(crdt1, crdt2), do: GCounter.value(crdt1) === GCounter.value(crdt2)
  def merge(_, crdt2) when elem(crdt2,0)!==GCounter do
    raise Ravel.CRDTMismatchError [type1: GCounter, type2: elem(crdt2, 0)]
  end
  def merge(crdt1, crdt2), do: GCounter.merge(crdt1, crdt2)
  def value(crdt),         do: GCounter.value(crdt)
  
end

defimpl Inspect, for: Ravel.GCounter do
  import Inspect.Algebra
  
  def inspect({Ravel.GCounter, dict}=counter, opts) do
    case opts.pretty do
      false ->
        concat ["#GCounter<", Inspect.List.inspect(HashDict.to_list(dict) |> Enum.sort, opts), ">"]
      true ->
        value = Ravel.GCounter.value counter
        "#GCounter<#{value}>"
    end
  end
end
