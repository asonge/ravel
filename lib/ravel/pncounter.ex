
defmodule Ravel.PNCounter do
    @moduledoc "An increment-decrement counter"
    
    @type actor :: term
    @type value :: {inc :: pos_integer, decr :: pos_integer}
  
    defrecordp :s, __MODULE__, dict: nil
  
    @opaque pncounter :: { __MODULE__, dict: Dict.t | nil }
    
    @doc "Creates a new PNCounter"
    @spec new() :: pncounter
    def new() do
        s(dict: HashDict.new())
    end
    
    @doc "The actors currently in the Hash"
    @spec actors(pncounter) :: [actor()]
    def actors(s(dict: t)) do
        Enum.sort(HashDict.keys(t))
    end
    
    # This is broken if v1 is a subset of v2, try both ways because import error in some elixir versions <0.12.4
    @doc "See if CRDT's are equal, not if their values are equal."
    @spec equal?(counter, counter) :: boolean when counter: pncounter
    def equal?(s(dict: v1),s(dict: v2)) do
      Dict.size(v1)==Dict.size(v2) and Dict.equal?(v1, v2)
    end
    
    @doc "Decrement a PNCounter"
    @spec decrement(pncounter, actor) :: pncounter
    def decrement(counter, actor) do
        decrement(counter, actor, 1)
    end
    
    @doc "Decrement a PNCounter by n"
    @spec decrement(pncounter, actor, pos_integer) :: pncounter
    def decrement(s(dict: t), actor, n) when n > 0 do
        s(dict: HashDict.update(t, actor, {0, n}, fn({pos, neg}) -> {pos, neg+n} end))
    end
    
    @doc "Checks for the equality of the PNCounter, not the total value"
    @spec equal(pncounter, pncounter) :: boolean
    def equal(s(dict: v1), s(dict: v2)) do
        v1 == v2
    end
    
    @doc "Increment a PNCounter"
    @spec increment(pncounter, actor) :: pncounter
    def increment(t, actor) do
        increment(t, actor, 1)
    end
    
    @doc "Increment a PNCounter by n"
    @spec increment(pncounter, actor, pos_integer) :: pncounter
    def increment(s(dict: t), actor, n) when n > 0 do
        s(dict: HashDict.update(t, actor, {n, 0}, fn({pos, neg}) -> {pos+n, neg} end))
    end
    
    @doc "Merge 2 PNCounters together"
    @spec merge(pncounter, pncounter) :: pncounter
    def merge(s(dict: v1)=_counter1, s(dict: v2)=_counter2) do
        s(dict: HashDict.merge(v1, v2, fn(_k, {inc1, decr1}, {inc2, decr2}) -> {max(inc1, inc2), max(decr1, decr2)} end))
    end
    
    @doc "The total/current value of the PNCounter"
    @spec value(pncounter) :: integer
    def value(s(dict: t)) do
        Enum.reduce(t, 0, fn({_, {inc, decr}}, acc) -> acc + inc - decr end)
    end
    
end

defimpl Ravel.CRDT, for: Ravel.PNCounter do
  alias Ravel.PNCounter
  
  def actors(crdt),        do: PNCounter.actors(crdt)
  def equal?(crdt1, crdt2),       do: PNCounter.equal?(crdt1, crdt2)
  def equal_value?(crdt1, crdt2), do: PNCounter.value(crdt1)===PNCounter.value(crdt2)
  def merge(_, crdt2) when elem(crdt2,0)!==PNCounter do
    raise Ravel.CRDTMismatchError [type1: PNCounter, type2: elem(crdt2, 0)]
  end
  def merge(crdt1, crdt2), do: PNCounter.merge(crdt1, crdt2)
  def value(crdt),         do: PNCounter.value(crdt)
  
end

defimpl Inspect, for: Ravel.PNCounter do
  import Inspect.Algebra
  
  def inspect({Ravel.PNCounter, dict}=counter, opts) do
    case opts.pretty do
      false ->
        concat ["#PNCounter<", Inspect.List.inspect(HashDict.to_list(dict) |> Enum.sort, opts), ">"]
      true ->
        value = Ravel.PNCounter.value counter
        "#PNCounter<#{value}>"
    end
  end
end
