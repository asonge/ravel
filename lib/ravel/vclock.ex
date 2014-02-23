
defmodule Ravel.VClock do
  @moduledoc "A vector clock implementation built on top of HashDict"
  
  @type actor :: term
  @type counter :: pos_integer
  
  defrecordp :s, __MODULE__, dict: nil

  @opaque vclock :: { __MODULE__, dict: Dict.t | nil }
  
  @doc "Returns a new vector clock"
  @spec new() :: vclock
  def new do
    s(dict: HashDict.new())
  end
  
  @doc "Returns a new vector clock, given previous {actor, counter} pairs"
  @spec new([{actor(), counter()}]) :: vclock
  def new(vclock_dict) do
    # TODO: validate the vector clock.
    case Enum.all?(vclock_dict, fn({_k,_v}) -> true; (_) -> false end) do
      true -> s(dict: HashDict.new(vclock_dict))
      false -> throw :invalid_dict
    end
  end
  
  def actors(s(dict: v)) do
    Dict.keys(v)
  end
  
  @doc "Returns true when v2 descends from v1, returns false otherwise"
  @spec descends(vclock, vclock) :: boolean
  def descends(s(dict: v1), s(dict: v2)) do
    Enum.reduce(v2, true, fn
      (_, false) -> false;
      ({k,counter2}, true) ->
        case v1[k] do
          nil -> false
          counter1 -> counter1 >= counter2
        end
    end)
  end
  
  @doc "Returns true when v1 dominates v2"
  @spec dominates(vclock, vclock) :: boolean
  def dominates(clock1, clock2) do
    descends(clock1, clock2) and !descends(clock2, clock1)
  end
  
  @doc "Subtracts elements from the first vclock where actor counters in the second vclock are larger"
  @spec subtract_dots(vclock, vclock) :: vclock
  def subtract_dots(s(dict: v1), s(dict: v2)) do
    s(dict: Enum.reduce(v1, HashDict.new(), fn({k, counter1}, acc) ->
      case v2[k] do
        nil -> Dict.put(acc, k, counter1)
        counter2 when counter1 <= counter2 -> HashDict.put(acc, k, counter1)
        counter2 when counter1 > counter2 -> acc
      end
    end))
  end
  
  @doc "Combines a list of vclocks"
  @spec merge([vclock]) :: vclock
  def merge([]) do [] end
  def merge([vclock|rest]) when is_list(rest) do merge2(rest, vclock) end
  
  @doc "Combine 2 vclocks"
  @spec merge(vclock, vclock) :: vclock
  def merge(v1, v2) do
    merge([v1, v2])
  end
  
  defp merge2([], a) do a end
  defp merge2([s(dict: b)|clocks], s(dict: a)) do
    merge2(clocks, s(dict: Dict.merge(a, b, fn(_, v1, v2) ->
      max(v1, v2)
    end)))
  end
  
  @doc "Get the counter for the given actor"
  @spec get_counter(vclock, actor) :: counter()
  def get_counter(s(dict: v), actor) do
    Dict.get(v, actor, 0)
  end
  
  @doc "Increment actor by 1"
  @spec increment(vclock, actor) :: vclock
  def increment(s(dict: v), actor) do
    s(dict: HashDict.update(v, actor, 1, &(&1+1)))
  end
  
  @doc "Return the nodes in the system"
  @spec nodes(vclock) :: [actor()]
  def nodes(s(dict: v)) do
    Enum.sort HashDict.keys v
  end
  
  @doc "Return the equality of the vector clocks"
  @spec equal(vclock, vclock) :: boolean
  def equal(s(dict: v1), s(dict: v2)) do
    v1 == v2
  end
  
  @doc "Replace {new, old} actors"
  @spec replace_actors(vclock, [{actor(), actor()}]) :: vclock()
  def replace_actors(s(dict: vclock), map) do
    s(dict: Enum.map(vclock, fn({k, v}) ->
      case Enum.find(map, fn({_,old}) -> old==k end) do
        nil -> {k, v}
        {new, _} -> {new, v}
      end
    end) |> HashDict.new())
  end
  
end

import Inspect.Algebra

defimpl Inspect, for: Ravel.VClock do
  def inspect({Ravel.VClock, dict}, opts) do
    concat ["#VClock<", Inspect.List.inspect(HashDict.to_list(dict) |> Enum.sort, opts), ">"]
  end
end
