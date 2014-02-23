
defmodule Ravel.ORSet do
  @moduledoc """
  An observed-remove Set.
  
  Doesn't use tombstones, but has 1 vclock for each entry and a vclock for the
  set. When sets are merged, the set vclocks are used to make sure that deletes
  don't reappear.
  
  Based on `riak_dt_orswot`.
  """
  
  alias Ravel.VClock
  
  @behaviour Set
  
  defrecordp :s, __MODULE__, [clock: nil, values: nil]
  
  @type vclock :: VClock.vclock
  @type actor :: term
  
  @opaque orset :: {__MODULE__, clock :: vclock, values :: Dict.t}
  
  @doc "Creates a new ORSet"
  @spec new() :: orset
  def new() do
    s(clock: VClock.new(), values: HashDict.new())
  end
  
  @doc """
  Create a new ORSet
  """
  @spec new(Enum.t) :: orset
  def new(enum) do
    Enum.reduce enum, new, fn
      ({actor, value}, s()=acc) ->
        add!(acc, actor, value);
      (_, _) ->
        {:error, :need_actor_tuple}
    end
  end
  
  @doc "Creates a new ORSet given an Enum"
  @spec new(Enum.t, actor) :: orset
  def new(enum, actor) do
    Enum.reduce(enum, new, fn(v, acc) -> add!(acc, actor, v) end)
  end
  
  @doc "List the current actors used"
  @spec actors(orset) :: [actor]
  def actors(s(clock: clock)=_orset) do
    VClock.actors(clock)
  end
  
  @doc "Add an element to the set"
  @spec add(orset, actor, term) :: {:ok, orset}
  def add(s(clock: setclock, values: values)=orset, actor, elem) do
    new_clock = VClock.increment(setclock, actor)
    new_value_clock = VClock.new([{actor, VClock.get_counter(new_clock, actor)}])
    new_values = HashDict.update(values, elem, new_value_clock, fn(old_value_clock) ->
      VClock.merge(new_value_clock, old_value_clock)
    end)
    {:ok, s(orset, clock: new_clock, values: new_values)}
  end
  
  @doc "Add an element to the set"
  @spec add!(orset, actor, term) :: orset
  def add!(orset, actor, elem) do
    {:ok, new_orset} = add(orset, actor, elem)
    new_orset
  end
  
  @doc "Removes element from set, but violates semantics and ignores preconditions!"
  @spec delete(orset, term) :: orset
  def delete(orset, elem) do
    case remove(orset, elem) do
      {:ok, newset} -> newset
      _ -> orset
    end
  end
  
  @doc "Difference between 2 sets, return as a merged orset."
  @spec difference(orset, orset) :: orset
  def difference(set1, set2) do
    Enum.reduce(to_list(set2), set1, &delete(&2, &1))
  end
  
  @doc """
  Check if the sets have no common elements and are causally disjoint.
  
  The semantics of this function are not simple. If a merge function would
  remove an element, even if that element is not "present" in each set, then
  the sets are not considered disjoint.
  
  If you want these simpler semantics, you can simply cast to a HashSet and
  rerun the disjoint function.
  """
  @spec disjoint?(orset, orset) :: boolean
  def disjoint?(s(clock: clock1, values: values1)=_orset1, s(clock: clock2, values: values2)=_orset2) do
    # Set.disjoint?(HashSet.new(value(set1)), HashSet.new(value(set2)))
    # newclock = VClock.merge(clock1, clock2)
    keys1 = HashSet.new(Dict.keys(values1))
    keys2 = HashSet.new(Dict.keys(values2))
    
    case Enum.count(Set.intersection(keys1, keys2)) do
      0 ->
        Enum.all?(values1, &do_disjoint_detect(clock2, &1)) and 
        Enum.all?(values2, &do_disjoint_detect(clock1, &1))
      _ ->
        false
    end
    
  end
  
  @doc "Return an empty list of the same type as orset"
  @spec empty(orset) :: boolean
  def empty(s()=_orset), do: new
  
  @doc "Check if the orsets are equal"
  @spec equal?(orset, orset) :: boolean
  def equal?(s(values: v1), s(values: v2)) do
    Dict.size(v1) == Dict.size(v2) and Dict.equal?(v1, v2)
  end
  
  @doc "Intersection"
  @spec intersection(orset, orset) :: orset
  def intersection(s(clock: clock1, values: values1)=v1, s(clock: clock2, values: values2)=_v2) do
    # I'm doing this just like merge(), but only for common keys, removing deletes in the merge_common_keys()
    newclock = VClock.merge(clock1, clock2)
    keys1 = HashSet.new(Dict.keys(values1))
    keys2 = HashSet.new(Dict.keys(values2))
    
    common_keys = Set.intersection(keys1, keys2)
    
    newvalues = merge_common_keys(common_keys, values1, values2)
    s(v1, clock: newclock, values: newvalues)
  end
  
  @doc "Check if the set contains an element"
  @spec member?(orset, term) :: boolean
  def member?(s(values: values), elem) do
    values[elem] !== nil
  end
  
  @doc "Merge two orsets"
  @spec merge(orset, orset) :: orset
  def merge(v1, v2) when v1 === v2 do
    v1
  end
  def merge(s(clock: clock1, values: values1)=v1, s(clock: clock2, values: values2)=_v2) do
    newclock = VClock.merge(clock1, clock2)
    keys1 = HashSet.new(Dict.keys(values1))
    keys2 = HashSet.new(Dict.keys(values2))
    
    common_keys = Set.intersection(keys1, keys2)
    uniquekeys1 = Set.difference(keys1, common_keys)
    uniquekeys2 = Set.difference(keys2, common_keys)
    
    newvalues = merge_common_keys(common_keys, values1, values2)
    |> merge_disjoint_keys(uniquekeys1, values1, clock2)
    |> merge_disjoint_keys(uniquekeys2, values2, clock1)
    
    s(v1, clock: newclock, values: newvalues)
  end
  
  @doc "Insert new element into set"
  @spec put(orset, {actor, term}) :: orset
  def put(orset, {actor, elem}), do: add!(orset, actor, elem)
  
  @doc "Check if one set is a subset of the other...gotta think about the deletes"
  @spec subset?(orset, orset) :: boolean
  def subset?(s(values: v1)=_set1, s(values: v2)=_set2) do
    Enum.all?(v1, fn({k,_}) -> v2[k] !== nil end)
  end
  
  @doc "To list, is value"
  @spec to_list(orset) :: [term]
  def to_list(s(values: values)=_orset) do
    Dict.keys(values)
  end
  
  @doc "Callback implementation for Enumerable support, do not use directly"
  @spec reduce(orset, Enumerable.acc, Enumberable.reducer) :: Enumerable.result
  def reduce(s(values: v), acc, fun) do
    do_reduce(Dict.keys(v), acc, fun)
  end
  
  @doc "Remove an element from the set"
  @spec remove(orset, term) :: {:ok, orset} | {:error, {:precondition, {:not_present, term() }}}
  def remove(s(values: values)=orset, elem) do
    case values[elem] do
      nil -> {:error, {:precondition, {:not_present, elem}}}
      _ -> {:ok, s(orset, values: Dict.delete(values, elem))}
    end
  end
  
  @doc "Remove an element from the set"
  @spec remove!(orset, term) :: orset
  def remove!(orset, elem) do
    {:ok, new_orset} = remove(orset, elem)
    new_orset
  end
  
  @doc "Get the size of the values"
  @spec size(orset) :: non_neg_integer
  def size(s(values: v)) do
    Dict.size(v)
  end
  
  @doc "Get the value for this CRDT"
  @spec value(orset) :: [term]
  def value(s(values: v)=_orset) do
    HashSet.new(Dict.keys(v))
  end
  
  @doc """
  Union is an alias for the merge operation.
  
  Note: This is not always the same as converting each ORSet to a HashSet and
  running union on the Sets. Because this an alias operation for merge, deletes
  currently propogate. If you believe these semantics are invalid, make a case.
  """
  @spec union(orset, orset) :: orset
  def union(set1, set2), do: merge(set1, set2)
  
  # Merge common keys into a new values dictionary
  @spec merge_common_keys(Set.t, Dict.t, Dict.t) :: Dict.t
  defp merge_common_keys(common_keys, values1, values2) do
    Enum.map(common_keys, fn(k) ->
      {k, VClock.merge(values1[k], values2[k])}
    end) |> HashDict.new
  end
  
  # Merge disjoint keys into the values dictionary
  @spec merge_disjoint_keys(Dict.t, Set.t, Dict.t, vclock) :: Dict.t
  defp merge_disjoint_keys(new_values, keys, values, setclock) do
    Enum.reduce(keys, new_values, fn(k, acc) ->
      vclock = values[k]
      case VClock.descends(setclock, vclock) do
        false ->
          new_clock = VClock.subtract_dots(vclock, setclock)
          Dict.put(acc, k, new_clock)
        true ->
          acc
      end
    end)
  end
  
  # Checks if these updates are all disjoint.
  defp do_disjoint_detect(setclock, {_k,clock}) do
    not VClock.descends(setclock, clock)
  end
  
  defp do_reduce(_,     { :halt, acc }, _fun),   do: { :halted, acc }
  defp do_reduce(list,  { :suspend, acc }, fun), do: { :suspended, acc, &do_reduce(list, &1, fun) }
  defp do_reduce([],    { :cont, acc }, _fun),   do: { :done, acc }
  defp do_reduce([h|t], { :cont, acc }, fun),    do: do_reduce(t, fun.(h, acc), fun)
  
end

defimpl Ravel.CRDT, for: Ravel.ORSet do
  alias Ravel.ORSet
  
  def actors(crdt),        do: ORSet.actors(crdt)
  def equal?(crdt1, crdt2),       do: ORSet.equal?(crdt1, crdt2)
  def equal_value?(crdt1, crdt2) do
    HashSet.equal?(HashSet.new(ORSet.value(crdt1)),HashSet.new(ORSet.value(crdt2)))
  end
  def merge(_, crdt2) when elem(crdt2,0)!==ORSet do
    raise Ravel.CRDTMismatchError [type1: ORSet, type2: elem(crdt2, 0)]
  end
  def merge(crdt1, crdt2), do: ORSet.merge(crdt1, crdt2)
  def value(crdt),         do: ORSet.value(crdt)
  
end

defimpl Enumerable, for: Ravel.ORSet do
  
  alias Ravel.ORSet
  
  def count(orset),          do: {:ok, ORSet.size orset}
  def member?(orset, value), do: {:ok, ORSet.member?(orset, value)}
  def reduce(orset, acc, fun), do: ORSet.reduce(orset, acc, fun)
  
end

defimpl Inspect, for: Ravel.ORSet do
  import Inspect.Algebra
  
  def inspect({Ravel.ORSet, clock, values}, opts) do
    concat [
      "#ORSet<", Kernel.inspect(clock, [pretty: true]), ", ",
      Inspect.List.inspect(HashDict.to_list(values) |> Enum.sort, opts),
      ">" ]
  end
  
end
