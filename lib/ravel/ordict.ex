
defmodule Ravel.ORDict do
  @moduledoc """
  An observed-remove Dictionary.
  
  Doesn't use tombstones, but has 1 vclock for each entry and a vclock for the
  dict. When dicts are merged, the dict vclocks are used to make sure that deletes
  don't reappear.
  
  Based on `riak_dt_map`.
  """
  alias Ravel.CRDT
  alias Ravel.VClock
  
  @behaviour Dict
  
  defrecordp :s, __MODULE__, [clock: nil, values: nil]
  
  @type vclock :: VClock.vclock
  @type actor :: term
  @type key :: term
  @type crdt :: tuple # First element of tuple is the Module
  
  @opaque ordict :: {__MODULE__, clock :: vclock, values :: Dict.t}
  # @opaque rawkey :: term
  @opaque rawvalue :: {vclock, crdt}
  
  @doc "Creates a new ORDict"
  @spec new() :: ordict
  def new() do
    s(clock: VClock.new(), values: HashDict.new())
  end
  
  @doc """
  Create a new ORDict
  """
  @spec new(Enum.t) :: ordict
  def new(enum) do
    Enum.reduce enum, new, fn
      ({actor, {key, value}}, s()=acc) ->
        add!(acc, actor, key, value);
      (_, _) ->
        {:error, :need_actor_tuple}
    end
  end
  
  @doc "Creates a new ORDict given an Enum"
  @spec new(Enum.t, Keyword.t) :: ordict
  def new(enum, opts) when is_list(opts) do
    actor = Keyword.fetch!(opts, :actor)
    Enum.reduce enum, new, fn({k,v}, acc) -> add!(acc, actor, k, v) end
  end
  
  @doc "Creates a new ORDict given an Enum and a transformation function"
  @spec new(Enum.t, (term -> {actor, {key, crdt}})) :: ordict
  def new(enum, fun) when is_function(fun) do
    Enum.map(enum, fun) |> new
  end
  
  @doc "Get which actors are being used"
  @spec actors(ordict) :: [actor]
  def actors(s(clock: clock)=_ordict) do
    VClock.actors(clock)
  end
  
  @doc "Add an element to the dict"
  @spec add(ordict, actor, key, crdt) :: {:ok, ordict}
  def add(s(clock: setclock, values: values)=ordict, actor, key, crdt) when is_atom(elem(crdt, 0)) do
    new_clock = VClock.increment(setclock, actor)
    new_value_clock = VClock.new([{actor, VClock.get_counter(new_clock, actor)}])
    new_value = HashDict.update(values, key, {new_value_clock, crdt}, fn({old_value_clock,_old_crdt}) ->
      {VClock.merge(new_value_clock, old_value_clock), crdt}
    end)
    {:ok, s(ordict, clock: new_clock, values: new_value)}
  end
  
  @doc "Add an element to the dict"
  @spec add!(ordict, actor, key, crdt) :: ordict
  def add!(ordict, actor, key, value) do
    {:ok, new_ordict} = add(ordict, actor, key, value)
    new_ordict
  end
  
  @doc "Removes element from dict, but violates semantics and ignores preconditions!"
  @spec delete(ordict, key) :: ordict
  def delete(ordict, key) do
    case remove(ordict, key) do
      {:ok, newdict} -> newdict
      _ -> ordict
    end
  end
  
  @doc "Removes element from dict, but violates semantics and ignores preconditions!"
  @spec drop(ordict, [key]) :: ordict
  def drop(s(values: values)=ordict, keys) do
    s ordict, values: Dict.drop(values, keys)
  end
  
  @doc "Return an empty list of the same type as ordict"
  @spec empty(ordict) :: boolean
  def empty(s()=_ordict), do: new
  
  @doc "Check if the CRDT's are exactly equal"
  @spec equal?(ordict, ordict) :: boolean
  def equal?(s(values: v1), s(values: v2)) do
    Dict.size(v1)===Dict.size(v2) and
    Enum.all?(Dict.keys(v1), fn(k) ->
      {clock1, value1} = v1[k]
      {clock2, value2} = v2[k]
      VClock.equal?(clock1, clock2) and
      CRDT.equal?(value1, value2)
    end)
  end
  
  @doc "Check if the CRDT's as values are equal"
  @spec equal_value?(ordict, ordict) :: boolean
  def equal_value?(s(values: v1), s(values: v2)) do
    Dict.size(v1)===Dict.size(v2) and
    Enum.all?(Dict.keys(v1), fn(k) ->
      {_, value1} = v1[k]
      {_, value2} = v2[k]
      CRDT.equal_value?(value1, value2)
    end)
  end
  
  @doc "Returns `{ :ok, value }` associated with key in dict. If dict does not contain key, returns `:error`."
  @spec fetch(ordict, key) :: crdt
  def fetch(s(values: v)=_ordict, key) do
    case Dict.fetch v, key do
      {:ok, {_, crdt}} -> {:ok, crdt}
      other -> other
    end
  end
  
  @doc "Returns the value associated with key in dict. If dict does not contain key, it raises KeyError."
  @spec fetch!(ordict, key) :: crdt
  def fetch!(s(values: v)=_ordict, key) do
    {_, crdt} = Dict.fetch!(v, key)
    crdt
  end
  
  @doc "Returns the value associated with key in dict. If dict does not contain key, returns default (or nil if not provided)."
  @spec get(ordict, key, crdt) :: crdt | nil
  def get(s(values: v)=_ordict, key, default \\ nil) do
    case Dict.get v, key do
      nil -> default
      {_, crdt} -> crdt
    end
  end
  
  @doc "Returns whether the given key exists in the given dict."
  @spec has_key?(ordict, key) :: boolean
  def has_key?(s(values: v)=_ordict, key) do
    Dict.has_key?(v, key)
  end
  
  @doc "Returns a list of all keys in dict. The keys are not guaranteed to be in any order."
  @spec keys(ordict) :: list
  def keys(s(values: v)=_ordict) do
    Dict.keys(v)
  end
  
  @doc "Merge two ordicts"
  @spec merge(ordict, ordict) :: ordict
  def merge(ordict1, ordict2) when ordict1 === ordict2 do
    ordict1
  end
  def merge(ordict1, ordict2) do
    merge(ordict1, ordict2, &merge_common_keys/3)
  end

  @doc "Merge two ordicts with a different merge function...USE WITH CAUTION"
  @spec merge(ordict, ordict, (key, crdt, crdt -> crdt)) :: :not_supported
  def merge(s(clock: clock1, values: values1)=v1, s(clock: clock2, values: values2)=_v2, fun) do
    newclock = VClock.merge(clock1, clock2)
    keys1 = HashSet.new(Dict.keys(values1))
    keys2 = HashSet.new(Dict.keys(values2))
    
    common_keys = Set.intersection(keys1, keys2)
    uniquekeys1 = Set.difference(keys1, common_keys)
    uniquekeys2 = Set.difference(keys2, common_keys)
    
    newvalues = fun.(common_keys, values1, values2)
    |> merge_disjoint_keys(uniquekeys1, values1, clock2)
    |> merge_disjoint_keys(uniquekeys2, values2, clock1)
    
    s(v1, clock: newclock, values: newvalues)
  end
  
  @doc "Returns the value associated with `key` in `dict` as well as the `dict` without `key`."
  @spec pop(ordict, key, term) :: {term, ordict}
  def pop(s(values: v)=ordict, key, default \\ nil) do
    {_clock, new_value} = Dict.get v, key, default
    new_dict = remove! ordict, key
    {new_value, new_dict}
  end
  
  @doc "Insert new element into dict"
  @spec put(ordict, {key, actor}, crdt) :: ordict
  def put(ordict, {key, actor}, value) do
    add!(ordict, actor, key, value)
  end
  
  @doc """
  This operation is not supported.
  
  Concurrent adds will violate the semantics of this operation.
  """
  @spec put_new(ordict, {key, actor}, crdt) :: ordict
  def put_new(_ordict, {key, _actor}, _crdt) do
    raise KeyError, key: key
  end
  
  @doc "Callback implementation for Enumerable support, do not use directly"
  @spec reduce(ordict, Enumerable.acc, Enumberable.reducer) :: Enumerable.result
  def reduce(s(values: v), acc, fun) do
    do_reduce(Enum.map(v, fn({k,{_,v}}) -> {k,v} end) |> Enum.sort, acc, fun)
  end
  
  @doc "Remove an element from the dict"
  @spec remove(ordict, key) :: {:ok, ordict} | {:error, {:precondition, {:not_present, term() }}}
  def remove(s(values: values)=ordict, key) do
    case values[key] do
      nil -> {:error, {:precondition, {:not_present, key}}}
      _ -> {:ok, s(ordict, values: Dict.delete(values, key))}
    end
  end
  
  @doc "Remove an element from the dict"
  @spec remove!(ordict, key) :: ordict
  def remove!(ordict, key) do
    {:ok, new_ordict} = remove(ordict, key)
    new_ordict
  end
  
  @doc "Get the size of the values"
  @spec size(ordict) :: non_neg_integer
  def size(s(values: values)) do
    Dict.size(values)
  end
  
  @doc """
  TODO: implement
  """
  @spec split(ordict, [key]) :: {ordict, ordict}
  def split(_ordict, _keys) do
    :ok
  end
  
  @doc """
  TODO: implement
  """
  @spec take(ordict, [key]) :: ordict
  def take(_ordict, _keys) do
    :ok
  end
  
  @doc "To list, is value"
  @spec to_list(ordict) :: [term]
  def to_list(ordict), do: value(ordict)
  
  @doc """
  Update a value in `dict` by calling fun on the value to get a new value. If
  key is not present in dict then initial will be stored as the first value.
  """
  @spec update(ordict, key, crdt, (crdt -> crdt)) :: ordict
  def update(s(values: v)=ordict, {actor, key}, crdt, fun) do
    case v[key] do
      nil ->
        add!(ordict, actor, key, crdt)
      {_old_clock, old_crdt} ->
        new_crdt = fun.(old_crdt)
        add!(ordict, actor, key, new_crdt)
    end
  end
  
  @doc """
  Update a value in `dict` by calling fun on the value to get a new value. An
  exception is generated if `key` is not present in the `dict`.
  """
  @spec update!(ordict, key, (crdt -> crdt)) :: ordict | no_return
  def update!(s(values: v)=ordict, {actor, key}, fun) do
    case v[key] do
      nil ->
        raise KeyError, key: key
      {_old_clock, old_crdt} ->
        new_crdt = fun.(old_crdt)
        add!(ordict, actor, key, new_crdt)
    end
  end
  
  @doc "Get the value of a ORDict. In this case, it's a list."
  @spec value(ordict) :: [term()]
  def value(s(values: values)) do
    Enum.map values, fn({k, {_actor, v}}) ->
      # module = elem(v, 0)
      # {k, module.value(v)}
      {k, v}
    end
  end
  
  @doc "Get the values as a list"
  @spec values(ordict) :: [crdt]
  def values(s(values: v)=_ordict) do
    Dict.values(v) |> Enum.map fn({_,crdt}) -> crdt end
  end
  
  
  ####### TODO #############
  
  # Merge common keys into a new values dictionary
  @spec merge_common_keys(Set.t, Dict.t, Dict.t) :: Dict.t
  defp merge_common_keys(common_keys, values1, values2) do
    Enum.map(common_keys, fn(k) ->
      {clock1, crdt1} = values1[k]
      {clock2, crdt2} = values2[k]
      
      new_clock = VClock.merge clock1, clock2
      new_value = CRDT.merge(crdt1, crdt2)
      {k, {new_clock, new_value}}
    end) |> HashDict.new
  end
  
  # Merge disjoint keys into the values dictionary
  @spec merge_disjoint_keys(Dict.t, Set.t, Dict.t, vclock) :: Dict.t
  defp merge_disjoint_keys(new_values, keys, values, setclock) do
    Enum.reduce(keys, new_values, fn(k, acc) ->
      {vclock,crdt} = values[k]
      case VClock.descends(setclock, vclock) do
        false ->
          new_clock = VClock.subtract_dots(vclock, setclock)
          Dict.put(acc, k, {new_clock, crdt})
        true ->
          acc
      end
    end)
  end
  
  defp do_reduce(_,     { :halt, acc }, _fun),   do: { :halted, acc }
  defp do_reduce(list,  { :suspend, acc }, fun), do: { :suspended, acc, &do_reduce(list, &1, fun) }
  defp do_reduce([],    { :cont, acc }, _fun),   do: { :done, acc }
  defp do_reduce([h|t], { :cont, acc }, fun),    do: do_reduce(t, fun.(h, acc), fun)
  
end

defimpl Ravel.CRDT, for: Ravel.ORDict do
  alias Ravel.ORDict
  
  def actors(crdt),        do: ORDict.actors(crdt)
  def equal?(crdt1, crdt2),       do: ORDict.equal?(crdt1, crdt2)
  def equal_value?(crdt1, crdt2), do: ORDict.equal_value?(crdt1, crdt2)
  def merge(_, crdt2) when elem(crdt2,0)!==ORDict do
    raise Ravel.CRDTMismatchError [type1: ORDict, type2: elem(crdt2, 0)]
  end
  def merge(crdt1, crdt2), do: ORDict.merge(crdt1, crdt2)
  def value(crdt),         do: ORDict.value(crdt)
  
end

defimpl Enumerable, for: Ravel.ORDict do
  
  alias Ravel.ORDict
  
  def count(ordict),          do: {:ok, ORDict.size ordict}
  def member?(ordict, value), do: {:ok, ORDict.has_key?(ordict, value)}
  def reduce(ordict, acc, fun), do: ORDict.reduce(ordict, acc, fun)
  
end

defimpl Inspect, for: Ravel.ORDict do
  import Inspect.Algebra
  
  def inspect({Ravel.ORDict, clock, values}, opts) do
    case opts.pretty do
      true ->
        concat [
          "#ORDict<",
          Inspect.List.inspect(Enum.map(HashDict.to_list(values), fn({k,{_,v}}) -> {k,v} end) |> Enum.sort, opts),
          ">"
        ]
      false ->
        concat [
          "#ORDict<",
          Kernel.inspect(clock, [pretty: true]), ", ",
          Inspect.List.inspect(HashDict.to_list(values) |> Enum.sort, opts),
          ">"
        ]
    end
  end
  
end
